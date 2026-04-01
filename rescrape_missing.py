"""
Re-scrape only missing/failed categories.
Uses the same logic as category_mainbot.py but targets specific indices.
Saves results to GitHub repo.
"""
import sys, subprocess, importlib, ssl, os

try:
    import certifi
    os.environ.setdefault('SSL_CERT_FILE', certifi.where())
except ImportError:
    pass
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

for imp, pkg in {"requests": "requests", "certifi": "certifi", "curl_cffi": "curl_cffi"}.items():
    try:
        importlib.import_module(imp)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

import random, json, re, socket, csv, io, base64, time
from datetime import datetime, timezone
import requests
from curl_cffi import requests as curl_requests

# ── Config ──
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO") or "Zaiinalii/fiverr-bot-storage"
GITHUB_BRANCH = "main"
CATEGORIES_FILE = "categories.csv"
RESULTS_DIR = "category_results"

MAX_PAGES = 10
VM_ID = f"rescrape-{socket.gethostname()[-8:]}-{random.randint(1000,9999)}"

FINGERPRINT_POOL = [
    "chrome124", "chrome131", "chrome120", "chrome119", "chrome116",
    "safari17_2", "safari17_0", "safari15_5", "edge101", "edge99",
]
_current_fp = random.choice(FINGERPRINT_POOL)

# VPN
VPN_ENABLED = int(os.environ.get("VPN_ENABLED", "0"))
OPENVPN_CONFIG_DIR = os.environ.get("OPENVPN_CONFIG_DIR", "/etc/openvpn/configs")
OPENVPN_AUTH_FILE = os.environ.get("OPENVPN_AUTH_FILE", "/etc/openvpn/auth.txt")

SESSION_IP = None

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── VPN ──
def vpn_disconnect():
    if not VPN_ENABLED: return
    try: subprocess.run(["sudo","killall","openvpn"], capture_output=True, timeout=10)
    except: pass
    time.sleep(3)

def vpn_connect_random():
    if not VPN_ENABLED: return True
    configs = [os.path.join(OPENVPN_CONFIG_DIR, f) for f in os.listdir(OPENVPN_CONFIG_DIR) if f.endswith('.ovpn')] if os.path.isdir(OPENVPN_CONFIG_DIR) else []
    if not configs: return False
    config = random.choice(configs)
    try:
        subprocess.run(["sudo","rm","-f","/tmp/openvpn.log"], capture_output=True, timeout=5)
        subprocess.run(["sudo","openvpn","--config",config,"--auth-user-pass",OPENVPN_AUTH_FILE,"--auth-nocache","--daemon","--log","/tmp/openvpn.log"], capture_output=True, timeout=10)
        for _ in range(15):
            time.sleep(2)
            try:
                out = subprocess.run(["sudo","cat","/tmp/openvpn.log"], capture_output=True, text=True, timeout=5).stdout
                if "Initialization Sequence Completed" in out: return True
                if "AUTH_FAILED" in out: return False
            except: pass
    except: pass
    return False

def get_ip():
    global SESSION_IP
    try:
        r = requests.get("http://ip-api.com/json/?fields=query,country", timeout=10)
        SESSION_IP = r.json().get("query","?")
        log(f"📍 IP: {SESSION_IP}")
    except:
        SESSION_IP = "?"

def rotate_vpn():
    reset_session()
    if VPN_ENABLED:
        vpn_disconnect()
        time.sleep(3)
        vpn_connect_random()
        time.sleep(5)
    get_ip()

# ── HTTP Session ──
_session = None

def _pick_fp():
    global _current_fp
    _current_fp = random.choice(FINGERPRINT_POOL)

def get_session():
    global _session
    if _session is None:
        _session = curl_requests.Session(impersonate=_current_fp)
        log(f"🌐 Session (TLS: {_current_fp})")
    return _session

def reset_session():
    global _session
    _session = None
    _pick_fp()

def http_get(url, extra_headers=None, timeout=30):
    try:
        return get_session().get(url, headers=extra_headers, timeout=timeout, allow_redirects=True), None
    except Exception as e:
        return None, str(e)

def detect_captcha(resp):
    if resp is None: return False
    if resp.status_code in (403, 429, 503): return True
    text = resp.text[:5000].lower() if resp.text else ""
    return any(x in text for x in ['px-captcha','challenge-platform','perimeterx'])

def extract_perseus_props(html):
    if not html or len(html) < 1000: return None, "HTML too short"
    m = re.search(r'<script[^>]*id="perseus-initial-props"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m: return None, "No perseus-initial-props"
    try: return json.loads(m.group(1)), None
    except: return None, "JSON parse error"

def warmup():
    log("🏠 Warmup...")
    resp, err = http_get("https://www.fiverr.com/")
    if err or detect_captcha(resp):
        log(f"   ⚠️ Warmup failed")
        return False
    if resp.status_code == 200:
        log(f"   ✅ OK ({len(resp.text):,}B)")
        return True
    return False

# ── GitHub API ──
GITHUB_API = f"https://api.github.com/repos/{GITHUB_REPO}/contents"

def _gh_h():
    return {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

def gh_read(path):
    try:
        r = requests.get(f"{GITHUB_API}/{path}?ref={GITHUB_BRANCH}", headers=_gh_h(), timeout=15)
        if r.status_code == 404: return None, None, "not_found"
        if r.status_code != 200: return None, None, f"HTTP {r.status_code}"
        d = r.json()
        return base64.b64decode(d["content"]).decode("utf-8"), d["sha"], None
    except Exception as e:
        return None, None, str(e)

def gh_write(path, content, sha=None, msg="auto"):
    payload = {"message": msg, "content": base64.b64encode(content.encode("utf-8")).decode("ascii"), "branch": GITHUB_BRANCH}
    if sha: payload["sha"] = sha
    try:
        r = requests.put(f"{GITHUB_API}/{path}", headers=_gh_h(), json=payload, timeout=15)
        if r.status_code in (200, 201): return True
        return False
    except:
        return False

# ── Categories ──
def load_categories():
    content, _, err = gh_read(CATEGORIES_FILE)
    if err:
        log(f"❌ Can't load categories: {err}")
        return []
    return list(csv.DictReader(io.StringIO(content)))

def find_missing_indices():
    """Find indices that are missing or have 0 gigs."""
    force = os.environ.get('FORCE_INDICES', '').strip()
    if force:
        indices = [int(x.strip()) for x in force.split(',') if x.strip()]
        categories = load_categories()
        log(f'\U0001f3af FORCED re-scrape of {len(indices)} specific indices')
        return indices, categories
    log("🔍 Finding missing/failed categories...")
    r = requests.get(f"{GITHUB_API}/{RESULTS_DIR}", headers=_gh_h(), timeout=15)
    if r.status_code != 200:
        log(f"❌ Can't list results: {r.status_code}")
        return list(range(900))  # rescrape all

    files = r.json()
    saved = {}
    for f in files:
        idx = int(f['name'][:4])
        saved[idx] = f

    categories = load_categories()
    if not categories:
        return []

    missing = []
    zero_gig = []

    # Missing indices
    for i in range(len(categories)):
        if i not in saved:
            missing.append(i)

    # 0-gig files (small files)
    small = [(idx, info) for idx, info in saved.items() if info['size'] < 500]
    for idx, info in small:
        try:
            url = info.get('download_url', '')
            if url:
                resp = requests.get(url, timeout=15)
                data = resp.json()
            else:
                resp = requests.get(f"{GITHUB_API}/{RESULTS_DIR}/{info['name']}", headers=_gh_h(), timeout=15)
                data = json.loads(base64.b64decode(resp.json()['content']).decode())
            if data.get('total_gigs_found', 0) == 0:
                zero_gig.append(idx)
        except:
            zero_gig.append(idx)

    all_bad = sorted(set(missing + zero_gig))
    log(f"   Missing: {len(missing)}, 0-gig: {len(zero_gig)}, Total to rescrape: {len(all_bad)}")
    return all_bad, categories

# ── Parser ──
SKIP_USERNAMES = {'categories','search','resources','help','cp','business','pro','home','hc','forums','logo-maker','pages','support','seller_dashboard','inbox','manage_orders'}

def parse_listing_page(html, page_num):
    props, err = extract_perseus_props(html)
    if err: return [], None, err
    items = props.get("items", [])
    total = None
    try: total = props.get("appData",{}).get("pagination",{}).get("total")
    except: pass
    if not items: return [], total, "No items"

    gigs, seen, org, ad = [], set(), 0, 0
    for item in items:
        try:
            u = (item.get("seller_name") or "").lower().strip()
            s = (item.get("cached_slug") or "").lower().strip()
            if not u or not s or u in SKIP_USERNAMES: continue
            gid = f"{u}_{s}"
            if gid in seen: continue
            seen.add(gid)
            gt = (item.get("type") or "").lower()
            is_sp = gt in ("promoted","ads","ad")
            if is_sp: ad += 1
            else: org += 1
            sr = item.get("seller_rating",{})
            br = item.get("buying_review_rating"); bc = item.get("buying_review_rating_count")
            rs = sr.get("score") if isinstance(sr,dict) else None; rc = sr.get("count") if isinstance(sr,dict) else None
            ar = br or rs; rvc = bc or rc
            p = item.get("price_i")
            lr = (item.get("seller_level") or "").lower()
            sl = 3 if 'top_rated' in lr else 2 if 'level_two' in lr else 1 if 'level_one' in lr else 0 if 'new' in lr else None
            gigs.append({"gig_id":gid,"username":u,"slug":s,"title":item.get("title",""),"url":f"https://www.fiverr.com{item.get('gig_url',f'/{u}/{s}')}","fiverr_gig_id":item.get("gigId")or item.get("gig_id"),"page":page_num,"organic_position":((page_num-1)*48)+org if not is_sp else None,"is_sponsored":is_sp,"ad_position":ad if is_sp else None,"seller_level":sl,"seller_country":item.get("seller_country",""),"avg_rating":round(float(ar),2) if ar else None,"review_count":int(rvc) if rvc else None,"starting_price":float(p) if p and p>0 else None,"is_pro":item.get("is_pro",False),"is_fiverr_choice":item.get("is_fiverr_choice",False)})
        except: continue
    return gigs, total, None

def scrape_category(cat_url):
    all_gigs = []
    base = f"https://www.fiverr.com{cat_url}"
    cat_total = None
    for pg in range(1, MAX_PAGES + 1):
        url = f"{base}?source=category_tree&page={pg}"
        ref = f"{base}?source=category_tree&page={max(1,pg-1)}" if pg > 1 else "https://www.fiverr.com/"
        resp, err = http_get(url, extra_headers={"Referer": ref})
        if err: break
        if detect_captcha(resp): return all_gigs, cat_total, "captcha"
        gigs, total, perr = parse_listing_page(resp.text, pg)
        if pg == 1 and total:
            cat_total = total
            log(f"    Total: {total:,}")
        if not gigs: break
        all_gigs.extend(gigs)
        log(f"    Pg {pg}: {len(gigs)} gigs (total: {len(all_gigs)})")
        if len(gigs) < 20: break
        d = random.uniform(2.0,4.5)
        if random.random() < 0.3: d += random.uniform(2.0,6.0)
        if random.random() < 0.1: d += random.uniform(5.0,12.0)
        time.sleep(d)

    if cat_total and cat_total >= 480 and len(all_gigs) < 450:
        return all_gigs, cat_total, "low_yield"
    if len(all_gigs) == 0:
        return all_gigs, cat_total, "no_gigs"
    return all_gigs, cat_total, None

def save_result(cat_index, result):
    slug = result["category_url"].strip("/").replace("/","_")
    fn = f"{RESULTS_DIR}/{cat_index:04d}_{slug}.json"
    content = json.dumps(result, indent=2, ensure_ascii=False)
    existing, sha, _ = gh_read(fn)
    # Don't overwrite existing file if it has more gigs
    if existing:
        try:
            old_data = json.loads(existing)
            old_count = old_data.get('total_gigs_found', 0)
            new_count = result.get('total_gigs_found', 0)
            if old_count > new_count:
                log(f"   ⏭️ Skipping save — existing has {old_count} gigs vs new {new_count}")
                return
        except: pass
    ok = gh_write(fn, content, sha=sha, msg=f"[{VM_ID}] rescrape cat {cat_index}: {result['sub_category']} — {result['total_gigs_found']} gigs")
    if ok:
        log(f"   ✅ Saved to GitHub: {fn}")
    else:
        os.makedirs("category_results_local", exist_ok=True)
        with open(f"category_results_local/{cat_index:04d}_{slug}.json","w",encoding="utf-8") as f:
            f.write(content)
        log(f"   💾 Saved locally (GitHub failed)")

# ── Main ──
def main():
    log(f"🔄 CATEGORY RE-SCRAPER — {VM_ID}")

    if VPN_ENABLED:
        try:
            out = subprocess.check_output(["pgrep","-l","openvpn"], text=True)
            if "openvpn" in out.lower(): log("✅ VPN already running")
        except: vpn_connect_random()
    get_ip()

    reset_session()
    if not warmup():
        rotate_vpn()
        if not warmup():
            log("❌ Cannot connect")
            sys.exit(1)

    bad_indices, categories = find_missing_indices()
    if not bad_indices:
        log("🎉 Nothing to re-scrape!")
        return

    log(f"\n📋 Re-scraping {len(bad_indices)} categories...\n")

    done = 0
    failed = 0
    consecutive_captchas = 0

    for i, idx in enumerate(bad_indices):
        cat = categories[idx]
        log(f"\n📌 [{i+1}/{len(bad_indices)}] idx={idx} {cat['main_category']} > {cat['sub_category']}")

        best_gigs = []
        success = False
        for attempt in range(1, 4):
            if attempt > 1:
                log(f"   🔄 Retry {attempt}/3")
                reset_session()
                time.sleep(random.uniform(5, 10))
                warmup()

            gigs, cat_total, error = scrape_category(cat['url'])
            if len(gigs) > len(best_gigs):
                best_gigs = gigs

            if error == "captcha":
                consecutive_captchas += 1
                log(f"   🛑 CAPTCHA ({consecutive_captchas})")
                rotate_vpn()
                time.sleep(5)
                warmup()
                if consecutive_captchas >= 5:
                    log("❌ 5 consecutive CAPTCHAs — stopping")
                    return
                continue
            if error == "no_gigs":
                log(f"   ⚠️ 0 gigs returned — retrying with new session")
                continue
            if error == "low_yield":
                log(f"   ⚠️ Low yield ({len(gigs)}/{cat_total})")
                continue

            consecutive_captchas = 0
            result = {"main_category":cat['main_category'],"sub_category":cat['sub_category'],"category_url":cat['url'],"scraped_at":datetime.now(timezone.utc).isoformat(),"scraped_by":VM_ID,"total_gigs_found":len(gigs),"gigs":gigs}
            save_result(idx, result)
            done += 1
            success = True
            break

        if not success and best_gigs:
            log(f"   ⚠️ Saving best: {len(best_gigs)} gigs")
            result = {"main_category":cat['main_category'],"sub_category":cat['sub_category'],"category_url":cat['url'],"scraped_at":datetime.now(timezone.utc).isoformat(),"scraped_by":VM_ID,"total_gigs_found":len(best_gigs),"gigs":best_gigs,"partial":True}
            save_result(idx, result)
            done += 1
        elif not success:
            failed += 1

        time.sleep(random.uniform(3.0, 6.0))

    log(f"\n{'='*50}")
    log(f"DONE! Re-scraped: {done}, Failed: {failed}")
    log(f"{'='*50}")

if __name__ == "__main__":
    main()
