"""
Fetch TOTAL gig count for each category from Fiverr.
Only hits page 1 of each category, grabs the total from perseus-initial-props.
Saves results to GitHub as category_totals.json and locally as CSV.
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

VM_ID = f"count-{socket.gethostname()[-8:]}-{random.randint(1000,9999)}"

FINGERPRINT_POOL = [
    "chrome124", "chrome131", "chrome120", "chrome119", "chrome116",
    "safari17_2", "safari17_0", "safari15_5", "edge101", "edge99",
]
_current_fp = random.choice(FINGERPRINT_POOL)

# VPN
VPN_ENABLED = int(os.environ.get("VPN_ENABLED", "0"))
OPENVPN_CONFIG_DIR = os.environ.get("OPENVPN_CONFIG_DIR", "/etc/openvpn/configs")
OPENVPN_AUTH_FILE = os.environ.get("OPENVPN_AUTH_FILE", "/etc/openvpn/auth.txt")

# Range support for distributed execution
START_INDEX = int(os.environ.get("START_INDEX", "0"))
END_INDEX = int(os.environ.get("END_INDEX", "999"))

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

def extract_total_from_html(html):
    """Extract total gig count from perseus-initial-props."""
    if not html or len(html) < 1000:
        return None, "HTML too short"
    m = re.search(r'<script[^>]*id="perseus-initial-props"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None, "No perseus-initial-props"
    try:
        props = json.loads(m.group(1))
    except:
        return None, "JSON parse error"
    
    # Try pagination.total
    try:
        total = props.get("appData", {}).get("pagination", {}).get("total")
        if total and isinstance(total, int):
            return total, None
    except:
        pass
    
    # Fallback: count items on page to estimate
    items = props.get("items", [])
    if items:
        return len(items), "estimated_from_items"
    
    return None, "No total found"

def warmup():
    log("🏠 Warmup...")
    resp, err = http_get("https://www.fiverr.com/")
    if err or detect_captcha(resp):
        return False
    if resp.status_code == 200:
        log(f"   ✅ OK")
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
        return r.status_code in (200, 201)
    except:
        return False

def load_categories():
    content, _, err = gh_read(CATEGORIES_FILE)
    if err:
        log(f"❌ Can't load categories: {err}")
        return []
    return list(csv.DictReader(io.StringIO(content)))

# ── Fetch total for one category ──
def fetch_category_total(cat_url):
    """Hit page 1 of a category and extract total gig count."""
    url = f"https://www.fiverr.com{cat_url}?source=category_tree&page=1"
    resp, err = http_get(url, extra_headers={"Referer": "https://www.fiverr.com/"})
    if err:
        return None, f"http_error: {err}"
    if detect_captcha(resp):
        return None, "captcha"
    total, parse_err = extract_total_from_html(resp.text)
    return total, parse_err

# ── Main ──
def main():
    log(f"📊 CATEGORY TOTAL COUNT SCRAPER — {VM_ID}")
    log(f"   Range: {START_INDEX} to {END_INDEX}")

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

    categories = load_categories()
    if not categories:
        log("❌ No categories loaded")
        return

    # Load existing results to resume
    existing = {}
    content, _, err = gh_read("category_totals.json")
    if not err and content:
        try:
            existing = json.loads(content)
            log(f"📂 Loaded {len(existing)} existing results")
        except:
            pass

    results = dict(existing)  # carry over existing
    consecutive_captchas = 0

    # Filter to our range
    indices = [i for i in range(len(categories)) if START_INDEX <= i <= END_INDEX]
    # Skip already done
    todo = [i for i in indices if str(i) not in results]
    log(f"📋 {len(todo)} categories to fetch (skipping {len(indices) - len(todo)} already done)\n")

    for n, idx in enumerate(todo):
        cat = categories[idx]
        main_cat = cat['main_category']
        sub_cat = cat['sub_category']
        cat_url = cat['url']

        log(f"[{n+1}/{len(todo)}] idx={idx} {main_cat} > {sub_cat}")

        total = None
        for attempt in range(1, 4):
            if attempt > 1:
                log(f"   🔄 Retry {attempt}/3")
                reset_session()
                time.sleep(random.uniform(3, 7))
                warmup()

            total, error = fetch_category_total(cat_url)

            if error == "captcha":
                consecutive_captchas += 1
                log(f"   🛑 CAPTCHA ({consecutive_captchas})")
                rotate_vpn()
                time.sleep(5)
                warmup()
                if consecutive_captchas >= 5:
                    log("❌ 5 consecutive CAPTCHAs — saving and stopping")
                    break
                continue

            consecutive_captchas = 0

            if total is not None:
                log(f"   ✅ {total:,} gigs")
                break
            else:
                log(f"   ⚠️ Failed: {error}")

        if consecutive_captchas >= 5:
            break

        results[str(idx)] = {
            "index": idx,
            "main_category": main_cat,
            "sub_category": sub_cat,
            "category_url": cat_url,
            "total_gigs": total,
            "scraped_at": datetime.now(timezone.utc).isoformat()
        }

        # Save every 50 categories
        if (n + 1) % 50 == 0:
            log(f"   💾 Saving checkpoint ({len(results)} total)...")
            content_str = json.dumps(results, indent=2, ensure_ascii=False)
            _, sha, _ = gh_read("category_totals.json")
            gh_write("category_totals.json", content_str, sha=sha,
                     msg=f"[{VM_ID}] totals checkpoint: {len(results)} categories")

        # Delay between requests
        time.sleep(random.uniform(1.5, 3.5))

    # ── Final save to GitHub ──
    log(f"\n💾 Final save: {len(results)} categories")
    content_str = json.dumps(results, indent=2, ensure_ascii=False)
    _, sha, _ = gh_read("category_totals.json")
    ok = gh_write("category_totals.json", content_str, sha=sha,
                  msg=f"[{VM_ID}] totals final: {len(results)} categories")
    if ok:
        log("✅ Saved to GitHub: category_totals.json")
    else:
        with open("category_totals.json", "w", encoding="utf-8") as f:
            f.write(content_str)
        log("💾 Saved locally: category_totals.json")

    # ── Also save CSV locally ──
    csv_path = "category_totals.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["index", "main_category", "sub_category", "category_url", "total_gigs", "keywords_needed"])
        for key in sorted(results.keys(), key=lambda x: int(x)):
            r = results[key]
            total = r.get("total_gigs") or 0
            keywords_needed = max(1, total // 500)
            w.writerow([r["index"], r["main_category"], r["sub_category"], r["category_url"], total, keywords_needed])
    log(f"📄 CSV saved: {csv_path}")

    # ── Summary ──
    totals = [r.get("total_gigs", 0) or 0 for r in results.values()]
    with_data = [t for t in totals if t > 0]
    log(f"\n{'='*50}")
    log(f"Categories with total: {len(with_data)}/{len(results)}")
    if with_data:
        log(f"Min: {min(with_data):,}")
        log(f"Max: {max(with_data):,}")
        log(f"Avg: {sum(with_data)//len(with_data):,}")
        log(f"Sum: {sum(with_data):,}")
        kw = sum(max(1, t // 500) for t in with_data)
        log(f"Total keywords needed (1 per 500): {kw:,}")
    log(f"{'='*50}")

if __name__ == "__main__":
    main()
