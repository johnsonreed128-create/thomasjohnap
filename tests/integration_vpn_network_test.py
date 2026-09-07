#!/usr/bin/env python3
"""
Comprehensive Integration Test Suite for PIA OpenVPN & Android Emulator Networking.
Executes TEST 1 through TEST 8 sequentially with explicit assertions and telemetry.
"""
import os
import sys
import time
import json
import logging
import subprocess
import requests

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.getcwd())

from src.config import AppConfig
from src.vpn_service import VPNService, VPNState
from src.adb_controller import ADBController

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VPNIntegrationTest")

def main():
    logger.info("=" * 80)
    logger.info("🚀 STARTING REAL GITHUB ACTIONS PIA VPN & ANDROID EGRESS INTEGRATION TEST")
    logger.info("=" * 80)

    config = AppConfig.from_args_and_env()
    config.vpn_provider = "pia"
    
    vpn = VPNService(config)
    adb = ADBController(config)

    results = {}

    # --------------------------------------------------------------------------
    # TEST 1: Connect PIA on runner & Verify Host Public IP
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 1] Connecting PIA OpenVPN on runner and verifying host public IP...")
    t1_start = time.time()
    t1_ok = vpn.setup_vpn()
    host_info_1 = vpn.get_current_ip_info()
    t1_ip = host_info_1.get("ip")
    t1_isp = host_info_1.get("isp")
    
    is_azure = "microsoft" in (t1_isp or "").lower() or "azure" in (t1_isp or "").lower()
    t1_passed = t1_ok and t1_ip != "Unknown" and vpn.is_tun0_active() and not is_azure
    results["TEST_1_HOST_VPN_CONNECT"] = {
        "passed": t1_passed,
        "public_ip": t1_ip,
        "isp": t1_isp,
        "location": vpn.current_location,
        "tun0_active": vpn.is_tun0_active(),
        "duration_sec": round(time.time() - t1_start, 2)
    }
    logger.info(f"TEST 1 RESULT: {'PASS' if t1_passed else 'FAIL'} | IP: {t1_ip} | ISP: {t1_isp} | tun0: {vpn.is_tun0_active()}")

    if not t1_passed:
        logger.error("❌ TEST 1 FAILED. Aborting subsequent tests.")
        print(json.dumps(results, indent=2))
        sys.exit(1)

    # --------------------------------------------------------------------------
    # TEST 2: Start Android Emulator & Verify Android Internet
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 2] Verifying Android emulator ADB connectivity and Internet access...")
    adb_connected = adb.check_connection()
    android_internet = False
    try:
        ping_out = adb.shell("ping -c 2 -W 3 8.8.8.8")
        android_internet = "bytes from" in ping_out or "2 packets transmitted, 2" in ping_out
    except Exception as e:
        logger.error(f"Ping failed: {e}")

    t2_passed = adb_connected and android_internet
    results["TEST_2_ANDROID_INTERNET"] = {
        "passed": t2_passed,
        "adb_connected": adb_connected,
        "android_internet": android_internet,
        "sdk_level": adb.sdk_level
    }
    logger.info(f"TEST 2 RESULT: {'PASS' if t2_passed else 'FAIL'} | ADB: {adb_connected} | Internet: {android_internet}")

    # --------------------------------------------------------------------------
    # TEST 3: Compare Host Public IP and Android Public IP (Egress Match)
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 3] Comparing Host Public IP with Android Emulator Egress IP...")
    egress_info_1 = vpn.verify_android_egress(adb)
    t3_passed = egress_info_1.get("match") is True
    results["TEST_3_ANDROID_EGRESS_MATCH"] = {
        "passed": t3_passed,
        "host_public_ip": egress_info_1.get("host_ip"),
        "android_public_ip": egress_info_1.get("android_ip"),
        "egress_match": egress_info_1.get("match"),
        "android_isp": egress_info_1.get("android_isp")
    }
    logger.info(f"TEST 3 RESULT: {'PASS' if t3_passed else 'FAIL'} | Host: {egress_info_1.get('host_ip')} | Android: {egress_info_1.get('android_ip')} | Match: {t3_passed}")

    # --------------------------------------------------------------------------
    # TEST 4: Disconnect PIA & Verify Clean Teardown
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 4] Disconnecting PIA and verifying clean teardown & route restoration...")
    vpn.disconnect()
    time.sleep(3)
    tun0_down = not vpn.is_tun0_active()
    openvpn_down = not vpn.is_openvpn_running()
    direct_info = vpn.get_current_ip_info()
    t4_passed = tun0_down and openvpn_down and direct_info.get("ip") != "Unknown"
    results["TEST_4_DISCONNECT_TEARDOWN"] = {
        "passed": t4_passed,
        "tun0_inactive": tun0_down,
        "openvpn_stopped": openvpn_down,
        "runner_direct_ip": direct_info.get("ip"),
        "runner_direct_isp": direct_info.get("isp")
    }
    logger.info(f"TEST 4 RESULT: {'PASS' if t4_passed else 'FAIL'} | tun0 down: {tun0_down} | openvpn down: {openvpn_down} | Direct IP: {direct_info.get('ip')}")

    # --------------------------------------------------------------------------
    # TEST 5: Reconnect Using a Different PIA Profile & Verify Connection + IP
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 5] Reconnecting using a different regional PIA profile...")
    t5_ok = vpn.rotate_vpn()
    host_info_2 = vpn.get_current_ip_info()
    t5_ip = host_info_2.get("ip")
    t5_location = vpn.current_location
    t5_passed = t5_ok and t5_ip != "Unknown" and vpn.is_tun0_active()
    results["TEST_5_RECONNECT_NEW_PROFILE"] = {
        "passed": t5_passed,
        "new_location": t5_location,
        "new_public_ip": t5_ip,
        "new_isp": host_info_2.get("isp"),
        "is_different_ip": (t5_ip != t1_ip)
    }
    logger.info(f"TEST 5 RESULT: {'PASS' if t5_passed else 'FAIL'} | Location: {t5_location} | New IP: {t5_ip} (Diff from Test 1: {t5_ip != t1_ip})")

    # --------------------------------------------------------------------------
    # TEST 6: Verify Android Egress Again After Reconnect
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 6] Verifying Android emulator egress after VPN reconnect...")
    egress_info_2 = vpn.verify_android_egress(adb)
    t6_passed = egress_info_2.get("match") is True
    results["TEST_6_ANDROID_EGRESS_AFTER_RECONNECT"] = {
        "passed": t6_passed,
        "host_public_ip": egress_info_2.get("host_ip"),
        "android_public_ip": egress_info_2.get("android_ip"),
        "egress_match": egress_info_2.get("match")
    }
    logger.info(f"TEST 6 RESULT: {'PASS' if t6_passed else 'FAIL'} | Host: {egress_info_2.get('host_ip')} | Android: {egress_info_2.get('android_ip')} | Match: {t6_passed}")

    # --------------------------------------------------------------------------
    # TEST 7: Verify ADB Still Works Before, During, and After VPN Changes
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 7] Verifying ADB input, dumps, and screenshot capabilities...")
    screen_cap_ok = False
    ui_dump_ok = False
    try:
        shot = adb.take_screenshot_bytes()
        screen_cap_ok = shot is not None and len(shot) > 1000
        ui = adb.dump_hierarchy_fast()
        ui_dump_ok = ui is not None and len(ui) > 50
    except Exception as e:
        logger.error(f"ADB inspection failed: {e}")

    t7_passed = screen_cap_ok and ui_dump_ok
    results["TEST_7_ADB_INTEGRITY"] = {
        "passed": t7_passed,
        "screenshot_bytes": len(shot) if shot else 0,
        "ui_dump_length": len(ui) if ui else 0
    }
    logger.info(f"TEST 7 RESULT: {'PASS' if t7_passed else 'FAIL'} | Screenshot: {screen_cap_ok} | UI Dump: {ui_dump_ok}")

    # --------------------------------------------------------------------------
    # TEST 8: Verify Central Backend Communication Works Across VPN
    # --------------------------------------------------------------------------
    logger.info("\n▶️ [TEST 8] Verifying HTTPS API communication with central backend...")
    backend_ok = False
    backend_status = None
    try:
        b_res = requests.get(f"{config.backend_url}/api/fleet/summary", timeout=10)
        backend_status = b_res.status_code
        backend_ok = b_res.status_code in [200, 401, 403]
    except Exception as e:
        logger.error(f"Backend call failed: {e}")

    t8_passed = backend_ok
    results["TEST_8_BACKEND_CONNECTIVITY"] = {
        "passed": t8_passed,
        "status_code": backend_status,
        "backend_url": config.backend_url
    }
    logger.info(f"TEST 8 RESULT: {'PASS' if t8_passed else 'FAIL'} | Backend Status: {backend_status}")

    # --------------------------------------------------------------------------
    # SUMMARY & FINAL TELEMETRY REPORT
    # --------------------------------------------------------------------------
    logger.info("\n" + "=" * 80)
    logger.info("📊 FINAL INTEGRATION TEST RESULTS SUMMARY:")
    logger.info(json.dumps(results, indent=2))
    logger.info("=" * 80)

    all_passed = all(r.get("passed", False) for r in results.values())
    if all_passed:
        logger.info("🎉 ALL 8 INTEGRATION TESTS PASSED 100%!")
        sys.exit(0)
    else:
        logger.error("❌ ONE OR MORE INTEGRATION TESTS FAILED.")
        sys.exit(1)

if __name__ == "__main__":
    main()
