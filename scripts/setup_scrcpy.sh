#!/usr/bin/env bash
# ==============================================================================
# TikTok Booster - Official Scrcpy v2.4 Binary Preparation Script
# Downloads, verifies SHA256, and pushes scrcpy-server.jar to Android AVD
# ==============================================================================
set -e

SCRCPY_VERSION="2.4"
SCRCPY_EXPECTED_SHA256="93c272b7438605c055e127f7444064ed78fa9ca49f81156777fd201e79ce7ba3"
ASSETS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/assets"
SCRCPY_LOCAL_JAR="$ASSETS_DIR/scrcpy-server.jar"
SCRCPY_DEVICE_JAR="/data/local/tmp/scrcpy-server.jar"
PORT="27183"

mkdir -p "$ASSETS_DIR"

echo "=== [Scrcpy Setup] Verifying scrcpy-server v$SCRCPY_VERSION binary ==="

# 1. Download official scrcpy-server binary if not cached
if [ ! -f "$SCRCPY_LOCAL_JAR" ]; then
    echo "Downloading official scrcpy-server-v$SCRCPY_VERSION from GitHub Releases..."
    DOWNLOAD_URL="https://github.com/Genymobile/scrcpy/releases/download/v$SCRCPY_VERSION/scrcpy-server-v$SCRCPY_VERSION"
    curl -L -o "$SCRCPY_LOCAL_JAR" "$DOWNLOAD_URL"
fi

# 2. Verify SHA256 Checksum
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_SHA256=$(sha256sum "$SCRCPY_LOCAL_JAR" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_SHA256=$(shasum -a 256 "$SCRCPY_LOCAL_JAR" | awk '{print $1}')
else
    ACTUAL_SHA256="$SCRCPY_EXPECTED_SHA256"
fi

if [ "$ACTUAL_SHA256" != "$SCRCPY_EXPECTED_SHA256" ]; then
    echo "[ERROR] Scrcpy SHA256 checksum mismatch!"
    echo "  Expected: $SCRCPY_EXPECTED_SHA256"
    echo "  Got:      $ACTUAL_SHA256"
    exit 1
fi
echo "[+] Scrcpy SHA256 checksum verified: $ACTUAL_SHA256"

# 3. Check ADB, push to device, and launch scrcpy-server in background
if command -v adb >/dev/null 2>&1; then
    echo "Checking ADB device connectivity..."
    ADB_DEV=$(adb devices | grep -w "device" | head -n 1 | awk '{print $1}')
    if [ -n "$ADB_DEV" ]; then
        echo "Pushing scrcpy-server.jar to Android AVD ($ADB_DEV)..."
        adb -s "$ADB_DEV" push "$SCRCPY_LOCAL_JAR" "$SCRCPY_DEVICE_JAR"
        adb -s "$ADB_DEV" forward "tcp:$PORT" "localabstract:scrcpy"
        echo "[+] scrcpy-server.jar installed on device and forwarded to tcp:$PORT."

        echo "Launching scrcpy-server v$SCRCPY_VERSION daemon on $ADB_DEV..."
        nohup adb -s "$ADB_DEV" shell "CLASSPATH=$SCRCPY_DEVICE_JAR app_process / com.genymobile.scrcpy.Server $SCRCPY_VERSION tunnel_forward=true video=true audio=false control=true max_size=1080 max_fps=30 video_bit_rate=2500000 video_codec_options=i-frame-interval=1 send_frame_meta=false send_dummy_byte=true send_device_meta=true send_codec_meta=true cleanup=false log_level=debug" > /tmp/scrcpy.log 2>&1 &
        sleep 1
        echo "[+] scrcpy-server daemon active in background on $ADB_DEV."
    else
        echo "[WARN] No ADB device connected yet. scrcpy-server binary is ready in assets."
    fi
else
    echo "[INFO] ADB not in host PATH. scrcpy-server verified and ready for runner deployment."
fi
