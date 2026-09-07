import os
import time
import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Optional, List

logger = logging.getLogger("EmulatorProvider")

class EmulatorProvider(ABC):
    """
    Abstract Base Class defining the contract for Android emulator runtime providers.
    All upper layers (ADBController, AutoLogin, Scrcpy, Telemetry) communicate through
    the target resolved by this provider.
    """

    def __init__(self, config):
        self.config = config

    @abstractmethod
    def start(self) -> bool:
        """Starts or connects to the emulator instance."""
        pass

    @abstractmethod
    def stop(self) -> bool:
        """Stops or disconnects from the emulator instance."""
        pass

    @abstractmethod
    def restart(self) -> bool:
        """Restarts the emulator instance."""
        pass

    @abstractmethod
    def wait_until_ready(self, timeout_seconds: int = 120) -> bool:
        """Blocks until the emulator has completed booting and ADB is ready."""
        pass

    @abstractmethod
    def get_adb_target(self) -> Optional[str]:
        """Returns the specific ADB serial or host:port string."""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """Checks if the emulator process / ADB connection is currently active."""
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Performs teardown / cleanup of temporary emulator resources."""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Returns provider identifier name ('avd' or 'ldplayer')."""
        pass


class AVDProvider(EmulatorProvider):
    """
    Android Virtual Device (AVD / QEMU / KVM) Provider.
    Wraps and preserves 100% of the existing AVD behavior on Linux cloud runners and local environments.
    """

    def __init__(self, config):
        super().__init__(config)
        self.serial: Optional[str] = getattr(config, "adb_device_id", None)

    def get_provider_name(self) -> str:
        return "avd"

    def get_adb_target(self) -> Optional[str]:
        if self.serial:
            return self.serial
        # Auto-discover emulator serial from adb devices if present
        try:
            res = subprocess.run(["adb", "devices"], capture_output=True, text=True, timeout=5)
            lines = [l.strip() for l in res.stdout.strip().split("\n")[1:] if l.strip()]
            for line in lines:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "device":
                    if parts[0].startswith("emulator-"):
                        self.serial = parts[0]
                        return self.serial
            # If no emulator- prefix, take first ready device
            for line in lines:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "device":
                    self.serial = parts[0]
                    return self.serial
        except Exception as e:
            logger.debug(f"AVD target discovery notice: {e}")
        return getattr(self.config, "adb_device_id", None)

    def is_running(self) -> bool:
        target = self.get_adb_target()
        if not target:
            return False
        try:
            res = subprocess.run(
                ["adb", "-s", target, "shell", "getprop", "sys.boot_completed"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return res.stdout.strip() == "1"
        except Exception:
            return False

    def start(self) -> bool:
        # On CI/CD runners, AVD is booted by runner workflow.
        # On local machines, start confirms connectivity.
        target = self.get_adb_target()
        logger.info(f"AVD Provider initialized with target: {target or 'auto-discovery'}")
        return True

    def wait_until_ready(self, timeout_seconds: int = 120) -> bool:
        start_t = time.time()
        logger.info(f"Waiting for AVD emulator to complete boot (timeout: {timeout_seconds}s)...")
        while time.time() - start_t < timeout_seconds:
            if self.is_running():
                logger.info(f"[+] AVD emulator ({self.get_adb_target()}) is online and boot completed.")
                return True
            time.sleep(2)
        logger.error("[-] Timeout waiting for AVD emulator boot completion.")
        return False

    def stop(self) -> bool:
        target = self.get_adb_target()
        if target and target.startswith("emulator-"):
            try:
                subprocess.run(["adb", "-s", target, "emu", "kill"], capture_output=True, timeout=5)
            except Exception:
                pass
        return True

    def restart(self) -> bool:
        self.stop()
        time.sleep(2)
        return self.start()

    def cleanup(self) -> None:
        pass


class LDPlayerProvider(EmulatorProvider):
    """
    LDPlayer Android Emulator Provider.
    Communicates via standard ADB over TCP (default 127.0.0.1:5555 / 127.0.0.1:5557 / emulator serial).
    Designed for GitHub-hosted Windows (windows-latest) and local Windows host environments.
    """

    def __init__(self, config):
        super().__init__(config)
        self.instance: int = int(getattr(config, "ldplayer_instance", 0))
        self.adb_host: str = getattr(config, "ldplayer_adb_host", "127.0.0.1")
        # Standard LDPlayer port convention: instance 0 = 5555, instance 1 = 5557, etc.
        default_port = 5555 + (self.instance * 2)
        cfg_port = getattr(config, "ldplayer_adb_port", None)
        self.adb_port: int = int(cfg_port) if cfg_port is not None else default_port
        self.ldplayer_path: Optional[str] = getattr(config, "ldplayer_path", None)
        if not self.ldplayer_path and os.name == "nt":
            common_ld_paths = [
                r"C:\LDPlayer\LDPlayer9\ldconsole.exe",
                r"C:\LDPlayer\LDPlayer9\dnconsole.exe",
                r"C:\leidian\LDPlayer9\ldconsole.exe",
                r"C:\leidian\LDPlayer9\dnconsole.exe",
                r"C:\Program Files\LDPlayer\LDPlayer9\ldconsole.exe",
                r"C:\Program Files (x86)\LDPlayer\LDPlayer9\ldconsole.exe",
                r"D:\LDPlayer\LDPlayer9\ldconsole.exe"
            ]
            for p in common_ld_paths:
                if os.path.exists(p):
                    self.ldplayer_path = p
                    break

        self.target: str = getattr(config, "adb_device_id", None) or f"{self.adb_host}:{self.adb_port}"

    def get_provider_name(self) -> str:
        return "ldplayer"

    def _find_adb_cmd(self) -> str:
        """Finds active adb executable name or full path."""
        if os.name == "nt":
            common_adb_paths = [
                os.path.expandvars(r"%LOCALAPPDATA%\Android\Sdk\platform-tools\adb.exe"),
                r"C:\Program Files\Android\platform-tools\adb.exe",
                r"C:\LDPlayer\LDPlayer9\adb.exe",
                r"C:\leidian\LDPlayer9\adb.exe",
            ]
            for p in common_adb_paths:
                if os.path.exists(p):
                    return p
        return "adb"

    def _validate_environment(self) -> bool:
        """
        Validates whether the current runner host environment supports LDPlayer.
        LDPlayer is a native Windows virtualization application.
        """
        if os.name != "nt" and os.getenv("ALLOW_LDPLAYER_NON_WINDOWS", "false").lower() not in ("true", "1"):
            logger.error(
                "[-] Incompatible Runner Environment: LDPlayer requires a Windows runner host (e.g. windows-latest). "
                "For standard Linux/Ubuntu GitHub Actions runners, please set EMULATOR_PROVIDER=avd."
            )
            return False
        return True

    def _ensure_adb_connected(self) -> bool:
        """Attempts connection to LDPlayer's ADB TCP endpoint if not already listed."""
        adb_bin = self._find_adb_cmd()
        try:
            res = subprocess.run([adb_bin, "devices"], capture_output=True, text=True, timeout=5)
            # Check if target is already in device list
            for line in res.stdout.strip().split("\n")[1:]:
                if self.target in line and "\tdevice" in line:
                    return True
                # Also support emulator-5554 naming if LDPlayer registered under emulator alias
                if f"emulator-{self.adb_port - 1}" in line and "\tdevice" in line:
                    self.target = f"emulator-{self.adb_port - 1}"
                    return True

            # Attempt ADB connect to TCP port
            logger.info(f"Connecting ADB to LDPlayer instance at {self.target}...")
            conn_res = subprocess.run([adb_bin, "connect", self.target], capture_output=True, text=True, timeout=10)
            logger.info(f"ADB connect response: {conn_res.stdout.strip()}")
            return "connected" in conn_res.stdout.lower() or "already connected" in conn_res.stdout.lower()
        except Exception as e:
            logger.debug(f"LDPlayer ADB connect attempt notice: {e}")
            return False

    def get_adb_target(self) -> Optional[str]:
        self._ensure_adb_connected()
        return self.target

    def is_running(self) -> bool:
        target = self.get_adb_target()
        if not target:
            return False
        try:
            adb_bin = self._find_adb_cmd()
            res = subprocess.run(
                [adb_bin, "-s", target, "shell", "getprop", "sys.boot_completed"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return res.stdout.strip() == "1"
        except Exception:
            return False

    def start(self) -> bool:
        if not self._validate_environment():
            raise RuntimeError(
                "LDPlayer provider requires a Windows runner host (e.g. windows-latest). "
                "Use EMULATOR_PROVIDER=avd for Linux runners."
            )

        # If ldconsole path is configured and device not yet running, invoke launch command
        if self.ldplayer_path and os.path.exists(self.ldplayer_path) and not self.is_running():
            logger.info(f"Launching LDPlayer instance {self.instance} via ldconsole ({self.ldplayer_path})...")
            try:
                subprocess.run([self.ldplayer_path, "launch", "--index", str(self.instance)], timeout=15)
            except Exception as e:
                logger.warning(f"Could not trigger LDPlayer console launch: {e}")

        return self._ensure_adb_connected()

    def wait_until_ready(self, timeout_seconds: int = 120) -> bool:
        start_t = time.time()
        logger.info(f"Waiting for LDPlayer ({self.target}) to become ready (timeout: {timeout_seconds}s)...")
        while time.time() - start_t < timeout_seconds:
            self._ensure_adb_connected()
            if self.is_running():
                logger.info(f"[+] LDPlayer ({self.target}) is online and ready!")
                return True
            time.sleep(3)
        logger.error(f"[-] Timeout waiting for LDPlayer ({self.target}) ready status.")
        return False

    def stop(self) -> bool:
        if self.ldplayer_path and os.path.exists(self.ldplayer_path):
            try:
                subprocess.run([self.ldplayer_path, "quit", "--index", str(self.instance)], timeout=10)
                return True
            except Exception:
                pass
        return True

    def restart(self) -> bool:
        self.stop()
        time.sleep(3)
        return self.start()

    def cleanup(self) -> None:
        pass


def create_emulator_provider(config) -> EmulatorProvider:
    """
    Factory function to instantiate the configured EmulatorProvider.
    Defaults to AVDProvider if unspecified or set to 'avd'.
    Raises ValueError with a clear, descriptive message for unsupported providers.
    """
    raw_provider = getattr(config, "emulator_provider", "avd")
    provider_type = (raw_provider or "avd").lower().strip()

    if provider_type == "avd" or not provider_type:
        return AVDProvider(config)
    elif provider_type == "ldplayer":
        return LDPlayerProvider(config)
    else:
        supported = ["avd", "ldplayer"]
        raise ValueError(
            f"Unsupported EMULATOR_PROVIDER '{raw_provider}'. "
            f"Supported providers are: {', '.join(supported)}. (Default: 'avd')"
        )
