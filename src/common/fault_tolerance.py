import os
import logging

_CRASH_SENTINEL_DIR = os.environ.get("CRASH_SENTINEL_DIR", "/tmp/crash_sentinels")

def crash(at: str):
    """Simulate a sudden crash (SIGKILL equivalent via os._exit) at a specific point
    for fault-tolerance testing across any service.

    Triggered when the CRASH_AT environment variable matches `name`.
    Uses an independent sentinel file in `/tmp/crash_sentinels` to ensure
    it only crashes once and does not loop indefinitely on container restart.
    """
    target = os.environ.get("CRASH_AT")
    if not target or target != at:
        return

    os.makedirs(_CRASH_SENTINEL_DIR, exist_ok=True)
    flag_file = os.path.join(_CRASH_SENTINEL_DIR, f".crashed_{at}")

    if not os.path.exists(flag_file):
        try:
            with open(flag_file, "w") as f:
                f.write("1")
        except Exception:
            pass
        logging.critical(f"[SIMULATED CRASH] Crashing at point: {at} (os._exit)")
        os._exit(1)
