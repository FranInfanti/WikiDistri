import os
import logging

_CRASH_SENTINEL_DIR = os.environ.get("CRASH_SENTINEL_DIR", "/tmp/crash_sentinels")
_CONTROL_DIR = os.environ.get("CRASH_CONTROL_DIR", "/crash_control")
_NAME = os.environ.get("CRASH_NAME", "")


def _armed_by_control_file(at: str) -> bool:
    """True (una sola vez) si chaos.py armó este proceso para crashear en `at`."""
    if not _NAME:
        return False

    path = os.path.join(_CONTROL_DIR, _NAME)
    try:
        with open(path) as f:
            target = f.read().strip()
    except OSError:
        return False

    if target != at:
        return False

    try:
        os.remove(path)
    except OSError:
        return False
    return True


def crash(at: str):
    """Simulate a sudden crash (SIGKILL equivalent via os._exit) at a specific point
    for fault-tolerance testing across any service.

    Two triggers:
      * CRASH_AT=<point> in the environment (crashes once, sentinel file in
        CRASH_SENTINEL_DIR so it does not loop on container restart).
      * A control file {CRASH_CONTROL_DIR}/{CRASH_NAME} containing <point>, written
        by chaos.py while the process is running. It is deleted when it fires.
    """
    if _armed_by_control_file(at):
        logging.critical(f"[SIMULATED CRASH] Crashing at point: {at} (control file, os._exit)")
        os._exit(1)

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