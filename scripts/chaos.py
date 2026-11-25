"""
Crashea procesos cada cierto tiempo en puntos concretos del código (fault_tolerance.crash).
NO los levanta de nuevo: solo los frena. Los reinicia quien corresponda (watchdog, a mano).

Cada ronda: espera --interval (+ jitter), elige un proceso que esté corriendo y un punto,
escribe el punto en {control-dir}/{proceso} y espera a que el contenedor se detenga.
Si el punto no se alcanza dentro de --timeout, desarma el trigger y sigue.

Ejemplo:
    python chaos.py --interval 30 --jitter 10 --mode random --max-crashes 20
    python chaos.py --processes word_reader_0 word_reader_1 --points BEFORE_SAVE_EOF AFTER_EOF_ACK
"""

import argparse
import itertools
import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

PROCESSES = [f"word_reader_{i}" for i in range(4)]
POINTS = [
    "BEFORE_SAVE_PENDING", "AFTER_SAVE_PENDING", "AFTER_DATA_ACK",
    "AFTER_DATA_PROCESSED", "AFTER_DATA_COMPLETED",
    "BEFORE_SAVE_EOF", "AFTER_SAVE_EOF", "AFTER_EOF_ACK",
    "AFTER_EOF_COMPLETED", "BEFORE_EOF_COMPLETED",
]


def log(msg: str):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def is_running(name: str) -> bool:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True,
    )
    return out.returncode == 0 and out.stdout.strip() == "true"


def arm(control_dir: str, name: str, point: str):
    os.makedirs(control_dir, exist_ok=True)
    path = os.path.join(control_dir, name)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(point)
    os.replace(tmp, path)


def disarm(control_dir: str, name: str):
    try:
        os.remove(os.path.join(control_dir, name))
    except FileNotFoundError:
        pass


def wait_until_stopped(name: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_running(name):
            return True
        time.sleep(0.5)
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--processes", nargs="+", default=PROCESSES, help="nombres de contenedor")
    ap.add_argument("--points", nargs="+", default=POINTS, help="puntos de fault_tolerance.crash")
    ap.add_argument("--interval", type=float, default=15, help="segundos entre crashes")
    ap.add_argument("--jitter", type=float, default=0, help="se suma un valor al azar entre 0 y jitter")
    ap.add_argument("--timeout", type=float, default=60, help="segundos esperando que se alcance el punto")
    ap.add_argument("--mode", choices=("random", "round-robin"), default="random")
    ap.add_argument("--max-crashes", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--min-alive", type=int, default=1, help="no crashea si quedarían menos de N vivos")
    ap.add_argument("--control-dir", default="./crash_control")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)

    rng = random.Random(args.seed)
    rr_procs = itertools.cycle(args.processes)
    rr_points = itertools.cycle(args.points)
    crashed, missed = Counter(), Counter()
    total = 0

    log(f"procesos={args.processes} puntos={args.points} cada {args.interval}s (+0..{args.jitter}s)")
    try:
        while not args.max_crashes or total < args.max_crashes:
            time.sleep(args.interval + rng.uniform(0, args.jitter))

            running = [p for p in args.processes if is_running(p)]
            if len(running) <= args.min_alive:
                log(f"solo {len(running)} vivos (min-alive={args.min_alive}); salteo la ronda")
                continue

            if args.mode == "random":
                name, point = rng.choice(running), rng.choice(args.points)
            else:
                name = next(p for p in rr_procs if p in running)
                point = next(rr_points)

            arm(args.control_dir, name, point)
            log(f"armado {name} en {point}; espero hasta {args.timeout}s")

            if wait_until_stopped(name, args.timeout):
                total += 1
                crashed[(name, point)] += 1
                log(f"CRASH #{total}: {name} se detuvo en {point}")
            else:
                disarm(args.control_dir, name)
                missed[(name, point)] += 1
                log(f"no se alcanzó {point} en {name}; desarmado")
    except KeyboardInterrupt:
        log("Interrupt")
    finally:
        for p in args.processes:
            disarm(args.control_dir, p)
        log(f"resumen: {total} crashes, {sum(missed.values())} puntos no alcanzados")
        for (n, pt), c in sorted(crashed.items()):
            log(f"  crash   {n:<16} {pt:<22} x{c}")
        for (n, pt), c in sorted(missed.items()):
            log(f"  no hit  {n:<16} {pt:<22} x{c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())