"""
Crashea procesos cada cierto tiempo en puntos concretos del código (fault_tolerance.crash).
Por defecto NO los levanta de nuevo: solo los frena (los reinicia el watchdog o a mano).
Con --restart levanta el contenedor caído (docker start) a los --restart-min..--restart-max segundos.

Cada ronda: espera --interval (+ jitter), elige un proceso que esté corriendo y un punto,
escribe el punto en {control-dir}/{proceso} (por defecto con docker exec dentro del contenedor) y espera a que el contenedor se detenga.
Si el punto no se alcanza dentro de --timeout, desarma el trigger y sigue.

Ejemplo:
    python chaos.py --interval 30 --jitter 10 --mode random --max-crashes 20
    python chaos.py --restart --restart-min 10 --restart-max 30
    python chaos.py --processes word_reader_0 word_reader_1 --points BEFORE_SAVE_EOF AFTER_EOF_ACK
"""

import argparse
import itertools
import os
import random
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime

PROCESSES = (
    [f"filter_{i}" for i in range(3)]+ 
    [f"image_downloader_{i}" for i in range(3)]+ 
    [f"word_reader_{i}" for i in range(4)]+ 
    [f"sum_count_{i}" for i in range(10)]+
    [f"sum_amount_{i}" for i in range(3)]+
    [f"join_top_{i}" for i in range(3)]+
    [f"average_{i}" for i in range(3)]+
    [f"join_average_{i}" for i in range(10)]
    )

POINTS = [
#   "BEFORE_SAVE_PENDING", todo ok
    "AFTER_SAVE_PENDING", 
#    "AFTER_DATA_ACK", todo ok
    "AFTER_DATA_PROCESSED", "AFTER_DATA_COMPLETED",
#   "BEFORE_SAVE_EOF", "AFTER_SAVE_EOF", "AFTER_EOF_ACK",
#    "AFTER_EOF_COMPLETED", "BEFORE_EOF_COMPLETED", Por el momento dificil hacer un buen timing
]

def log(msg: str):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def is_running(name: str) -> bool:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True,
    )
    return out.returncode == 0 and out.stdout.strip() == "true"


def _exec(name: str, script: str, *args: str) -> bool:
    try:
        out = subprocess.run(["docker", "exec", name, "sh", "-c", script, "sh", *args],
                             capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        return False
    return out.returncode == 0


def arm(via: str, control_dir: str, name: str, point: str) -> bool:
    if via == "exec":
        return _exec(name,
                     'mkdir -p "$1" && echo "$2" > "$1/$3.tmp" && mv "$1/$3.tmp" "$1/$3"',
                     control_dir, point, name)
    os.makedirs(control_dir, exist_ok=True)
    path = os.path.join(control_dir, name)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(point)
    os.replace(tmp, path)
    return True


def disarm(via: str, control_dir: str, name: str):
    if via == "exec":
        _exec(name, 'rm -f "$1/$2"', control_dir, name)
        return
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

def start_container(name: str) -> bool:
    out = subprocess.run(["docker", "start", name], capture_output=True, text=True)
    if out.returncode != 0:
        log(f"no se pudo levantar {name}: {out.stderr.strip()}")
        return False
    return True
class Restarter:
    """Levanta contenedores caídos tras un delay al azar, sin frenar las rondas de chaos."""

    def __init__(self, rng: random.Random, lo: float, hi: float):
        self.rng, self.lo, self.hi = rng, lo, hi
        self.pending: dict[str, threading.Timer] = {}
        self.restarted = Counter()
        self.failed = Counter()
        self.lock = threading.Lock()

    def schedule(self, name: str):
        delay = self.rng.uniform(self.lo, self.hi)
        timer = threading.Timer(delay, self._run, args=(name,))
        timer.daemon = True
        with self.lock:
            self.pending[name] = timer
        timer.start()
        log(f"{name} se levanta en {delay:.0f}s")

    def _run(self, name: str):
        with self.lock:
            self.pending.pop(name, None)
        if is_running(name):
            return
        if start_container(name):
            self.restarted[name] += 1
            log(f"RESTART: {name} levantado")
        else:
            self.failed[name] += 1

    def flush(self):
        """Al salir: levanta ya los que quedaron esperando, para no dejar el sistema a medias."""
        with self.lock:
            names = list(self.pending)
            for t in self.pending.values():
                t.cancel()
            self.pending.clear()
        for name in names:
            self._run(name)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--processes", nargs="+", default=PROCESSES, help="nombres de contenedor")
    ap.add_argument("--points", nargs="+", default=POINTS, help="puntos de fault_tolerance.crash")
    ap.add_argument("--interval", type=float, default=10, help="segundos entre crashes")
    ap.add_argument("--jitter", type=float, default=0, help="se suma un valor al azar entre 0 y jitter")
    ap.add_argument("--timeout", type=float, default=10, help="segundos esperando que se alcance el punto")
    ap.add_argument("--mode", choices=("random", "round-robin"), default="random")
    ap.add_argument("--max-crashes", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--min-alive", type=int, default=1, help="no crashea si quedarían menos de N vivos")
    ap.add_argument("--restart", action="store_true", help="vuelve a levantar el contenedor que se crasheó")
    ap.add_argument("--restart-min", type=float, default=10, help="mínimo de segundos antes de levantarlo")
    ap.add_argument("--restart-max", type=float, default=30, help="máximo de segundos antes de levantarlo")
    ap.add_argument("--arm-via", choices=("exec", "volume"), default="exec",
                    help="exec: escribe el archivo con docker exec (sin volumen); volume: en --control-dir del host")
    ap.add_argument("--control-dir", default=None,
                    help="exec: ruta dentro del contenedor (default /crash_control, = CRASH_CONTROL_DIR); "
                         "volume: ruta en el host (default ./crash_control)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    if args.restart_min > args.restart_max:
        ap.error("--restart-min no puede ser mayor que --restart-max")
    via = args.arm_via
    if args.control_dir is None:
        args.control_dir = "/crash_control" if via == "exec" else "./crash_control"

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)

    rng = random.Random(args.seed)
    rr_procs = itertools.cycle(args.processes)
    rr_points = itertools.cycle(args.points)
    crashed, missed = Counter(), Counter()
    total = 0
    restarter = Restarter(rng, args.restart_min, args.restart_max) if args.restart else None

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

            if not arm(via, args.control_dir, name, point):
                log(f"no se pudo armar {name} (docker exec falló); salteo")
                continue
            log(f"armado {name} en {point}; espero hasta {args.timeout}s")

            if wait_until_stopped(name, args.timeout):
                total += 1
                crashed[(name, point)] += 1
                log(f"CRASH #{total}: {name} se detuvo en {point}")
                if via == "volume":
                    disarm(via, args.control_dir, name)
                if restarter:
                    restarter.schedule(name)
            else:
                disarm(via, args.control_dir, name)
                missed[(name, point)] += 1
                log(f"no se alcanzó {point} en {name}; desarmado")
    except KeyboardInterrupt:
        log("interrumpido")
    finally:
        for p in args.processes:
            disarm(via, args.control_dir, p)
        if restarter:
            restarter.flush()
        log(f"resumen: {total} crashes, {sum(missed.values())} puntos no alcanzados")
        for (n, pt), c in sorted(crashed.items()):
            log(f"  crash   {n:<16} {pt:<22} x{c}")
        if restarter:
            log(f"  reinicios: {sum(restarter.restarted.values())} ok, {sum(restarter.failed.values())} fallidos")
        for (n, pt), c in sorted(missed.items()):
            log(f"  no hit  {n:<16} {pt:<22} x{c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())