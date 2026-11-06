import argparse
import csv
import hashlib
import sys
from collections import Counter
from pathlib import Path

QUERIES = (1, 2, 3, 4, 5)
ORDERED_QUERIES = (4,)

def find_file(folder: Path, query: int) -> Path | None:
    for name in (f"query_{query}.csv",):
        path = folder / name
        if path.is_file():
            return path
    return None

def iter_lines(path: Path):
    with path.open("rb") as f:
        for line in f:
            yield line.rstrip(b"\r\n")


def digest(line: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(line, digest_size=8).digest(), "big")


def count_lines(path: Path, transform=None) -> tuple[Counter, int]:
    counts: Counter = Counter()
    total = 0
    for line in iter_lines(path):
        if transform is not None:
            line = transform(line)
        counts[digest(line)] += 1
        total += 1
    return counts, total


def sample_lines(path: Path, wanted: set[int], limit: int, transform=None) -> list[str]:
    found: list[str] = []
    seen: set[int] = set()
    for line in iter_lines(path):
        if transform is not None:
            line = transform(line)
        h = digest(line)
        if h in wanted and h not in seen:
            seen.add(h)
            found.append(line.decode("utf-8", errors="replace"))
            if len(found) >= limit:
                break
    return found

def normalize_csv(line: bytes) -> bytes:
    if b'"' not in line:
        return line
    try:
        return ",".join(next(csv.reader([line.decode("utf-8")]))).encode("utf-8")
    except (ValueError, StopIteration, UnicodeDecodeError, csv.Error):
        return line

def loose_image_row(line: bytes) -> bytes:
    try:
        name, url, image = next(csv.reader([line.decode("utf-8")]))
    except (ValueError, StopIteration, UnicodeDecodeError):
        return line
    return f"{name}\x00{url}".encode("utf-8")


def compare(dist_path: Path, serial_path: Path, show: int, transform=None):
    out: list[str] = []
    dist, n_dist = count_lines(dist_path, transform)
    serial, n_serial = count_lines(serial_path, transform)
    ok = True

    if n_dist != n_serial:
        ok = False
        out.append(f"    cantidad de líneas distinta: distribuido={n_dist} serial={n_serial}")

    if dist != serial:
        ok = False
        missing = serial - dist
        extra = dist - serial
        out.append(
            f"    contenido distinto: faltan {sum(missing.values())} líneas del serial, "
            f"sobran {sum(extra.values())} del distribuido"
        )
        for line in sample_lines(serial_path, set(missing), show, transform):
            out.append(f"      falta : {line}")
        for line in sample_lines(dist_path, set(extra), show, transform):
            out.append(f"      sobra : {line}")

    return ok, n_dist, n_serial, out


def compare_ordered(dist_path: Path, serial_path: Path, show: int, transform=None):
    out: list[str] = []
    n_dist = n_serial = 0
    diffs = 0

    with dist_path.open("rb") as fd, serial_path.open("rb") as fs:
        while True:
            ld, ls = fd.readline(), fs.readline()
            if not ld and not ls:
                break
            if ld:
                n_dist += 1
            if ls:
                n_serial += 1
            ld, ls = ld.rstrip(b"\r\n"), ls.rstrip(b"\r\n")
            if transform is not None:
                ld, ls = transform(ld), transform(ls)
            if ld != ls:
                diffs += 1
                if diffs <= show:
                    out.append(
                        f"      línea {max(n_dist, n_serial)}: "
                        f"serial={ls.decode('utf-8', errors='replace')!r} "
                        f"distribuido={ld.decode('utf-8', errors='replace')!r}"
                    )

    ok = diffs == 0 and n_dist == n_serial
    head: list[str] = []
    if n_dist != n_serial:
        head.append(f"    cantidad de líneas distinta: distribuido={n_dist} serial={n_serial}")
    if diffs:
        head.append(f"    el orden/contenido difiere en {diffs} posiciones (primeras {min(diffs, show)}):")
    return ok, n_dist, n_serial, head + out


def validate(dist_dir, serial_dir, loose_images: bool = False, show: int = 5,
             ordered=ORDERED_QUERIES, normalize: bool = True, stream=None) -> bool:
    stream = stream or sys.stdout
    dist_dir, serial_dir = Path(dist_dir), Path(serial_dir)
    all_ok = True

    for query in QUERIES:
        dist_path = find_file(dist_dir, query)
        serial_path = find_file(serial_dir, query)

        if dist_path is None or serial_path is None:
            all_ok = False
            where = []
            if dist_path is None:
                where.append(f"distribuido ({dist_dir})")
            if serial_path is None:
                where.append(f"serial ({serial_dir})")
            print(f"query {query}: FALLA - no se encontró el archivo en " + " y ".join(where), file=stream)
            partial = dist_dir / f"query_{query}.csv.partial"
            if dist_path is None and partial.exists():
                print(f"    existe {partial.name}: la corrida no terminó bien", file=stream)
            continue

        if query in ordered:
            ok, n_dist, n_serial, details = compare_ordered(
                dist_path, serial_path, show, normalize_csv if normalize else None
            )
        else:
            if query == 2 and loose_images:
                transform = loose_image_row
            else:
                transform = normalize_csv if normalize else None
            ok, n_dist, n_serial, details = compare(dist_path, serial_path, show, transform)

        status = "OK   " if ok else "FALLA"
        print(f"query {query}: {status} (distribuido={n_dist} líneas, serial={n_serial} líneas)", file=stream)
        for line in details:
            print(line, file=stream)
        all_ok = all_ok and ok

    print("RESULTADO:", "todas las queries coinciden" if all_ok else "hay diferencias", file=stream)
    return all_ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dist_dir", help="carpeta con los query_N.csv del sistema distribuido")
    parser.add_argument("serial_dir", help="carpeta con los query_N.csv de la ejecución serial")
    parser.add_argument("--loose-images", action="store_true",
                        help="query 2: ignora el nombre del archivo de imagen (solo compara si hay o no)")
    parser.add_argument("--ordered", type=int, nargs="*", default=list(ORDERED_QUERIES),
                        help="queries donde el orden importa (default: 4)")
    parser.add_argument("--strict-csv", action="store_true",
                        help="no normaliza el quoting CSV (por defecto '\"a,b\",c' == 'a,b,c')")
    parser.add_argument("--show", type=int, default=5, help="líneas de ejemplo por diferencia (default 5)")
    args = parser.parse_args(argv)

    return 0 if validate(args.dist_dir, args.serial_dir, args.loose_images, args.show, tuple(args.ordered), not args.strict_csv) else 1


if __name__ == "__main__":
    raise SystemExit(main())