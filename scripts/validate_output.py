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

def compare(first_path: Path, second_path: Path, show: int, transform=None):
    out: list[str] = []
    first, n_first = count_lines(first_path, transform)
    second, n_second = count_lines(second_path, transform)
    ok = True

    if n_first != n_second:
        ok = False
        out.append(f"    cantidad de líneas distinta: primero={n_first} segundo={n_second}")

    if first != second:
        ok = False
        missing = second - first
        extra = first - second
        out.append(
            f"    contenido distinto: faltan {sum(missing.values())} líneas del segundo, "
            f"sobran {sum(extra.values())} del primero"
        )
        for line in sample_lines(second_path, set(missing), show, transform):
            out.append(f"      falta : {line}")
        for line in sample_lines(first_path, set(extra), show, transform):
            out.append(f"      sobra : {line}")

    return ok, n_first, n_second, out

def compare_ordered(first_path: Path, second_path: Path, show: int, transform=None):
    out: list[str] = []
    n_first = n_second = 0
    diffs = 0

    with first_path.open("rb") as fd, second_path.open("rb") as fs:
        while True:
            ld, ls = fd.readline(), fs.readline()
            if not ld and not ls:
                break
            if ld:
                n_first += 1
            if ls:
                n_second += 1
            ld, ls = ld.rstrip(b"\r\n"), ls.rstrip(b"\r\n")
            if transform is not None:
                ld, ls = transform(ld), transform(ls)
            if ld != ls:
                diffs += 1
                if diffs <= show:
                    out.append(
                        f"      línea {max(n_first, n_second)}: "
                        f"second={ls.decode('utf-8', errors='replace')!r} "
                        f"first={ld.decode('utf-8', errors='replace')!r}"
                    )

    ok = diffs == 0 and n_first == n_second
    head: list[str] = []
    if n_first != n_second:
        head.append(f"    cantidad de líneas distinta: primero={n_first} segundo={n_second}")
    if diffs:
        head.append(f"    el orden/contenido difiere en {diffs} posiciones (primeras {min(diffs, show)}):")
    return ok, n_first, n_second, head + out

def validate(first_dir, second_dir, loose_images: bool = False, show: int = 5,
            normalize: bool = True, stream=None) -> bool:
    stream = stream or sys.stdout
    first_dir, second_dir = Path(first_dir), Path(second_dir)
    all_ok = True

    for query in QUERIES:
        first_path = find_file(first_dir, query)
        second_path = find_file(second_dir, query)

        if first_path is None or second_path is None:
            all_ok = False
            where = []
            if first_path is None:
                where.append(f"primero ({first_dir})")
            if second_path is None:
                where.append(f"segundo ({second_dir})")
            print(f"query {query}: FALLA - no se encontró el archivo en " + " y ".join(where), file=stream)
            partial = first_dir / f"query_{query}.csv.partial"
            if first_path is None and partial.exists():
                print(f"    existe {partial.name}: la corrida no terminó bien", file=stream)
            continue

        if query in ORDERED_QUERIES:
            ok, n_first, n_second, details = compare_ordered(
                first_path, second_path, show, normalize_csv if normalize else None
            )
        else:
            if query == 2 and loose_images:
                transform = loose_image_row
            else:
                transform = normalize_csv if normalize else None
            ok, n_first, n_second, details = compare(first_path, second_path, show, transform)

        status = "OK   " if ok else "FALLA"
        print(f"query {query}: {status} (primero={n_first} líneas, segundo={n_second} líneas)", file=stream)
        for line in details:
            print(line, file=stream)
        all_ok = all_ok and ok

    print("RESULTADO:", "todas las queries coinciden" if all_ok else "hay diferencias", file=stream)
    return all_ok

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("first_dir", help="una carpeta con los query_N.csv")
    parser.add_argument("second_dir", help="otra carpeta con los query_N.csv")
    parser.add_argument("--loose-images", action="store_true",
                        help="query 2: ignora el nombre del archivo de imagen")
    parser.add_argument("--strict-csv", action="store_true",
                        help="no normaliza el quoting CSV (por defecto '\"a,b\",c' == 'a,b,c')")
    parser.add_argument("--show", type=int, default=5, help="líneas de ejemplo por diferencia (default 5)")
    args = parser.parse_args(argv)

    return 0 if validate(args.first_dir, args.second_dir, args.loose_images, args.show, not args.strict_csv) else 1

if __name__ == "__main__":
    SystemExit(main())