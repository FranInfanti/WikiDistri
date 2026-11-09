import io
import sys
import csv
import hashlib
import argparse

from pathlib import Path
from collections import Counter

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

def image_presence_row(line: bytes) -> bytes:
    try:
        name, url, image = next(csv.reader([line.decode("utf-8")]))
    except (ValueError, StopIteration, UnicodeDecodeError, csv.Error):
        return line
    normalized = io.StringIO(newline="")
    csv.writer(normalized, lineterminator="").writerow([name, url, bool(image)])
    return normalized.getvalue().encode("utf-8")

def validate_image_files(csv_path: Path, image_dir: Path, show: int) -> list[str]:
    errors: list[str] = []
    image_root = image_dir.resolve()

    with csv_path.open(encoding="utf-8", newline="") as file:
        for line_number, row in enumerate(csv.reader(file), start=1):
            if len(row) != 3 or not row[2]:
                continue

            image_path = Path(row[2])
            resolved_path = (image_dir / image_path).resolve()
            if image_path.is_absolute() or not resolved_path.is_relative_to(image_root):
                errors.append(f"    line {line_number}: image path is outside the output directory: {row[2]}")
            elif not resolved_path.is_file():
                errors.append(f"    line {line_number}: image file does not exist: {row[2]}")
            elif resolved_path.stat().st_size == 0:
                errors.append(f"    line {line_number}: image file is empty: {row[2]}")

            if len(errors) >= show:
                break

    return errors

def compare(first_path: Path, second_path: Path, show: int, transform=None):
    out: list[str] = []
    first, n_first = count_lines(first_path, transform)
    second, n_second = count_lines(second_path, transform)
    ok = True

    if n_first != n_second:
        ok = False
        out.append(f"    line count differs: first={n_first}, second={n_second}")

    if first != second:
        ok = False
        missing = second - first
        extra = first - second
        out.append(
            f"    content differs: {sum(missing.values())} lines missing from second, "
            f"{sum(extra.values())} extra in first"
        )
        for line in sample_lines(second_path, set(missing), show, transform):
            out.append(f"      missing: {line}")
        for line in sample_lines(first_path, set(extra), show, transform):
            out.append(f"      extra: {line}")

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
                        f"      line {max(n_first, n_second)}: "
                        f"second={ls.decode('utf-8', errors='replace')!r} "
                        f"first={ld.decode('utf-8', errors='replace')!r}"
                    )

    ok = diffs == 0 and n_first == n_second
    head: list[str] = []
    if n_first != n_second:
        head.append(f"    line count differs: first={n_first}, second={n_second}")
    if diffs:
        head.append(f"    order/content differs at {diffs} positions (first {min(diffs, show)}):")
    return ok, n_first, n_second, head + out

def validate(first_dir, second_dir, show: int = 5,
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
                where.append(f"first ({first_dir})")
            if second_path is None:
                where.append(f"second ({second_dir})")
            print(f"query {query}: FAIL - file not found in " + " and ".join(where), file=stream)
            partial = first_dir / f"query_{query}.csv.partial"
            if first_path is None and partial.exists():
                print(f"    {partial.name} exists: the run did not finish successfully", file=stream)
            continue

        if query in ORDERED_QUERIES:
            ok, n_first, n_second, details = compare_ordered(
                first_path, second_path, show, normalize_csv if normalize else None
            )
        else:
            transform = image_presence_row if query == 2 else normalize_csv if normalize else None
            ok, n_first, n_second, details = compare(first_path, second_path, show, transform)

        if query == 2:
            image_errors = validate_image_files(first_path, first_dir, show)
            if image_errors:
                ok = False
                details.extend(image_errors)

        status = "OK   " if ok else "FAIL "
        print(f"query {query}: {status} (first={n_first} lines, second={n_second} lines)", file=stream)
        for line in details:
            print(line, file=stream)
        all_ok = all_ok and ok

    print("RESULT:", "OK; all queries match" if all_ok else "FAIL;differences found", file=stream)
    return all_ok

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("first_dir", help="directory containing the query_N.csv files to validate")
    parser.add_argument("second_dir", help="directory containing the expected query_N.csv files")
    parser.add_argument("--strict-csv", action="store_true",
                        help="do not normalize CSV quoting (by default '\"a,b\",c' == 'a,b,c')")
    parser.add_argument("--show", type=int, default=5, help="examples per difference (default: 5)")
    args = parser.parse_args(argv)

    return 0 if validate(args.first_dir, args.second_dir, args.show, not args.strict_csv) else 1

if __name__ == "__main__":
    SystemExit(main())