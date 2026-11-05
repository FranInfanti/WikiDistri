"""Descarga los Parquet de Wikipedia a input y permite exportarlos a CSV."""

import os
import kagglehub

from pathlib import Path
from contextlib import closing

DATASET = "wikimedia-foundation/wikipedia-structured-contents"
LANGUAGES = ("en", "fr")
FILES_PER_LANGUAGE = 5

COLUMNS = (
    "abstract",
    "date_modified",
    "image",
    "in_language",
    "name",
    "sections",
    "url",
)

INPUT_ROOT = Path(__file__).resolve().parents[1] / "input"

def _download_from_kaggle(path = None):
    requested_file = path or "dataset completo"
    print(f"Started download from Kaggle: {requested_file}", flush=True)
    if path is None:
        downloaded_path = Path(kagglehub.dataset_download(DATASET))
    else:
        downloaded_path = Path(kagglehub.dataset_download(DATASET, path=path))

    if downloaded_path.is_file():
        size = downloaded_path.stat().st_size
        print(
            f"Download finished: {downloaded_path} ({size:,} bytes)",
            flush=True,
        )
    else:
        print(f"Download finished: {downloaded_path}", flush=True)
    return downloaded_path

def _language_for_path(path, languages):
    for part in path.parts:
        part = part.lower()
        for language in languages:
            if part == f"{language}wiki" or part.startswith(f"{language}wiki_"):
                return language
    return None

def _interleaved_files(files_by_language, languages):
    for index in range(max(map(len, files_by_language.values()), default=0)):
        for language in languages:
            if index < len(files_by_language[language]):
                yield files_by_language[language][index]

def parquet_files(input_file: str = None, stop = None):
    """Entrega archivos locales o descarga los Parquet necesarios a input."""

    if input_file:
        yield Path(input_file)
        return

    languages = tuple(
        value.strip()
        for value in os.environ.get("WIKI_LANGUAGES", ",".join(LANGUAGES)).split(",")
    )

    if not languages or any(language not in LANGUAGES for language in languages):
        raise ValueError("WIKI_LANGUAGES debe contener en, fr o en,fr")

    files_per_language = os.environ.get("FILES_PER_LANGUAGE", str(FILES_PER_LANGUAGE))
    count = None if files_per_language == "all" else int(files_per_language)
    if count is not None and count < 1:
        raise ValueError("FILES_PER_LANGUAGE debe ser mayor que cero o all")

    input_root = Path(
        os.environ.get("INPUT_DIR")
        or os.environ.get("DOWNLOAD_DIR", str(INPUT_ROOT))
    )
    input_root.mkdir(parents=True, exist_ok=True)

    os.environ["KAGGLEHUB_CACHE"] = str(input_root)

    if count is None:
        dataset_dir = _download_from_kaggle()
        files_by_language = {}

        for language in languages:
            pattern = f"{language}wiki/data/{language}wiki_namespace_0_*.parquet"
            files = sorted(dataset_dir.glob(pattern))

            if not files:
                raise FileNotFoundError(f"No se encontraron Parquet para {language}")

            files_by_language[language] = files

        yield from _interleaved_files(files_by_language, languages)

        return

    local_files_by_language = {language: {} for language in languages}
    for path in sorted(input_root.rglob("*.parquet")):
        language = _language_for_path(path, languages)
        if language in local_files_by_language:
            local_files_by_language[language].setdefault(path.name, path)

    files_by_language = {}
    existing_names_by_language = {}
    for language in languages:
        local_files = local_files_by_language[language]
        files_by_language[language] = sorted(local_files.values())[:count]
        existing_names_by_language[language] = set(local_files)

    next_remote_index = {language: 0 for language in languages}
    for index in range(count):
        for language in languages:
            if stop is not None and stop.is_set():
                return

            files = files_by_language[language]
            if index < len(files):
                yield files[index]
                continue

            remote_index = next_remote_index[language]
            file_name = f"{language}wiki_namespace_0_{remote_index:05}.parquet"
            while file_name in existing_names_by_language[language]:
                remote_index += 1
                file_name = f"{language}wiki_namespace_0_{remote_index:05}.parquet"

            remote_path = f"{language}wiki/data/{file_name}"
            downloaded_file = _download_from_kaggle(remote_path)
            files.append(downloaded_file)
            existing_names_by_language[language].add(file_name)
            next_remote_index[language] = remote_index + 1
            yield downloaded_file

def download_dataset():
    with closing(parquet_files()) as paths:
        for _ in paths:
           pass

if __name__ == "__main__":
    SystemExit(download_dataset())
