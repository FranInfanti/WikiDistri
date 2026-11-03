"""Descarga los Parquet de Wikipedia a input y permite exportarlos a CSV."""

import os
from contextlib import closing
from pathlib import Path

import kagglehub
import pyarrow.parquet as pq

DATASET = "wikimedia-foundation/wikipedia-structured-contents"
LANGUAGES = ("en", "fr")
FILES_PER_LANGUAGE = 5
MAX_ROWS = 0
BATCH_SIZE = 1000

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
OUTPUT_FILE = INPUT_ROOT / "articles.csv"


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


def parquet_files(input_file=None, stop=None):
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

    input_dir = os.environ.get("INPUT_DIR")

    if input_dir:
        files_by_language = {language: [] for language in languages}
        other_files = []

        for path in sorted(Path(input_dir).rglob("*.parquet")):
            language = _language_for_path(path, LANGUAGES)
            if language in files_by_language:
                files_by_language[language].append(path)
            elif language is None:
                other_files.append(path)

        if not any(files_by_language.values()) and not other_files:
            raise FileNotFoundError(f"No se encontraron Parquet en {input_dir}")

        for language in languages:
            if count is not None:
                files_by_language[language] = files_by_language[language][:count]
        yield from _interleaved_files(files_by_language, languages)
        yield from (other_files if count is None else other_files[:count])
        return

    input_root = Path(os.environ.get("DOWNLOAD_DIR", str(INPUT_ROOT)))
    input_root.mkdir(parents=True, exist_ok=True)
    # Se conserva KaggleHub y su organización de archivos, pero dentro de input.
    # No se utiliza el volumen separado kaggle-cache ni otro directorio de datos.
    os.environ["KAGGLEHUB_CACHE"] = str(input_root)

    if count is None:
        dataset_dir = Path(kagglehub.dataset_download(DATASET))
        files_by_language = {}

        for language in languages:
            pattern = f"{language}wiki/data/{language}wiki_namespace_0_*.parquet"
            files = sorted(dataset_dir.glob(pattern))

            if not files:
                raise FileNotFoundError(f"No se encontraron Parquet para {language}")

            files_by_language[language] = files

        yield from _interleaved_files(files_by_language, languages)

        return

    local_files = {}
    other_files = []
    for path in sorted(input_root.rglob("*.parquet")):
        if _language_for_path(path, LANGUAGES) in languages:
            local_files[path.name] = path
        elif _language_for_path(path, LANGUAGES) is None:
            other_files.append(path)

    if not local_files and other_files:
        yield from (other_files if count is None else other_files[:count])
        return

    for index in range(count):
        for language in languages:
            if stop is not None and stop.is_set():
                return
            prefix = f"{language}wiki/data/{language}wiki_namespace_0_"
            file_name = f"{language}wiki_namespace_0_{index:05}.parquet"
            local_path = local_files.get(file_name)
            if local_path is not None:
                yield local_path
            else:
                remote_path = f"{prefix}{index:05}.parquet"
                yield Path(kagglehub.dataset_download(DATASET, path=remote_path))

    yield from (other_files if count is None else other_files[:count])


def download_dataset(max_rows=None, batch_size=None, output_file=None):
    """Lee los Parquet por lotes y exporta sus columnas originales al CSV de inspección."""

    if max_rows is None:
        max_rows = int(os.environ.get("MAX_ROWS", MAX_ROWS))

    if batch_size is None:
        batch_size = int(os.environ.get("BATCH_SIZE", BATCH_SIZE))

    if max_rows < 0 or batch_size < 1:
        raise ValueError("MAX_ROWS debe ser >= 0 y BATCH_SIZE debe ser > 0")

    output_file = Path(output_file) if output_file else OUTPUT_FILE
    output_file.parent.mkdir(parents=True, exist_ok=True)
    written_rows = 0

    with output_file.open("w", encoding="utf-8", newline="") as output:
        with closing(parquet_files()) as paths:
            for parquet_path in paths:
                print(f"Procesando {parquet_path}", flush=True)

                with pq.ParquetFile(parquet_path) as parquet:
                    for batch in parquet.iter_batches(batch_size=batch_size, columns=COLUMNS):
                        articles = batch.to_pandas()

                        if max_rows:
                            articles = articles.head(max_rows - written_rows)

                        articles.to_csv(
                            output,
                            header=False,
                            index=False,
                            date_format="%Y-%m-%d %H:%M:%S",
                        )
                        written_rows += len(articles)

                        if max_rows and written_rows >= max_rows:
                            return


if __name__ == "__main__":
    download_dataset()
