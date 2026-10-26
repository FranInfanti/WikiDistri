"""Descarga los Parquet de Wikipedia y genera el CSV para el cliente."""

import os
from pathlib import Path

import kagglehub
import pyarrow.parquet as pq


DATASET = "wikimedia-foundation/wikipedia-structured-contents"
LANGUAGES = tuple(
    language.strip()
    for language in os.environ.get("WIKI_LANGUAGES", "en,fr").split(",")
)
FILES_PER_LANGUAGE = os.environ.get("FILES_PER_LANGUAGE", "5")
MAX_ROWS = int(os.environ.get("MAX_ROWS", "0"))
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
OUTPUT_FILE = Path(__file__).resolve().parents[1] / "input" / "articles.csv"


def parquet_files():
    """Entrega los Parquet configurados, o todos los del dataset si se pide `all`."""
    if any(language not in ("en", "fr") for language in LANGUAGES):
        raise ValueError("WIKI_LANGUAGES debe contener `en`, `fr` o `en,fr`")

    if FILES_PER_LANGUAGE == "all":
        dataset_dir = Path(kagglehub.dataset_download(DATASET))

        for language in LANGUAGES:
            pattern = f"{language}wiki/data/{language}wiki_namespace_0_*.parquet"
            files = sorted(dataset_dir.glob(pattern))

            if not files:
                raise FileNotFoundError(f"No se encontraron archivos para {language}")

            yield from files

        return

    count = int(FILES_PER_LANGUAGE)
    if count < 1:
        raise ValueError("FILES_PER_LANGUAGE debe ser mayor que cero o `all`")

    for language in LANGUAGES:
        prefix = f"{language}wiki/data/{language}wiki_namespace_0_"

        for index in range(count):
            yield kagglehub.dataset_download(
                DATASET,
                path=f"{prefix}{index:05}.parquet",
            )


def download_dataset():
    """Lee cada Parquet por lotes y agrega sus columnas útiles al CSV."""
    if MAX_ROWS < 0:
        raise ValueError("MAX_ROWS debe ser mayor o igual a cero")
    if MAX_ROWS and FILES_PER_LANGUAGE == "all":
        raise ValueError("Para limitar filas, elegí una cantidad de archivos")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    written_rows = 0

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as output:
        for parquet_path in parquet_files():
            print(f"Procesando {parquet_path}", flush=True)

            with pq.ParquetFile(parquet_path) as parquet:
                batches = parquet.iter_batches(
                    batch_size=BATCH_SIZE,
                    columns=COLUMNS,
                )

                for batch in batches:
                    articles = batch.to_pandas()
                    if MAX_ROWS:
                        articles = articles.head(MAX_ROWS - written_rows)

                    articles.to_csv(
                        output,
                        header=False,
                        index=False,
                        date_format="%Y-%m-%d %H:%M:%S",
                    )
                    written_rows += len(articles)

                    if MAX_ROWS and written_rows >= MAX_ROWS:
                        return


if __name__ == "__main__":
    download_dataset()
