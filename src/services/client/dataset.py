"""Lee desde disco y normaliza los Parquet obtenidos por download_dataset.py."""

import json
import logging
import os
from contextlib import closing
from datetime import datetime

import pyarrow.parquet as pq

from common import message_protocol
from scripts.download_dataset import COLUMNS, parquet_files


def normalize_article(row):
    """Normaliza los valores del dataset antes de preparar los batches completos."""

    date = row["date_modified"]

    if isinstance(date, str):
        date = datetime.fromisoformat(date)

    image = row["image"]
    language = row["in_language"]
    sections = row["sections"]

    if isinstance(sections, str):
        sections = json.loads(sections)

    return [
        row["abstract"],
        row["name"],
        row["url"],
        date.strftime("%Y-%m-%d %H:%M:%S") if date is not None else None,
        image.get("content_url") if image else None,
        language.get("identifier") if language else None,
        json.dumps(sections, ensure_ascii=False) if sections is not None else None,
    ]


def read_records(input_file=None, stop=None, *, batch_size=None, max_rows=None):
    """Lee el Parquet de disco por lotes y conserva el archivo al alcanzar MAX_ROWS."""

    if max_rows is None:
        max_rows = int(os.environ.get("MAX_ROWS", message_protocol.DEFAULT_MAX_ROWS))

    if batch_size is None:
        batch_size = int(os.environ.get("BATCH_SIZE", message_protocol.DEFAULT_BATCH_SIZE))

    read_size = min(int(os.environ.get("PARQUET_BATCH_SIZE", batch_size)), batch_size)

    if max_rows < message_protocol.UNLIMITED_ROWS or read_size < message_protocol.MIN_BATCH_RECORDS:
        raise ValueError("MAX_ROWS must be >= 0 and batch sizes must be > 0")

    count = 0

    with closing(parquet_files(input_file, stop)) as paths:
        for path in paths:
            if stop is not None and stop.is_set():
                return

            logging.info("Reading Parquet %s", path)

            with pq.ParquetFile(path) as parquet:
                for batch in parquet.iter_batches(
                    batch_size=min(read_size, max_rows) if max_rows else read_size,
                    columns=COLUMNS,
                ):
                
                    for index in range(batch.num_rows):
                        if stop is not None and stop.is_set():
                            return

                        row = {name: batch.column(column)[index].as_py()
                               for column, name in enumerate(batch.schema.names)}
                        yield normalize_article(row)
                        count += 1

                        if max_rows and count >= max_rows:
                            return
