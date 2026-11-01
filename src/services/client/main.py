import re
import os
import csv
import uuid
import socket
import signal
import base64
import hashlib
import logging
import threading
import mimetypes

from pathlib import Path
from contextlib import closing
from common import message_protocol
from urllib.parse import unquote, urlsplit
from services.client.dataset import read_records

SERVER_PORT = int(os.environ["SERVER_PORT"])
SERVER_HOST = os.environ["SERVER_HOST"]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
MAX_ROWS = int(os.environ.get("MAX_ROWS", 0))
INPUT_PARQUET = os.environ.get("INPUT_PARQUET")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "output/result.csv")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(Path(OUTPUT_FILE).parent)))
CLIENT_ID = os.environ.get("CLIENT_ID") or f"client_{uuid.uuid4().hex}"

if not re.fullmatch(r"[A-Za-z0-9_-]+", CLIENT_ID):
    raise ValueError("CLIENT_ID must contain only letters, digits, underscores and hyphens")

def save_image(
    image_dir: Path,
    article_url: str,
    image_url: str,
    content_type: str,
    encoded_image: str,
) -> str:
    """Guarda la imagen recibida y devuelve su ruta relativa al directorio de salida."""

    image_bytes = base64.b64decode(encoded_image, validate=True)
    image_name = unquote(urlsplit(article_url).path.rsplit("/", 1)[-1])
    image_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", image_name)

    if not image_name or image_name in {".", ".."}:
        image_name = "image"

    image_path = Path(image_name)
    suffix = Path(unquote(urlsplit(image_url).path)).suffix

    if not suffix and content_type:
        suffix = mimetypes.guess_extension(content_type, strict=False) or ""

        if content_type.lower() == "image/svg+xml":
            suffix = ".svg"

        elif content_type.lower() == "image/jpeg":
            suffix = ".jpg"

    if not suffix:
        suffix = ".bin"

    unique_name = (
        f"{image_path.stem}-{hashlib.sha256(article_url.encode('utf-8')).hexdigest()[:12]}{suffix}"
    )
    image_dir.mkdir(parents=True, exist_ok=True)
    (image_dir / unique_name).write_bytes(image_bytes)
    return (Path("images") / unique_name).as_posix()

def read_batches(input_file: str | None, batches: int, stop: int = None, max_rows: int = None):
    """Prepara un único batch de artículos completos para ambas ramas."""

    if not message_protocol.MIN_BATCH_RECORDS <= batches <= message_protocol.MAX_BATCH_RECORDS:
        raise ValueError(
            f"BATCH_SIZE must be between {message_protocol.MIN_BATCH_RECORDS} "
            f"and {message_protocol.MAX_BATCH_RECORDS}"
        )

    batch_size = 0
    batch = []
    with closing(read_records(input_file, stop, batch_size=batches, max_rows=max_rows)) as records:
        for record in records:
            record_bytes = message_protocol.encode_record(record)
            record_bytes_size = message_protocol.MAX_BATCH_PREFIX_LENGTH + len(record_bytes)

            if batch and (
                len(batch) >= batches or batch_size + record_bytes_size > message_protocol.MAX_PAYLOAD_SIZE
            ):
                yield batch

                batch = []
                batch_size = 0

            batch.append(record_bytes)
            batch_size += record_bytes_size

    if batch:
        yield batch

class Client:

    def __init__(self):
        """Inicializa el estado compartido por los threads de envío y recepción."""

        self.server_socket = None
        self.stop = threading.Event()
        self.sent_all = False
        self.received_eof = False
        self.terminated = False
        self.previous_sigterm = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, _signum, _frame):
        """Detiene ambos threads cerrando la conexión ante SIGTERM."""
        self.terminated = True
        self.stop.set()
        self.disconnect()

    def connect(self):
        """Abre la conexión TCP que comparten ambos threads."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((SERVER_HOST, SERVER_PORT))

    def fail(self, exc):
        """Informa el error y desbloquea al otro thread cerrando el socket."""

        if not self.stop.is_set():
            logging.error("Client error: %s", exc)

        self.stop.set()
        self.disconnect()

    def send_batches(self, input_file: str, batch_size: int, max_rows: int = None):
        """Envía artículos completos una sola vez y cierra la subida con un EOF."""

        try:
            with closing(read_batches(input_file, batch_size, self.stop, max_rows)) as batches:
                for batch in batches:
                    if self.stop.is_set():
                        return

                    message_protocol.external.send_message(
                        client_socket=self.server_socket,
                        message=message_protocol.batching.batch_messages(batch),
                        opcode=message_protocol.OPCODE_DATA,
                        batches=len(batch),
                    )

            if not self.stop.is_set():
                message_protocol.external.send_message(
                    client_socket=self.server_socket, 
                    message=b"", 
                    opcode=message_protocol.OPCODE_EOF, 
                    batches=0
                )
                self.sent_all = True
        except Exception as exc:
            self.fail(exc)

    def receive_results(self, output_dir):
        """Lee DATA con filas 'consulta,resultado' hasta el EOF global del Gateway."""

        csv.field_size_limit(message_protocol.MAX_PAYLOAD_SIZE)
        files = {}

        try:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            for job_id in range(1, 6):
                files[job_id] = (output_dir / f"query_{job_id}.csv.partial").open(
                    "w", encoding="utf-8", newline=""
                )
            image_results = csv.writer(files[2], lineterminator="\n")

            while not self.stop.is_set():
                opcode, batches, payload = message_protocol.external.recv_message(self.server_socket)

                if opcode == message_protocol.OPCODE_EOF:
                    if batches != 0 or payload:
                        raise ValueError("Invalid EOF from Gateway")

                    self.received_eof = True
                    return

                if opcode != message_protocol.OPCODE_DATA:
                    raise ValueError(f"Unexpected opcode from Gateway: {opcode}")

                for message in message_protocol.batching.unbatch_messages(payload, batches):
                    message = message.decode(encoding=message_protocol.ENCODING)
                    job, separator, row = message.partition(",")

                    if not separator or job not in {"1", "2", "3", "4", "5"}:
                        raise ValueError(
                            f"Invalid query identifier in result: {message[:80]}"
                        )

                    if job == "2":
                        fields = next(csv.reader([message]))

                        if len(fields) != 6:
                            raise ValueError("Invalid image result format")

                        _, name, url, image_url, content_type, encoded_image = fields
                        image_path = (
                            save_image(
                                output_dir / "images",
                                url,
                                image_url,
                                content_type,
                                encoded_image,
                            )
                            if encoded_image else ""
                        )
                        image_results.writerow([name, url, image_path])
                    else:
                        files[int(job)].write(row.rstrip("\r\n") + "\n")

        except Exception as exc:
            if not self.stop.is_set():
                self.fail(exc)

        finally:
            for file in files.values():
                file.close()

    def disconnect(self):
        """Desbloquea operaciones y cierra el socket permitiendo llamadas repetidas."""

        server_socket = self.server_socket
        self.server_socket = None

        if server_socket is not None:
            try:
                server_socket.shutdown(socket.SHUT_RDWR)

            except OSError:
                pass

            finally:
                server_socket.close()

    def run(self, input_file, output_dir, batch_size, max_rows=None):
        """Inicia ambos threads y confirma los archivos cuando finalizan correctamente."""

        threads = []

        try:
            output_dir = Path(output_dir) / CLIENT_ID / f"run_{uuid.uuid4().hex}"
            output_dir.mkdir(parents=True, exist_ok=False)
            logging.info("Client %s output: %s", CLIENT_ID, output_dir)
            self.connect()
            threads = [
                threading.Thread(
                    target=self.send_batches, args=(input_file, batch_size, max_rows)
                ),
                threading.Thread(
                    target=self.receive_results, args=(output_dir,)
                ),
            ]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            if self.sent_all and self.received_eof and not self.stop.is_set():
                for job_id in range(1, 6):
                    path = Path(output_dir) / f"query_{job_id}.csv.partial"
                    path.rename(path.with_suffix(""))

                return 0

            return 1

        finally:
            self.stop.set()
            self.disconnect()

            for thread in threads:
                if thread.ident is not None:
                    thread.join()

            signal.signal(signal.SIGTERM, self.previous_sigterm)

def main():
    """Lee y envía Parquet mientras recibe resultados sobre la misma conexión."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )

    try:
        return Client().run(INPUT_PARQUET, OUTPUT_DIR, BATCH_SIZE, MAX_ROWS)
    except Exception as exc:
        logging.error("Failed to run client: %s", exc)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
