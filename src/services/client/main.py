import io
import os
import csv
import base64
import hashlib
import mimetypes
import re
import signal
import socket
import logging
import threading

from pathlib import Path
from urllib.parse import unquote, urlsplit

from common import message_protocol

SERVER_PORT = int(os.environ["SERVER_PORT"])
SERVER_HOST = os.environ["SERVER_HOST"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
INPUT_FILE = os.environ.get("INPUT_FILE", "input/articles.csv")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "output/result.csv")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(Path(OUTPUT_FILE).parent)))


def save_image(image_dir: Path, article_url: str, image_url: str, content_type: str, encoded_image: str) -> str:
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


def read_records(input_file):
    """Lee el CSV incrementalmente y conserva todas las celdas de cada registro."""
    csv.field_size_limit(message_protocol.MAX_PAYLOAD_SIZE - message_protocol.MAX_PREFIX_LENGTH)

    with open(input_file, encoding="utf-8", newline="") as csvfile:
        for row in csv.reader(csvfile, strict=True):
            record = io.StringIO(newline="")
            csv.writer(record).writerow(row)
            yield record.getvalue()

def read_batches(input_file: str, batches: int):
    """Agrupa registros respetando la cantidad y el límite de bytes del protocolo."""
    if batches <= 0:
        raise ValueError("BATCH_SIZE must be greater than zero")

    batch = []
    batch_size = 0

    for record in read_records(input_file):
        record_size = message_protocol.batching.batch_payload_size([record])

        if record_size > message_protocol.MAX_PAYLOAD_SIZE:
            raise ValueError(
                f"Message length exceeds maximum payload size of {message_protocol.MAX_PAYLOAD_SIZE} bytes"
            )

        if batch and (
            len(batch) >= batches or batch_size + record_size > message_protocol.MAX_PAYLOAD_SIZE
        ):
            yield batch
            batch = []
            batch_size = 0

        batch.append(record)
        batch_size += record_size

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

    def send_batches(self, input_file, batch_size):
        """Envía el CSV configurado por batches y un único EOF."""
        try:
            for batch in read_batches(input_file, batch_size):
                if self.stop.is_set():
                    return

                message_protocol.external.send_message(
                    self.server_socket,
                    message_protocol.batching.batch_messages(batch),
                    message_protocol.OPCODE_DATA,
                    len(batch),
                )

            if not self.stop.is_set():
                message_protocol.external.send_message(
                    self.server_socket, b"", message_protocol.OPCODE_EOF, 0
                )
                self.sent_all = True

        except Exception as exc:
            self.fail(exc)

    def receive_results(self, output_dir):
        """Lee DATA con filas 'consulta,resultado' hasta el EOF global del Gateway."""
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
        """Desbloquea operaciones y cierra el socket; permite llamadas repetidas."""
        server_socket = self.server_socket
        self.server_socket = None

        if server_socket is not None:
            try:
                server_socket.shutdown(socket.SHUT_RDWR)

            except OSError:
                pass

            finally:
                server_socket.close()

    def run(self, input_dir, output_dir, batch_size):
        """Inicia ambos threads y espera su fin; confirma archivos sólo al completar."""
        threads = []

        try:
            self.connect()
            threads = [
                threading.Thread(
                    target=self.send_batches, args=(input_dir, batch_size)
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
    """Envía los CSV y recibe resultados en paralelo sobre la misma conexión."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )

    try:
        return Client().run(INPUT_FILE, OUTPUT_DIR, BATCH_SIZE)

    except Exception as exc:
        logging.error("Failed to run client: %s", exc)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
