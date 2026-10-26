import io
import os
import csv
import time
import signal
import socket
import logging

from common import message_protocol

CONNECTION_ATTEMPTS = 5
CONNECTION_RETRY_DELAY = 1

SERVER_PORT = int(os.environ["SERVER_PORT"])
SERVER_HOST = os.environ["SERVER_HOST"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
INPUT_FILE = os.environ.get("INPUT_FILE", "input/articles.csv")

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
        raise ValueError("BATCH_SIZE debe ser mayor que cero")

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
        """Inicializa el estado antes de registrar SIGTERM."""
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        """Interrumpe el trabajo y cierra la conexión ante SIGTERM."""
        logging.info("Received SIGTERM signal")
        self.closed = True
        self.disconnect()

        if callable(self._prev_sigterm_handler):
            self._prev_sigterm_handler(signum, frame)
        raise SystemExit(0)

    def connect(self, server_host, server_port):
        """Establece la conexión TCP con el Gateway, reintentando si falla."""
        for attempt in range(1, CONNECTION_ATTEMPTS + 1):
            server_socket = None
            try:
                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.connect((server_host, server_port))
                self.server_socket = server_socket
                return
            except OSError as exc:
                if server_socket is not None:
                    server_socket.close()

                if attempt == CONNECTION_ATTEMPTS:
                    raise

                logging.warning(
                    "Could not connect to the gateway (attempts %s/%s): %s. "
                    "Retrying in %s seconds(s)",
                    attempt,
                    CONNECTION_ATTEMPTS,
                    exc,
                    CONNECTION_RETRY_DELAY,
                )
                time.sleep(CONNECTION_RETRY_DELAY)

    def send_batches(self, input_file, batch_size):
        """Envía batches CSV y luego EOF, incluso cuando el archivo está vacío."""
        logging.info("Sending article batches")

        for batch in read_batches(input_file, batch_size):
            payload = message_protocol.batching.batch_messages(batch)
            message_protocol.external.send_message(
                self.server_socket, payload, message_protocol.OPCODE_DATA, len(batch)
            )

        message_protocol.external.send_message(
            self.server_socket, b"", message_protocol.OPCODE_EOF, 0
        )
        logging.info("Input sent; query results are not implemented yet")

    def receive_results(self, output_dir):
        """Pendiente: recibir y guardar los resultados de las cinco consultas."""

        message_protocol.external.send_message(
            client_socket=self.server_socket, 
            message=b"", 
            opcode=message_protocol.EOF, 
            batches=0
        )
        raise NotImplementedError("El Gateway todavía no devuelve resultados")

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

def main():
    """Lee el CSV configurado y lo envía al gateway."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )

    client = None
    try:
        if BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE debe ser mayor que cero")
        client = Client()
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_batches(INPUT_FILE, BATCH_SIZE)
        return 0
    except OSError as exc:
        logging.error("Error de archivo o conexión: %s", exc)
        return 1
    except Exception as exc:
        logging.error("%s", exc)
        return 2
    finally:
        if client:
            try:
                client.disconnect()
            finally:
                signal.signal(signal.SIGTERM, client._prev_sigterm_handler)

if __name__ == "__main__":
    raise SystemExit(main())
