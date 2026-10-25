import csv
import io
import logging
import os
import signal
import socket

def read_records(input_file):
    """Lee el CSV incrementalmente y conserva todas las celdas de cada registro."""
    with open(input_file, encoding="utf-8", newline="") as csvfile:
        for row in csv.reader(csvfile, strict=True):
            record = io.StringIO(newline="")
            csv.writer(record).writerow(row)
            yield record.getvalue()


def read_batches(input_file, batch_size):
    """Agrupa registros respetando la cantidad y el límite de bytes del protocolo."""
    from common import message_protocol

    if batch_size <= 0:
        raise ValueError("BATCH_SIZE debe ser mayor que cero")

    batch = []
    size = 0
    for record in read_records(input_file):
        record_size = (
            len(record.encode(message_protocol.ENCODING))
            + message_protocol.batching._LENGTH_SIZE
        )
        if record_size > message_protocol.external._MAX_PAYLOAD_SIZE:
            raise ValueError("Un registro CSV supera el tamaño admitido por el protocolo")
        if batch and (
            len(batch) >= batch_size
            or size + record_size > message_protocol.external._MAX_PAYLOAD_SIZE
        ):
            yield batch
            batch = []
            size = 0
        batch.append(record)
        size += record_size
    if batch:
        yield batch


class Client:
    def __init__(self):
        """Inicializa el estado antes de registrar SIGTERM."""
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = None
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
        """Establece la conexión TCP con el Gateway."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def send_batches(self, input_file, batch_size):
        """Envía batches CSV y luego EOF, incluso cuando el archivo está vacío."""
        from common import message_protocol

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
        """Pendiente: recibir las cinco consultas, guardar por ejecución sin
        sobrescribir, conservar bytes/formato de imágenes y marcar parciales
        hasta confirmar cada finalización, también para consultas vacías.
        """
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
    """Carga configuración, conecta, envía el CSV y libera los recursos."""
    logging.basicConfig(level=logging.INFO)
    client = None
    try:
        input_file = os.environ["INPUT_FILE"]
        server_host = os.environ["SERVER_HOST"]
        server_port = int(os.environ["SERVER_PORT"])
        batch_size = int(os.environ.get("BATCH_SIZE", "100"))
        if batch_size <= 0:
            raise ValueError("BATCH_SIZE debe ser mayor que cero")
        client = Client()
        client.connect(server_host, server_port)
        client.send_batches(input_file, batch_size)
        return 0
    except OSError as exc:
        logging.error("Error de archivo o conexión: %s", exc)
        return 1
    except Exception as exc:
        logging.error("%s", exc)
        return 2
    finally:
        if client is not None:
            try:
                client.disconnect()
            finally:
                signal.signal(signal.SIGTERM, client._prev_sigterm_handler)


if __name__ == "__main__":
    raise SystemExit(main())
