import os
import time
import signal
import socket
import logging
import threading
import itertools

from common import entity, middleware, message_protocol

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 5000))

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
FILTER_OUTPUT_QUEUE = os.environ.get("FILTER_OUTPUT_QUEUE", "filter_output_queue")
WORD_READER_OUTPUT_QUEUE = os.environ.get("WORD_READER_OUTPUT_QUEUE", "word_reader_output_queue")
PREFIX_INPUT_QUEUES = os.environ.get("PREFIX_INPUT_QUEUES", "job_input_queue")
JOBS = int(os.environ.get("JOBS", 5))

STOP_TIMEOUT = float(os.environ.get("STOP_TIMEOUT", 10.0))

class Gateway:

    def __init__(self, host: str, port: int):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.filter_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=FILTER_OUTPUT_QUEUE
        )

        self.word_reader_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=WORD_READER_OUTPUT_QUEUE
        )

        self.input_queues = {
            job_id: middleware.MessageMiddlewareQueueRabbitMQ(
                host=MOM_HOST,
                queue_name=f"{PREFIX_INPUT_QUEUES}_{job_id}"
            )
            for job_id in range(1, JOBS + 1)
        }

        self.server_host = host
        self.server_port = port
        self.server_socket = None
        self.is_running = True

        self.req_counter = itertools.count(1)

        self.publish_lock = threading.Lock()

        self.external_conns = {}
        self.external_conns_lock = threading.Lock()

        self.inner_conns = {}
        self.inner_conns_lock = threading.Lock()

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, setting shutdown flag and closing listener...")
        self.is_running = False

        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.server_socket.close()
            except Exception:
                pass

    def _wait_for_clients(self):
        deadline = time.time() + STOP_TIMEOUT

        logging.info(f"Waiting up to {STOP_TIMEOUT}s for active clients to finish...")

        with self.external_conns_lock:
            threads = list(self.external_conns.values())

        for thread in threads:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break

            thread.join(timeout=remaining)

        with self.external_conns_lock:
            active_sockets = list(self.external_conns.keys())

        if active_sockets:
            logging.warning(
                f"Grace period expired with {len(active_sockets)} active client(s). "
                "Forcibly closing client connections..."
            )
            for client_socket in active_sockets:
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    client_socket.close()
                except Exception:
                    pass
        else:
            logging.info("All clients finished gracefully before timeout.")

    def _publish_message(self, queue, message):
        with self.publish_lock:
            queue.send(message_protocol.internal.serialize(message))

    def _process_internal_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            opcode = fields[0]
            ack()
        except Exception:
            logging.exception("Error processing internal message")
            nack()

    def _process_external_eof(self, req_id: int):
        logging.info(f"Received EOF from client {req_id}. Propagating EOF...")

        message = [message_protocol.OPCODE_EOF, req_id, ""]

        self._publish_message(self.filter_output_queue, message)
        self._publish_message(self.word_reader_output_queue, message)

    def _process_external_data(self, req_id: int, batches: int, message: bytes):
        logging.info(f"Received DATA batch from client {req_id}. Publishing rows...")
        
        lines = message_protocol.batching.unbatch_messages(message, batches)
        for line in lines:
            wikipedia_article = entity.WikipediaArticle.from_csv(line)

            filter_message = [message_protocol.OPCODE_DATA, req_id] + wikipedia_article.to_list(
                name=True, url=True, image=True, in_language=True, date_modified=True, sections=True
            )
            self._publish_message(self.filter_output_queue, filter_message)

            word_reader_payload = [message_protocol.OPCODE_DATA, req_id] + wikipedia_article.to_list(
                name=True, sections=True
            )
            self._publish_message(self.word_reader_output_queue, word_reader_payload)

    def _handle_client(self, client_socket: socket.socket, req_id: int):
        logging.info(f"Connected client with req_id: {req_id}")
        
        try:
            while self.is_running:
                args = message_protocol.external.recv_message(client_socket)

                opcode = args.pop(0)
                if opcode == message_protocol.OPCODE_EOF:
                    self._process_external_eof(req_id)
                    break
                elif opcode == message_protocol.OPCODE_DATA:
                    self._process_external_data(req_id, *args)
                else:
                    logging.warning(f"Unexpected opcode from client {req_id}: {opcode}")
        except Exception:
            if self.is_running:
                logging.exception(f"Error handling client with req_id: {req_id}")
        finally:
            with self.external_conns_lock:
                try:
                    client_socket.close()
                except Exception:
                    pass

                self.external_conns.pop(client_socket, None)

    def _start_consuming(self):
        for job_id, queue in self.input_queues.items():
            thread = threading.Thread(
                target=queue.start_consuming,
                args=(self._process_internal_message,),
                daemon=True,
                name=f"Consumer-Job-{job_id}"
            )

            with self.inner_conns_lock:
                self.inner_conns[job_id] = thread
            
            thread.start()

    def start(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                self.server_socket = server_socket
                
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()

                self._start_consuming()

                logging.info(f"Gateway listening on {self.server_host}:{self.server_port}...")
                while self.is_running:
                    try:
                        client_socket, _ = server_socket.accept()
                    except OSError:
                        if not self.is_running:
                            break
                        raise

                    req_id = next(self.req_counter)

                    thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, req_id),
                        daemon=True,
                        name=f"ClientWorker-{req_id}"
                    )

                    with self.external_conns_lock:
                        self.external_conns[client_socket] = thread

                    thread.start()
        finally:
            self._wait_for_clients()

            self.filter_output_queue.close()
            self.word_reader_output_queue.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    
    try:
        Gateway(HOST, PORT).start()
    except Exception:
        logging.exception("An unexpected error occurred")
        return 2
    return 0

if __name__ == "__main__":
    main()