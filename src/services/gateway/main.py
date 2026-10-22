import os
import time
import signal
import socket
import logging
import functools
import threading
import itertools

from common import entity, middleware, message_protocol

_JOBS = 5
_TIMEOUT = 5.0

HOST = os.environ["HOST"]
PORT = int(os.environ["PORT"])

MOM_HOST = os.environ["MOM_HOST"]
FILTER_OUTPUT_QUEUE = os.environ["FILTER_OUTPUT_QUEUE"]
WORD_READER_OUTPUT_QUEUE = os.environ["WORD_READER_OUTPUT_QUEUE"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]

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
                queue_name=f"{GATEWAY_PREFIX}_{job_id}"
            )
            for job_id in range(1, _JOBS + 1)
        }

        self.server_host = host
        self.server_port = port
        self.server_socket = None
        self.is_running = True

        self.req_counter = itertools.count(1)
        self.publish_lock = threading.Lock()

        self.threads = []

        self.jobs_by_client = {}
        self.jobs_by_client_lock = threading.Lock()

        self.client_by_req = {}
        self.client_by_req_lock = threading.Lock()

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

    def _wait_for_threads(self):
        deadline = time.time() + _TIMEOUT

        logging.info(f"Waiting up to {_TIMEOUT}s for active threads to finish...")

        for input_queue in self.input_queues.values():
            input_queue.stop_consuming_threadsafe()

        for thread in self.threads:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break

            thread.join(timeout=remaining)

        logging.warning("Closing client connections...")

        with self.client_by_req_lock:
            for client_socket in self.client_by_req.values():
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    client_socket.close()
                except Exception:
                    pass

    def _publish_message(self, queue, message: list):
        """
        Publishes a message to a queue accessed by multiple threads.

        queue
            The queue to which the message is being published.

        message
            The message being published.
        """
        with self.publish_lock:
            queue.send(
                message_protocol.internal.serialize(message)
            )

    def _send_response(self, job_id, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            opcode = fields.pop(0)
            req_id = fields.pop(0)

            if opcode == message_protocol.OPCODE_DATA:
                with self.client_by_req_lock:
                    client_socket = self.client_by_req[req_id]

                    message_protocol.send_message(
                        client_socket=client_socket,
                        message="", # TODO: What do we send?
                        opcode=message_protocol.OPCODE_DATA
                    )
            elif opcode == message_protocol.OPCODE_EOF:
                with self.jobs_by_client_lock:
                    self.jobs_by_client[req_id][job_id] = True

                    finish = True
                    for job in self.jobs_by_client[req_id].values():
                        finish = finish and job

                if finish:
                    logging.info(f"All jobs done for the client with req_id: {req_id}. Goodbye...")
                    del self.jobs_by_client[req_id]
                    del self.client_by_req[req_id]

            ack()
        except Exception:
            nack()

    def _publish_eof(self, req_id: int):
        logging.info(f"Received EOF from client {req_id}. Propagating EOF...")

        message = [message_protocol.OPCODE_EOF, req_id, ""]

        self._publish_message(self.filter_output_queue, message)
        self._publish_message(self.word_reader_output_queue, message)

    def _publish_data(self, req_id: int, batches: int, message: bytes):
        logging.info(f"Received DATA batch from client {req_id}. Publishing rows...")
        
        lines = message_protocol.batching.unbatch_messages(message, batches)
        for line in lines:
            wikipedia_article = entity.WikipediaArticle.from_csv(line)

            filter_message = [message_protocol.OPCODE_DATA, req_id] + wikipedia_article.to_list(
                name=True, url=True, image=True, in_language=True, date_modified=True, sections=True
            )
            self._publish_message(self.filter_output_queue, filter_message)

            word_reader_message = [message_protocol.OPCODE_DATA, req_id] + wikipedia_article.to_list(
                name=True, sections=True
            )
            self._publish_message(self.word_reader_output_queue, word_reader_message)

    def _upload_data(self, client_socket: socket.socket, req_id: int):
        logging.info(f"Upload data for client with req_id: {req_id}")
        
        try:
            while self.is_running:
                opcode, *args = message_protocol.external.recv_message(client_socket)

                if opcode == message_protocol.OPCODE_EOF:
                    self._publish_eof(req_id)
                    return
                elif opcode == message_protocol.OPCODE_DATA:
                    self._publish_data(req_id, *args)  # type: ignore
                else:
                    logging.warning(f"Unexpected opcode from client {req_id}: {opcode}")
        except Exception:
            if self.is_running:
                logging.exception(f"Error while uploading data for client with req_id: {req_id}")

        logging.info(f"Data for client with req_id: {req_id} has been uploaded")

    def _handle_client(self, client_socket: socket.socket):
        req_id = next(self.req_counter)
        logging.info(f"Connected client with req_id: {req_id}")

        with self.jobs_by_client_lock:
            self.jobs_by_client[req_id] = {
                job_id: False
                for job_id in range(1, _JOBS + 1)
            }

        thread = threading.Thread(
            target=self._upload_data,
            args=(client_socket, req_id),
            name=f"ClientWorker-{req_id}"
        )

        with self.client_by_req_lock:
            self.client_by_req[req_id] = client_socket

        self.threads.append(thread)
        thread.start()

    def start(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                self.server_socket = server_socket
                
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()

                for job_id, queue in self.input_queues.items():
                    callback = functools.partial(self._send_response, job_id=job_id)
                    thread = threading.Thread(
                        target=queue.start_consuming,
                        args=(callback,),
                        name=f"Consumer-Job-{job_id}"
                    )

                    self.threads.append(thread)
                    thread.start()

                logging.info(f"Gateway listening on {self.server_host}:{self.server_port}...")
                while self.is_running:
                    try:
                        client_socket, _ = server_socket.accept()
                    except OSError:
                        if not self.is_running:
                            break
                        raise
                    self._handle_client(client_socket)
        finally:
            self._wait_for_threads()

            # close the queues...
            self.filter_output_queue.close()
            self.word_reader_output_queue.close()

            for input_queue in self.input_queues.values():
                input_queue.close()

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