import os
import time
import signal
import socket
import client
import logging
import functools
import threading
import itertools

from common import entity, middleware, message_protocol

_JOBS = 5
_TIMEOUT = 5.0
_HANDLE_CLIENT_REQUEST_PREFIX = "Uploader"
_HANDLE_CLIENT_RESPONSE_PREFIX = "Consumer"

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

        self.is_running = True
        self.server_socket = None
        self.server_host = host
        self.server_port = port
        self.req_counter = itertools.count(1)

        self.filter_output_queue_lock = threading.Lock()
        self.word_reader_output_queue_lock = threading.Lock()

        self.active_threads = {}
        self.active_threads_lock = threading.Lock()

        self.clients_requests = {}
        self.clients_requests_lock = threading.Lock()

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag and closing listener")
        self.is_running = False

        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass

    def _wait_for_threads(self):
        deadline = time.time() + _TIMEOUT
        logging.info(f"Waiting up to {_TIMEOUT}s for active threads to finish")

        for input_queue in self.input_queues.values():
            input_queue.stop_consuming_threadsafe()

        with self.active_threads_lock:
            active_threads = self.active_threads

        for thread in active_threads.values():
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        logging.warning("Graceful period expired, closing client connections")

        with self.clients_requests_lock:
            for client in self.clients_requests.values():
                client.close()

    def _publish_eof(self, req_id: int, articles_sent: int):
        logging.info("Upload EOF message")

        with self.filter_output_queue_lock:
            self.filter_output_queue.send(
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_EOF,
                    req_id=req_id,
                    message=articles_sent
                )
            )

        with self.word_reader_output_queue_lock:
            self.word_reader_output_queue.send(
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_EOF,
                    req_id=req_id,
                    message=articles_sent
                )
            )

        with self.active_threads_lock:
            self.active_threads.pop(f"{_HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}", None)

    def _publish_data(self, req_id: int, batches: int, message: bytes):
        logging.info("Upload DATA message")

        for line in message_protocol.batching.unbatch_messages(message, batches):
            wikipedia_article = entity.WikipediaArticle.from_csv(line)

            filter_list = wikipedia_article.to_list(
                name=True, url=True, image=True, in_language=True, date_modified=True, sections=True
            )
            with self.filter_output_queue_lock:
                self.filter_output_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=1,
                        message=[filter_list]
                    )
                )

            word_reader_list = wikipedia_article.to_list(
                abstract=True, url=True, date_modified=True, in_language=True,
            )
            with self.word_reader_output_queue_lock:
                self.word_reader_output_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=1,
                        message=[word_reader_list]
                    )
                )

    def _upload(self, req_id: int, client_req: client.Client):
        try:
            while self.is_running:
                opcode, batches, payload = message_protocol.external.recv_message(client_req.client_socket)

                if opcode == message_protocol.OPCODE_EOF:
                    self._publish_eof(req_id, client_req.articles_sent)
                    return
                elif opcode == message_protocol.OPCODE_DATA:
                    self._publish_data(req_id, batches, payload)
                    client_req.record_articles_sent(batches)
                else:
                    logging.warning("Unexpected opcode")
        except Exception:
            if self.is_running:
                logging.exception("Error while uploading data")

            self._remove_client(req_id)

    def _download(self, job_id, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            opcode, req_id, *_data = fields

            with self.clients_requests_lock:
                client = self.clients_requests[req_id]

                if opcode == message_protocol.OPCODE_DATA:
                    # TODO: Send the data to the client
                    pass
                elif opcode == message_protocol.OPCODE_EOF:
                    if client.finish(job_id):
                        logging.info("All jobs finished")
                        # TODO: Send general EOF to the client
                        
                        client.close()
                        del self.clients_requests[req_id]
            ack()
        except:
            nack()

    def _handle_client_request(self, client_socket):
        req_id = next(self.req_counter)
        logging.info(f"A new client has connected, assign req_id={req_id}")

        client_req = client.Client(client_socket, _JOBS)
        thread = threading.Thread(
            target=self._upload,
            args=(req_id, client_req),
            name=f"{_HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"
        )

        with self.clients_requests_lock:
            self.clients_requests[req_id] = client_req

        with self.active_threads_lock:
            self.active_threads[f"{_HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"] = thread

        thread.start()

    def _handle_client_response(self):
        for job_id, queue in self.input_queues.items():
            callback = functools.partial(self._download, job_id=job_id)
            thread = threading.Thread(
                target=queue.start_consuming,
                args=(callback,),
                name=f"{_HANDLE_CLIENT_RESPONSE_PREFIX}-{job_id}"
            )

            with self.active_threads_lock:
                self.active_threads[f"{_HANDLE_CLIENT_RESPONSE_PREFIX}-{job_id}"] = thread

            thread.start()

    def start(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                self.server_socket = server_socket

                logging.info("Listening to connections")
                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()

                self._handle_client_response()

                while self.is_running:
                    try:
                        client_socket, _ = server_socket.accept()
                    except OSError:
                        if not self.is_running:
                            break
                        raise

                    self._handle_client_request(client_socket)
        finally:
            self._wait_for_threads()

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