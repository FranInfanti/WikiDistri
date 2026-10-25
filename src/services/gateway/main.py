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
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
FILTER_OUTPUT_QUEUE = os.environ["FILTER_OUTPUT_QUEUE"]
WORD_READER_OUTPUT_QUEUE = os.environ["WORD_READER_OUTPUT_QUEUE"]

def job_format(job_id: int, messages: list):
    batch = []
    for message in messages:
        if job_id == 1:
            # [..., [ [url_0, date_modified_0], ..., [url_n, date_modified_n] ]]
            url, date_modified = message
            batch.append(
                f"{job_id},{url},{date_modified}"
            )
        elif job_id == 2:
            # [..., [ [name_0, url_0, image_0], ..., [name_n, url_n, image_n] ]]
            name, url, image_bytes = message
            batch.append(
                f"{job_id},{name},{url},{image_bytes}"
            )
        elif job_id == 3:
            # [..., [ word_0, ..., word_n ]]
            word = message
            batch.append(
                f"{job_id},{word}"
            )
        elif job_id == 4:
            # [..., [ word_0, ..., word_n ]]
            word = message
            batch.append(
                f"{job_id},{word}"
            )
        elif job_id == 5:
            # [..., [ [url_0, amount_0], ..., [url_n, amount_n] ]]
            url, amount = message
            batch.append(
                f"{job_id},{url},{amount}"
            )
        else:
            raise ValueError(f"Unknown job id: {job_id}")
    
    return message_protocol.batching.batch_messages(batch)

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
            active_threads = list(self.active_threads.values())

        for thread in active_threads:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        logging.warning("Graceful period expired, closing client connections")

        with self.clients_requests_lock:
            for client in self.clients_requests.values():
                client.close()

    def _publish_eof(self, req_id: int, articles_sent: int):
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

    def _publish_data(self, req_id: int, batches: int, message: bytes):
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
        logging.info("Start uploading")
        try:
            while self.is_running:
                opcode, batches, payload = ( 
                    message_protocol.external.recv_message(client_req.client_socket) # One thread per client
                )
                if opcode == message_protocol.OPCODE_EOF:
                    logging.info("End of upload")
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
            with self.active_threads_lock:
                client_req.close()
                self.clients_requests.pop(req_id, None)
        finally:
            with self.active_threads_lock:
                del self.active_threads[f"{_HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"]

    def _download(self, job_id, message, ack, nack):
        logging.info("Received a message from the system")
        try:
            opcode, req_id, batches, messages = (
                message_protocol.internal.deserialize(message)
            )

            with self.clients_requests_lock:
                client_req = self.clients_requests.get(req_id, None)

            if not client_req:
                if self.is_running:
                    logging.info(f"An error ocurred with client: {req_id}")
                ack() # client no longer exists, but we still ack the message
                return

            if opcode == message_protocol.OPCODE_DATA:
                message_bytes = job_format(job_id, messages)
                client_req.send(
                    opcode=message_protocol.OPCODE_DATA,
                    batches=batches,
                    message=message_bytes,
                )
            elif opcode == message_protocol.OPCODE_EOF:
                finished = client_req.record_job_finished(job_id)
                if finished:
                    logging.info("All jobs finished")
                    with self.clients_requests_lock:
                        self.clients_requests.pop(req_id, None)

                    client_req.send(
                        opcode=message_protocol.OPCODE_EOF,
                        batches=0,
                        message=b""
                    )
                    client_req.close()
            else:
                logging.info("Unknown opcode")
            ack()
        except client.ClientSendError:
            if self.is_running:
                logging.exception(f"An error occurred with client: {req_id}")
            ack()
        except Exception:
            if self.is_running:
                logging.exception("Unexpected error")
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