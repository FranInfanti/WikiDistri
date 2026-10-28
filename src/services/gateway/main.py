import os
import time
import signal
import socket
import client
import logging
import functools
import threading
import itertools

from common import entity, middleware, message_protocol, rabbitmq

HOST = os.environ["HOST"]
PORT = int(os.environ["PORT"])

JOBS = int(os.environ["JOBS"])
MOM_HOST = os.environ["MOM_HOST"]
BATCH_SIZE = int(os.environ["BATCH_SIZE"])
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
FILTER_QUEUE = os.environ["FILTER_QUEUE"]
WORD_READER_QUEUE = os.environ["WORD_READER_QUEUE"]

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
    _TIMEOUT = 5.0
    _HANDLE_CLIENT_REQUEST_PREFIX = "Uploader"
    _HANDLE_CLIENT_RESPONSE_PREFIX = "Consumer"

    def __init__(self, host: str, port: int):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[FILTER_QUEUE, WORD_READER_QUEUE]
        )

        self.input_queues = {
            job_id: middleware.MessageMiddlewareQueueRabbitMQ(
                host=MOM_HOST,
                queue_name=f"{GATEWAY_PREFIX}_{job_id}"
            )
            for job_id in range(1, JOBS + 1)
        }

        self.is_running = True
        self.server_socket = None
        self.server_host = host
        self.server_port = port
        self.req_counter = itertools.count(1)
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
        deadline = time.time() + self._TIMEOUT

        for input_queue in self.input_queues.values():
            input_queue.stop_consuming_threadsafe()

        with self.active_threads_lock:
            active_threads = list(self.active_threads.values())

        logging.info(f"Waiting up to {self._TIMEOUT} seconds for {len(active_threads)} active threads to stop")
        for thread in active_threads:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        remaining_threads = [thread.name for thread in active_threads if thread.is_alive()]
        if remaining_threads:
            logging.warning(f"Shutdown timed out with still {len(remaining_threads)} active threads")
        else:
            logging.info("All active threads stopped; closing client connections")

        with self.clients_requests_lock:
            for client in self.clients_requests.values():
                client.close()

    def _publish_eof(self, req_id: int, articles_sent: int):
        self.publisher.publish_queue(
            queue=FILTER_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
                message=articles_sent
            )
        )

        self.publisher.publish_queue(
            queue=WORD_READER_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
                message=articles_sent
            )
        )

    def _publish_data(self, req_id: int, batches: int, message: bytes):
        batch_filter = []
        serialized_filter_batch = b""

        batch_word_reader = []
        serialized_word_reader_batch = b""

        for line in message_protocol.batching.unbatch_messages(message, batches):
            wikipedia_article = entity.WikipediaArticle.from_csv(line)

            filter_message = wikipedia_article.to_list(
                name=True, url=True, image=True, in_language=True, date_modified=True, sections=True
            )
            reader_message = wikipedia_article.to_list(
                abstract=True, url=True, date_modified=True, in_language=True,
            )

            candidate_filter_batch = message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(batch_filter) + 1,
                message=[*batch_filter, filter_message]
            )
            if batch_filter and (
                len(batch_filter) >= BATCH_SIZE
                or len(candidate_filter_batch) > message_protocol.MAX_RABBITMQ_MESSAGE_SIZE
            ):
                self.publisher.publish_queue(queue=FILTER_QUEUE, message=serialized_filter_batch)

                batch_filter = []
                candidate_filter_batch = message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[filter_message]
                )

            if len(candidate_filter_batch) > message_protocol.MAX_RABBITMQ_MESSAGE_SIZE:
                raise ValueError(
                    f"Serialized filter message for req_id={req_id} exceeds "
                    f"RabbitMQ's {message_protocol.MAX_RABBITMQ_MESSAGE_SIZE}-byte limit"
                )

            batch_filter.append(filter_message)
            serialized_filter_batch = candidate_filter_batch

            candidate_word_reader_batch = message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(batch_word_reader) + 1,
                message=[*batch_word_reader, reader_message]
            )
            if batch_word_reader and (
                len(batch_word_reader) >= BATCH_SIZE
                or len(candidate_word_reader_batch) > message_protocol.MAX_RABBITMQ_MESSAGE_SIZE
            ):
                self.publisher.publish_queue(queue=WORD_READER_QUEUE, message=serialized_word_reader_batch)

                batch_word_reader = []
                candidate_word_reader_batch = message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[reader_message]
                )

            if len(candidate_word_reader_batch) > message_protocol.MAX_RABBITMQ_MESSAGE_SIZE:
                raise ValueError(
                    f"Serialized word-reader message for req_id={req_id} exceeds "
                    f"RabbitMQ's {message_protocol.MAX_RABBITMQ_MESSAGE_SIZE}-byte limit"
                )

            batch_word_reader.append(reader_message)
            serialized_word_reader_batch = candidate_word_reader_batch

        if batch_filter:
            self.publisher.publish_queue(queue=FILTER_QUEUE, message=serialized_filter_batch)

        if batch_word_reader:
            self.publisher.publish_queue(queue=WORD_READER_QUEUE, message=serialized_word_reader_batch)

    def _upload(self, req_id: int, client_req: client.Client):
        logging.info(f"Started upload for req_id={req_id}")
        try:
            while self.is_running:
                opcode, batches, payload = ( 
                    message_protocol.external.recv_message(client_req.client_socket) # One thread per client
                )
                if opcode == message_protocol.OPCODE_EOF:
                    logging.info(
                        f"Upload completed for req_id={req_id}; articles_sent={client_req.articles_sent}"
                    )
                    self._publish_eof(req_id, client_req.articles_sent)
                    return
                elif opcode == message_protocol.OPCODE_DATA:
                    self._publish_data(req_id, batches, payload)
                    client_req.record_articles_sent(batches)
                else:
                    logging.warning(
                        f"Received unexpected opcode={opcode} while uploading req_id={req_id}",
                    )
        except Exception:
            if self.is_running:
                logging.exception("Failed to upload data for req_id=%s", req_id)
            with self.active_threads_lock:
                client_req.close()
                self.clients_requests.pop(req_id, None)
        finally:
            with self.active_threads_lock:
                del self.active_threads[f"{self._HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"]

    def _download(self, job_id, message, ack, nack):
        req_id = None
        try:
            opcode, req_id, batches, messages = (
                message_protocol.internal.deserialize(message)
            )

            with self.clients_requests_lock:
                client_req = self.clients_requests.get(req_id, None)

            if not client_req:
                if self.is_running:
                    logging.debug(
                        f"Discarding response for inactive req_id={req_id} from job_id={job_id}",
                    )
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
                    logging.info(f"All jobs finished for req_id={req_id}")
                    with self.clients_requests_lock:
                        self.clients_requests.pop(req_id, None)

                    client_req.send(
                        opcode=message_protocol.OPCODE_EOF,
                        batches=0,
                        message=b""
                    )
                    client_req.close()
            else:
                logging.warning(
                    f"Received unexpected opcode={opcode} for req_id={req_id} from job_id={job_id}",
                )
            ack()
        except client.ClientSendError:
            if self.is_running:
                logging.exception(
                    f"Failed to send response to req_id={req_id} from job_id={job_id}",
                )
            ack()
        except Exception:
            if self.is_running:
                logging.exception(
                    f"Failed to process response for req_id={req_id} from job_id={job_id}",
                )
            nack()

    def _handle_client_request(self, client_socket: socket.socket):
        req_id = next(self.req_counter)
        logging.info(f"Accepted client connection; assigned req_id={req_id}")

        client_req = client.Client(
            client_socket=client_socket,
            jobs=JOBS
        )
        thread = threading.Thread(
            target=self._upload,
            args=(req_id, client_req),
            name=f"{self._HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"
        )

        with self.clients_requests_lock:
            self.clients_requests[req_id] = client_req

        with self.active_threads_lock:
            self.active_threads[f"{self._HANDLE_CLIENT_REQUEST_PREFIX}-{req_id}"] = thread

        thread.start()

    def _handle_client_response(self):
        for job_id, queue in self.input_queues.items():
            callback = functools.partial(self._download, job_id=job_id)
            thread = threading.Thread(
                target=queue.start_consuming,
                args=(callback,),
                name=f"{self._HANDLE_CLIENT_RESPONSE_PREFIX}-{job_id}"
            )

            with self.active_threads_lock:
                self.active_threads[f"{self._HANDLE_CLIENT_RESPONSE_PREFIX}-{job_id}"] = thread

            thread.start()

    def start(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                self.server_socket = server_socket

                server_socket.bind((self.server_host, self.server_port))
                server_socket.listen()
                logging.info("Listening to connections")

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

            self.publisher.stop()
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