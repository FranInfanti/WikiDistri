import os
import time
import signal
import base64
import logging
import requests
import threading

from common import middleware, message_protocol, entity, rabbitmq

MOM_HOST = os.environ["MOM_HOST"]

ID = int(os.environ["ID"])
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
IMAGE_DOWNLOADER_AMOUNT = int(os.environ["IMAGE_DOWNLOADER_AMOUNT"])
IMAGE_DOWNLOADER_PREFIX = os.environ["IMAGE_DOWNLOADER_PREFIX"]
IMAGE_DOWNLOADER_QUEUE = os.environ["IMAGE_DOWNLOADER_QUEUE"]

def define_exchanges(host, prefix, amount, exclude_id = None):
    exchanges = {}

    for i in range(amount):
        if i == exclude_id:
            continue

        exchanges[i] = middleware.MessageMiddlewareExchangeRabbitMQ(
            host, prefix, [f"{prefix}_{i}"]
        )

    return exchanges

class ImageDownloader():
    _TIMEOUT = 5.0

    def __init__(self):
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)
        
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=IMAGE_DOWNLOADER_QUEUE
        )
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=MOM_HOST,
            exchange_name=IMAGE_DOWNLOADER_PREFIX,
            routing_keys=[f"{IMAGE_DOWNLOADER_PREFIX}_{ID}"]
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[f"{GATEWAY_PREFIX}_2"],
            exchanges={
                i: IMAGE_DOWNLOADER_PREFIX
                for i in range(IMAGE_DOWNLOADER_AMOUNT)
                if i != ID
            }
        )
        self.is_running = True
        self.exchange_consumer = None
        self.message_tracker = {}

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("Failed to stop the input queue consumer during shutdown", exc_info=True)

    def _wait_for_threads(self):
        deadline = time.time() + self._TIMEOUT

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            logging.warning("Failed to stop the input exchange consumer during shutdown", exc_info=True)

        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to stop")
        if self.exchange_consumer:
            remaining = max(0.0, deadline - time.time())
            if remaining > 0:
                self.exchange_consumer.join(timeout=remaining)
            if self.exchange_consumer.is_alive():
                logging.warning(f"Shutdown timed out with still active threads")
            self.input_exchange.close()

        if self.publisher.stop(timeout=max(0.0, deadline - time.time())):
            logging.warning(f"Shutdown timed out with still one active thread")
        else:
            logging.info("All active threads stopped")

    def _download_image(self, req_id: int, _batches: int, messages: list):
        for message in messages:
            name, url, image = message
            image_bytes = None
            image_content_type = None

            if image:
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0"
                    }
                    response = requests.get(url=image, headers=headers, timeout=self._TIMEOUT * 2)
                    response.raise_for_status()
                    image_bytes = base64.b64encode(response.content).decode(message_protocol.ENCODING)
                    image_content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
                    logging.info(f"[{req_id}] - The image has been downloaded")
                except requests.HTTPError as e:
                    logging.warning(
                        f"HTTP Error: {e.response.status_code}; reason: {e.response.reason}"
                    )
                except requests.RequestException as e:
                    logging.warning(f"Request failed for {image}: {e}")

            self.publisher.publish_queue(
                queue=f"{GATEWAY_PREFIX}_2",
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[[name, url, image, image_content_type, image_bytes]]
                )
            )

            self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker()).record_recv()
        
    def _process_eof(self, req_id: int, _: int, expected_messages: int):
        logging.info(f"[{req_id}] - Receive the end of the images")
        
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        tracker.record_expected(expected_messages)

        if IMAGE_DOWNLOADER_AMOUNT == 1:
            if tracker.all_recv():
                logging.info(f"[{req_id}] - All of the images have been downloaded !!!")
                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_2",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )
            else:
                logging.error(
                    f"[%s] - Article count mismatch: expected %s, received %s",
                    req_id,
                    tracker.expected_messages,
                    tracker.recv_messages
                )

            del self.message_tracker[req_id]
            return

        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                batches=1,
                message=ID
            )
        )

    def _process_prepare(self, req_id: int, _: int, exchange_id: int):
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        logging.info(
            "[%s] - Send %s downloaded images to filter_%s",
            req_id,
            tracker.recv_messages,
            exchange_id
        )

        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                batches=1,
                message=tracker.recv_messages
            ),
            exchange_id=exchange_id
        )

    def _process_ok(self, req_id: int, _: int, articles_recv: int):
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        tracker.record_peer(articles_recv)

        if tracker.all_peers(IMAGE_DOWNLOADER_AMOUNT - 1):
            if tracker.all_recv():
                logging.info(f"[{req_id}] - All of the images have been downloaded !!!")

                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id
                    )
                )

                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_2",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                del self.requests_states[req_id]
            else:
                logging.info(f"[{req_id}] - There are still images that have not been downloaded")
                
                tracker.reset_peers()
                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        batches=1,
                        message=ID
                    )
                )

    def _delete_client(self, req_id: int, *_):
        logging.info(f"[{req_id}] - Eliminating the client")
        self.message_tracker.pop(req_id, None)

    def _recv_message(self, message, ack, nack):
        try:
            opcode, *args = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._download_image(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._process_eof(*args)
            elif opcode == message_protocol.OPCODE_PREPARE:
                self._process_prepare(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._process_ok(*args)
            elif opcode == message_protocol.OPCODE_COMMIT:
                self._delete_client(*args)
            else:
                logging.warning(f"Unexpected opcode={opcode}")
            ack()
        except:
            logging.exception("Unexpected error")
            nack()
        
    def start(self):
        try:
            self.exchange_consumer = threading.Thread(
                target=self.input_exchange.start_consuming,
                args=(self._recv_message,)
            )
            self.exchange_consumer.start()

            logging.info("Waiting to receive messages")
            self.input_queue.start_consuming(self._recv_message)
        except Exception:
            if self.is_running:
                logging.exception("An exception has occured")
                raise
            logging.warning("Interrupted by the SIGTERM")
        finally:
            self.input_queue.close()
            self._wait_for_threads()

def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    logging.getLogger("pika").propagate = False

    try:
        ImageDownloader().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    main()