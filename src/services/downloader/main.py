import os
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

    def __init__(self):
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)
        
        self.is_running = True
        self.requests_states = {}
        self.exchange_consumer = None

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

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("Failed to stop the input queue consumer during shutdown", exc_info=True)

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            logging.warning("Failed to stop the input exchange consumer during shutdown", exc_info=True)

    def _download_image(self, req_id: int, batches: int, messages: list):
        logging.info(f"[{req_id}] - Downloading {batches} images")

        for message in messages:
            name, url, image = message
            image_bytes = None
            image_content_type = None

            if image:
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0"
                    }
                    response = requests.get(url=image, headers=headers, timeout=(5, 30))
                    response.raise_for_status()
                    image_bytes = base64.b64encode(response.content).decode(message_protocol.ENCODING)
                    image_content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
                    logging.info(f"[{req_id}] - Image downloaded")
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

            self.requests_states.setdefault(req_id, entity.request_state.RequestState()).record_article()
        
    def _process_eof(self, req_id: int, _: int, articles_sent: int):
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        request_state.record_eof(articles_sent)

        if IMAGE_DOWNLOADER_AMOUNT == 1:
            if request_state.all_articles_recv():
                logging.info(f"[{req_id}] - All articles have been processed")

                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_2",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )
            else:
                logging.error(
                    f"[{req_id}] - Article count mismatch: expected %s, received %s",
                    request_state.expected_articles,
                    request_state.articles_recv
                )

            del self.requests_states[req_id]
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
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        
        logging.info(f"[{req_id}] - Articles received: {request_state.articles_recv}")

        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                batches=1,
                message=request_state.articles_recv
            ),
            exchange_id=exchange_id
        )

    def _process_ok(self, req_id: int, _: int, articles_recv: int):
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        request_state.record_peer_count(articles_recv)

        if request_state.all_peers(IMAGE_DOWNLOADER_AMOUNT):
            if request_state.all_articles_recv():
                logging.info(f"[{req_id}] - All articles were processed")

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
                logging.info(f"[{req_id}] - There are still items that were not processed.")
                
                request_state.reset_peers_count()
                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        message=ID
                    )
                )

    def _delete_client(self, req_id: int, *_):
        logging.info(f"[{req_id}] - Deleting client")
        self.requests_states.pop(req_id, None)

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
            logging.info(f"Start consuming from the inner exchange: {IMAGE_DOWNLOADER_PREFIX}_{ID}")
            self.exchange_consumer = threading.Thread(
                target=self.input_exchange.start_consuming,
                args=(self._recv_message,)
            )
            self.exchange_consumer.start()

            logging.info(f"Start consuming from the input queue: {IMAGE_DOWNLOADER_QUEUE}")
            self.input_queue.start_consuming(self._recv_message)
        except Exception:
            if self.is_running:
                logging.exception("An exception has occured")
                raise
            logging.warning("Interrupted by the SIGTERM")
        finally:
            logging.info("Releasing the resources")
            if self.exchange_consumer is not None:
                self.exchange_consumer.join()
            self.publisher.stop()
            self.input_queue.close()
            self.input_exchange.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    try:
        ImageDownloader().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    main()