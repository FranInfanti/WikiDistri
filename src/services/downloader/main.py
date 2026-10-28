import os
import signal
import base64
import logging
import requests
import threading

from common import middleware, message_protocol, entity

MOM_HOST = os.environ["MOM_HOST"]

ID = int(os.environ["ID"])
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
IMAGE_DOWNLOADER_AMOUNT = int(os.environ["IMAGE_DOWNLOADER_AMOUNT"])
IMAGE_DOWNLOADER_PREFIX = os.environ["IMAGE_DOWNLOADER_PREFIX"]
IMAGE_DOWNLOADER_INPUT_QUEUE = os.environ["IMAGE_DOWNLOADER_INPUT_QUEUE"]

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
            queue_name=IMAGE_DOWNLOADER_INPUT_QUEUE
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{GATEWAY_PREFIX}_2"
        )

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=MOM_HOST,
            exchange_name=IMAGE_DOWNLOADER_PREFIX,
            routing_keys=[f"{IMAGE_DOWNLOADER_PREFIX}_{ID}"]
        )

        self.output_exchanges = define_exchanges(
            host=MOM_HOST,
            prefix=IMAGE_DOWNLOADER_PREFIX,
            amount=IMAGE_DOWNLOADER_AMOUNT,
            exclude_id=ID
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            pass

    def _publish_message(self, message: bytes, exchange_id: int = None):
        exchanges = [self.output_exchanges[exchange_id]] \
            if exchange_id is not None else self.output_exchanges.values()

        for exchange in exchanges:
            exchange.send(message)

    def _download_image(self, req_id: int, batches: int, messages: list):
        logging.info(f"[{req_id}] - Downloading {batches} images")

        for message in messages:
            name, url, image = message
            image_bytes = None

            if image:
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0"
                    }
                    response = requests.get(url=image, headers=headers)
                    response.raise_for_status()
                    image_bytes = base64.b64encode(response.content).decode(message_protocol.ENCODING)
                except requests.HTTPError as e:
                    logging.warning(
                        f"HTTP Error: {e.response.status_code}; reason: {e.response.reason}"
                    )
                except requests.RequestException as e:
                    logging.warning(f"Request failed for {image}: {e}")

            self.output_queue.send(
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[[name, url, image_bytes]]
                )
            )

            self.requests_states.setdefault(req_id, entity.request_state.RequestState()).record_article()
        
    def _process_eof(self, req_id: int, _: int, articles_sent: int):
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        request_state.record_eof(articles_sent)

        if IMAGE_DOWNLOADER_AMOUNT == 1:
            if request_state.all_articles_recv():
                logging.info(f"[{req_id}] - All articles have been processed")
                self.output_queue.send(
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
        
        self._publish_message(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                batches=1,
                message=ID
            )
        )

    def _send_articles_recv(self, req_id: int, _: int, id: int):
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        
        logging.info(f"[{req_id}] - Articles received: {request_state.articles_recv}")
        self._publish_message(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                batches=1,
                message=request_state.articles_recv
            ),
            exchange_id=id
        )

    def _check_articles_recv(self, req_id: int, _: int, articles_recv: int):
        request_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        request_state.record_peer_count(articles_recv)

        if request_state.all_peers(IMAGE_DOWNLOADER_AMOUNT):
            if request_state.all_articles_recv():
                logging.info(f"[{req_id}] - All articles were processed")

                self._publish_message(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id
                    )
                )

                self.output_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                del self.requests_states[req_id]
            else:
                logging.info(f"[{req_id}] - There are still items that were not processed.")
                
                request_state.reset_peers_count()
                self._publish_message(
                    message_protocol.internal.serialize(
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
                self._send_articles_recv(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._check_articles_recv(*args)
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

            logging.info(f"Start consuming from the input queue: {IMAGE_DOWNLOADER_INPUT_QUEUE}")
            self.input_queue.start_consuming(self._recv_message)
        except Exception:
            if self.is_running:
                logging.exception("An exception has occured")
                raise
            logging.warning("Interrupted by the SIGTERM")
        finally:
            logging.info("Releasing the resources")

            self.input_queue.close()
            self.output_queue.close()
            self.input_exchange.close()

            for output_exchange in self.output_exchanges.values():
                output_exchange.close()

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