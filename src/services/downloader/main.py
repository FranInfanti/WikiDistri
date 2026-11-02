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
        
        self.is_running = True
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=IMAGE_DOWNLOADER_QUEUE
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=IMAGE_DOWNLOADER_PREFIX,
            instances=IMAGE_DOWNLOADER_AMOUNT,
            on_all_recv=self._on_recv_all
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[f"{GATEWAY_PREFIX}_2"],
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, proceed to shutdown the service...")
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to stop")

        timed_out = []
        components = (
            ("coordinator", self.coordinator.stop),
            ("publisher", self.publisher.stop),
        )
        for name, stop in components:
            if stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append(name)

        try:
            self.input_queue.close()
        except Exception:
            logging.exception("Failed to close the input queue")

        if timed_out:
            logging.error("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _download_image(self, req_id: int, _: int, messages: list):
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

            self.coordinator.record_recv(req_id=req_id)

    def _on_recv_all(self, req_id: int, _: list):
        logging.debug(f"Sending OPCODE_EOF message for req_id={req_id}")

        self.publisher.publish_queue(
            queue=f"{GATEWAY_PREFIX}_2",
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id
            )
        )

    def _process_message(self, message, ack, nack):
        try:
            opcode, *args = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._download_image(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                req_id, _, expected_messages = args
                self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)
            else:
                logging.warning(f"Unexpected OPCODE={opcode} received")
            ack()
        except:
            logging.exception("An error occured while processing a message")
            nack()
        
    def start(self):
        try:
            logging.info("Waiting to receive messages...")
            self.coordinator.start()
            self.input_queue.start_consuming(self._process_message)
        except Exception:
            if self.is_running:
                logging.critical(f"An error has occurred in the service")
                raise
            logging.debug("Interrupted by the SIGTERM signal")
        finally:
            self._close()

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
    SystemExit(main())