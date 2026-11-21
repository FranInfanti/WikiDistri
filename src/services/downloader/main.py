import os
import time
import signal
import base64
import logging
import requests

from datetime import datetime, timezone
from urllib.parse import unquote, urlsplit
from email.utils import parsedate_to_datetime
from common import middleware, message_protocol, rabbitmq

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
IMAGE_DOWNLOADER_INSTANCES = int(os.environ.get("IMAGE_DOWNLOADER_INSTANCES", 1))
IMAGE_DOWNLOADER_PREFIX = os.environ.get("IMAGE_DOWNLOADER_PREFIX", "image_downloader")
IMAGE_DOWNLOADER_QUEUE = os.environ.get("IMAGE_DOWNLOADER_QUEUE", "image_downloader_queue")

HTTP_RETRIES = 3
THUMBNAIL_WIDTH = 1024
WIKIMEDIA_API_URL = "https://commons.wikimedia.org/w/api.php"

def _retry_after(response, fallback: float) -> float:
    value = response.headers.get("Retry-After")
    if not value:
        return fallback

    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return fallback

def _get(session: requests.Session, url: str, timeout: float, params: dict | None = None):
    headers = {"User-Agent": "WikiDistri/1.0 (francisco.infanti@gmail.com)"}
    for attempt in range(HTTP_RETRIES + 1):
        response = session.get(url=url, headers=headers, params=params, timeout=timeout)
        if response.status_code != 429 or attempt == HTTP_RETRIES:
            response.raise_for_status()
            return response

        delay = _retry_after(response, fallback=2 ** attempt)
        response.close()
        logging.warning("Wikimedia returned HTTP 429; retrying in %.1f seconds", delay)
        time.sleep(delay)

def _thumbnail_url(session: requests.Session, url: str, timeout: float) -> str:
    filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    if not filename:
        raise ValueError(f"Image URL has no filename: {url}")

    response = _get(
        session=session,
        url=WIKIMEDIA_API_URL,
        timeout=timeout,
        params={
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "prop": "imageinfo",
            "iiprop": "url",
            "iiurlwidth": THUMBNAIL_WIDTH,
            "titles": f"File:{filename}",
        },
    )
    pages = response.json().get("query", {}).get("pages", [])
    for page in pages:
        imageinfo = page.get("imageinfo", [])
        if imageinfo and imageinfo[0].get("thumburl"):
            return imageinfo[0]["thumburl"]

    raise ValueError(f"Wikimedia did not return a thumbnail URL for {filename}")

def _download(session: requests.Session, url: str, timeout: float):
    thumbnail_url = _thumbnail_url(session=session, url=url, timeout=timeout)
    response = _get(session=session, url=thumbnail_url, timeout=timeout)

    image = base64.b64encode(response.content).decode(message_protocol.ENCODING)
    image_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()

    return thumbnail_url, image, image_type

class ImageDownloader:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_2"

    def __init__(self):
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)
        
        self.is_running = True
        self.session = requests.Session()
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=IMAGE_DOWNLOADER_QUEUE
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=IMAGE_DOWNLOADER_PREFIX,
            instances=IMAGE_DOWNLOADER_INSTANCES,
            on_all_recv=self._on_recv_all
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE],
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
            logging.warning("An error occurred while trying to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _download_image(self, req_id: int, _: int, messages: list):
        logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")
        for message in messages:
            name, url, image = message
            image_url = url
            image_bytes = None
            image_type = None

            if image:
                try:
                    logging.info(f"Downloading image {image} for req_id={req_id}")
                    image_url, image_bytes, image_type = _download(
                        session=self.session,
                        url=image,
                        timeout=self._TIMEOUT * 2
                    )
                except requests.HTTPError as e:
                    logging.warning(
                        f"HTTP Error: {e.response.status_code}; reason: {e.response.reason}"
                    )
                except requests.RequestException as e:
                    logging.warning(f"Request failed for {image}: {e}")
                except ValueError as e:
                    logging.warning(f"Could not resolve thumbnail for {image}: {e}")

            logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=f"{GATEWAY_PREFIX}_2",
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[[name, url, image_url, image_type, image_bytes]]
                )
            )

            self.coordinator.record_recv(req_id=req_id)

    def _on_recv_all(self, req_id: int, _: list):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
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
                logging.debug(f"Received an OPCODE_EOF message for req_id={req_id}")
                self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
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
                logging.critical("An unexpected error occurred while running the service", exc_info=True)
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