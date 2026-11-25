import os
import json
import time
import zlib
import cache
import base64
import signal
import logging
import requests
import threading

from collections.abc import Callable
from datetime import datetime, timezone
from urllib.parse import unquote, urlsplit
from email.utils import parsedate_to_datetime
from common import entity, fault_tolerance, message_protocol, middleware, rabbitmq, storage

ID = int(os.environ["ID"])
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
IMAGE_DOWNLOADER_INSTANCES = int(os.environ.get("IMAGE_DOWNLOADER_INSTANCES", 1))
IMAGE_DOWNLOADER_PREFIX = os.environ.get("IMAGE_DOWNLOADER_PREFIX", "image_downloader")
IMAGE_DOWNLOADER_QUEUE_PREFIX = os.environ.get("IMAGE_DOWNLOADER_QUEUE_PREFIX", "image_downloader_queue")

IMAGE_DOWNLOADER_CACHE_DIR = os.environ.get("IMAGE_DOWNLOADER_CACHE_DIR", "tmp/cache")
CACHE_TTL_SECONDS = int(os.environ.get("IMAGE_DOWNLOADER_CACHE_TTL", str(24 * 3600)))

# Single store for per-message idempotency and crash recovery. Keys:
#   "data:{req_id}:{batch_id}" -> DATA batches
#       PENDING   : {"stage": PENDING,   "payload": [...]}
#       COMPLETED : {"stage": COMPLETED, "recv_delta": N}
#   "eof:{req_id}:{batch_id}"  -> EOF messages
#       EOF_PENDING   : {"stage": EOF_PENDING,   "expected": N, "batch_id": ...}
#       EOF_COMPLETED : {"stage": EOF_COMPLETED}
IMAGE_DOWNLOADER_STATE_PATH = os.environ.get("IMAGE_DOWNLOADER_STATE_PATH", "tmp/state.json")

PENDING_STAGE = "PENDING"
COMPLETED_STAGE = "COMPLETED"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_COMPLETED_STAGE = "EOF_COMPLETED"


HTTP_RETRIES = 3
THUMBNAIL_WIDTH = 1024
WIKIMEDIA_API_URL = "https://commons.wikimedia.org/w/api.php"

def _get(session: requests.Session, url: str, timeout: float, params: dict | None = None):

    def _retry_after(response, fallback: float):
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

    headers = {"User-Agent": "WikiDistri/1.0 (finfanti@fi.uba.ar)"}
    for attempt in range(HTTP_RETRIES + 1):
        response = session.get(url=url, headers=headers, params=params, timeout=timeout)
        if response.status_code != 429 or attempt == HTTP_RETRIES:
            response.raise_for_status()
            return response

        delay = _retry_after(response, fallback=2 ** attempt)
        response.close()
        logging.warning("Wikimedia returned HTTP 429; retrying in %.1f seconds", delay)
        time.sleep(delay)


def _download(session: requests.Session, url: str, timeout: float):
    
    def _thumbnail_url(session: requests.Session, url: str, timeout: float):
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

    thumbnail_url = _thumbnail_url(session=session, url=url, timeout=timeout)
    response = _get(session=session, url=thumbnail_url, timeout=timeout)

    image = base64.b64encode(response.content).decode(message_protocol.ENCODING)
    image_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip()

    return thumbnail_url, image, image_type


class ImageDownloader:
    _TIMEOUT = 5.0
    _ENVELOPE_OVERHEAD = 64
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_2"

    def __init__(self):
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)
        
        self._is_running = True
        self._session = requests.Session()

        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        self._state = storage.AtomicStore(IMAGE_DOWNLOADER_STATE_PATH)
        self._cache = cache.ImageCache(IMAGE_DOWNLOADER_CACHE_DIR, CACHE_TTL_SECONDS)
        self._pending_eof: dict[int, str] = {}

        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE],
        )

        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=IMAGE_DOWNLOADER_PREFIX,
            instances=IMAGE_DOWNLOADER_INSTANCES,
            on_all_recv=self._on_recv_all,
            get_tracker=self._get_tracker,
            state_path="tmp/eof/state.json",
        )
        self.coordinator.start()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{IMAGE_DOWNLOADER_QUEUE_PREFIX}_{ID}",
        )

        self.__post_init__()

    def __post_init__(self):
        for msg_key in self._state.pending(COMPLETED_STAGE):
            try:
                _, req_id_str, _ = msg_key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed COMPLETED key: %s", msg_key)
                continue

            rec = self._state.get(msg_key)
            tracker = self._get_tracker(req_id)
            tracker.record_recv(rec.get("recv_delta", 0))

        for msg_key in self._state.pending(PENDING_STAGE):
            try:
                _, req_id_str, batch_id_str = msg_key.split(":", 2)
                req_id = int(req_id_str)
                batch_id = int(batch_id_str)
            except ValueError:
                logging.warning("Skipping malformed PENDING key: %s", msg_key)
                continue

            rec = self._state.get(msg_key)
            payload = rec.get("payload")
            if payload is None:
                logging.error(
                    "Cannot replay batch req_id=%s batch_id=%s — payload missing",
                    req_id,
                    batch_id,
                )
                continue

            logging.info("Replaying PENDING batch req_id=%s batch_id=%s", req_id, batch_id)
            recv = self._download_image(batch_id, req_id, None, payload)

            self._state.put(msg_key, {
                "stage": COMPLETED_STAGE,
                "recv_delta": recv,
            })
            self._cache.delete_batch(payload)

            with self._trackers_lock:
                tracker = self._get_tracker(req_id)
                tracker.record_recv(recv)

        self._cache.sweep()

        for eof_key in self._state.pending(EOF_PENDING_STAGE):
            try:
                _, req_id_str, _ = eof_key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed EOF_PENDING key: %s", eof_key)
                continue

            rec = self._state.get(eof_key)
            expected = rec.get("expected", 0)

            logging.info("Replaying EOF_PENDING req_id=%s with expected_messages=%s", req_id, expected)
            self._pending_eof[req_id] = eof_key
            already_finished = self.coordinator.on_eof(req_id=req_id, expected_messages=expected)
            if already_finished:
                self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
                self._pending_eof.pop(req_id, None)


    def _new_tracker(self):
        return entity.message_tracker.MessageTracker()

    def _get_tracker(self, req_id: int):
        with self._trackers_lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = self._new_tracker()
            return tracker


    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, proceed to shutdown the service...")
        self._is_running = False
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


    def _build_batches(self, records: list) -> list[list]:
        """Group downloaded image records into batches respecting BATCH_SIZE and RabbitMQ's max message size."""
        max_message_size = message_protocol.MAX_RABBITMQ_MESSAGE_SIZE
        batches = []
        batch, batch_size = [], self._ENVELOPE_OVERHEAD

        for record in records:
            record_size = len(json.dumps(record).encode(message_protocol.ENCODING)) + 2

            if batch and (len(batch) >= BATCH_SIZE or batch_size + record_size > max_message_size):
                batches.append(batch)
                batch, batch_size = [], self._ENVELOPE_OVERHEAD

            if self._ENVELOPE_OVERHEAD + record_size > max_message_size:
                raise ValueError(
                    f"Serialized image record exceeds RabbitMQ's {max_message_size}-byte limit"
                )

            batch.append(record)
            batch_size += record_size

        if batch:
            batches.append(batch)

        return batches

    def _flush_batches(self, batches: list[list], req_id: int, batch_id: int):
        # For the same batch, each batch is going to have the same amount of elements
        for idx, batch in enumerate(batches):
            message_id = zlib.crc32(f"{IMAGE_DOWNLOADER_PREFIX}_{ID}_{batch_id}_{idx}".encode())
            logging.debug(
                "Sending OPCODE_DATA batch of %s messages to %s for req_id=%s",
                len(batch),
                self._GATEWAY_QUEUE,
                req_id,
            )
            self.publisher.publish_queue(
                queue=self._GATEWAY_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    message_id=message_id,
                    req_id=req_id,
                    batches=len(batch),
                    message=batch,
                ),
            )

    def _download_image(self, batch_id: int, req_id: int, _: int | None, messages: list) -> int:
        logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")
        recv = 0
        results = []

        for message in messages:
            name, url, image = message
            image_url = url
            image_bytes = image_type = None
            recv += 1

            if image:
                cached = self._cache.get(image)
                if cached is not None:
                    logging.info(f"Cache hit for image {image} for req_id={req_id}")
                    image_url, image_bytes, image_type = cached
                else:
                    try:
                        logging.info(f"Downloading image {image} for req_id={req_id}")
                        image_url, image_bytes, image_type = _download(
                            session=self._session,
                            url=image,
                            timeout=self._TIMEOUT * 2,
                        )
                        try:
                            self._cache.put(image, (image_url, image_bytes, image_type))
                        except OSError:
                            logging.warning(f"Could not cache image {image}", exc_info=True)
                    except requests.HTTPError as e:
                        logging.warning(
                            f"HTTP Error: {e.response.status_code}; reason: {e.response.reason}"
                        )
                    except requests.RequestException as e:
                        logging.warning(f"Request failed for {image}: {e}")
                    except ValueError as e:
                        logging.warning(f"Could not resolve thumbnail for {image}: {e}")

            results.append([name, url, image_url, image_type, image_bytes])

        batches = self._build_batches(results)
        self._flush_batches(batches=batches, req_id=req_id, batch_id=batch_id)

        return recv

    def _on_recv_all(self, req_id: int, _):
        eof_key = self._pending_eof[req_id]
        batch_id = self._state.get(eof_key)["batch_id"]

        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        message_id = zlib.crc32(f"{IMAGE_DOWNLOADER_PREFIX}_{ID}_{batch_id}_{GATEWAY_PREFIX}".encode())
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=message_id,
                req_id=req_id,
            ),
        )

        self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
        self._pending_eof.pop(req_id, None)

        with self._trackers_lock:
            self._trackers.pop(req_id, None)


    def _handle_data(self, ack: Callable, *args):
        batch_id, req_id, _, messages = args
        msg_key = f"data:{req_id}:{batch_id}"

        stage = self._state.get(msg_key).get("stage")
        if stage is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": messages})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        recv = self._download_image(*args)
        fault_tolerance.crash(at="AFTER_DATA_DOWNLOAD")

        self._state.put(msg_key, {
            "stage": COMPLETED_STAGE,
            "recv_delta": recv,
        })
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")
        self._cache.delete_batch(messages)

        with self._trackers_lock:
            tracker = self._get_tracker(req_id)
            tracker.record_recv(recv)

    def _handle_eof(self, ack: Callable, *args):
        batch_id, req_id, _, expected_messages = args
        eof_key = f"eof:{req_id}:{batch_id}"

        stage = self._state.get(eof_key).get("stage")
        if stage is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        fault_tolerance.crash(at="BEFORE_SAVE_EOF")
        self._state.put(eof_key, {
            "stage": EOF_PENDING_STAGE,
            "expected": expected_messages,
            "batch_id": batch_id,
        })
        fault_tolerance.crash(at="AFTER_SAVE_EOF")

        self._pending_eof[req_id] = eof_key

        ack()
        fault_tolerance.crash(at="AFTER_EOF_ACK")

        already_finished = self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)
        if already_finished:
            self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
            self._pending_eof.pop(req_id, None)
        fault_tolerance.crash(at="AFTER_STARTING_EOF")

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        try:
            if opcode == message_protocol.OPCODE_DATA:
                self._handle_data(ack, *args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._handle_eof(ack, *args)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
                ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()


    def start(self):
        try:
            logging.info("Waiting to receive messages...")
            self.input_queue.start_consuming(self._process_message)
        except Exception:
            if self._is_running:
                logging.critical("An unexpected error occurred while running the service", exc_info=True)
                raise
            logging.debug("Interrupted by the SIGTERM signal")
        finally:
            self._close()


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    )
    logging.getLogger("pika").propagate = False
    try:
        ImageDownloader().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())