import os
import json
import time
import zlib
import signal
import logging
import threading

from datetime import datetime
from collections.abc import Callable
from common import entity, message_protocol, middleware, rabbitmq, storage, fault_tolerance

ID = int(os.environ["ID"])
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
FILTER_INSTANCES = int(os.environ.get("FILTER_INSTANCES", 1))
FILTER_PREFIX = os.environ.get("FILTER_PREFIX", "filter")
FILTER_QUEUE_PREFIX = os.environ.get("FILTER_QUEUE_PREFIX", "filter_queue")
IMAGE_DOWNLOADER_INSTANCES = int(os.environ.get("IMAGE_DOWNLOADER_INSTANCES", 1))
IMAGE_DOWNLOADER_PREFIX = os.environ.get("IMAGE_DOWNLOADER_PREFIX", "image_downloader_queue")

# Single store for per-message idempotency and crash recovery.  Keys:
#   "data:{req_id}:{batch_id}"  → DATA batches
#       PENDING   : {"stage": PENDING,   "payload": [...]}
#       COMPLETED : {"stage": COMPLETED, "recv_delta": N, "sent_delta": N}
#   "eof:{req_id}:{batch_id}"   → EOF messages
#       EOF_PENDING   : {"stage": EOF_PENDING,   "expected": N, "batch_id": N}
#       EOF_COMPLETED : {"stage": EOF_COMPLETED}
FILTER_STATE_PATH = os.environ.get("FILTER_STATE_PATH", f"tmp/state.json")

PENDING_STAGE = "PENDING"
COMPLETED_STAGE = "COMPLETED"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_COMPLETED_STAGE = "EOF_COMPLETED"

def section_contains(sections: str | None, *words: str):
    if not sections:
        return False

    parsed_sections = json.loads(sections)

    return any(
        section.get("name", "").casefold() == word.casefold()
        for section in parsed_sections
        for word in words
    )

class Filter:
    _TIMEOUT = 10.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_1"
    _MAX_GATEWAY_MESSAGE_SIZE = message_protocol.MAX_RABBITMQ_MESSAGE_SIZE

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.image_downloader_queue_names = [f"{IMAGE_DOWNLOADER_PREFIX}_{i}" for i in range(IMAGE_DOWNLOADER_INSTANCES)]
        self.publisher_downloader = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[IMAGE_DOWNLOADER_QUEUE],
        )
        self.publisher_gateway = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE],
        )

        self._is_running = True

        # In-memory trackers keyed by req_id. Rebuilt from _state deltas
        # on startup; updated after each batch is processed.
        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        # Single persistent store: handles both idempotency and the data
        # needed to rebuild in-memory trackers after a crash.
        self._state = storage.AtomicStore(FILTER_STATE_PATH)

        # Maps req_id → eof_key (the _state key for that client's pending
        # EOF). Written by the consumer thread before coordinator.on_eof();
        # read by the coordinator thread in _on_recv_all.
        self._pending_eof: dict[int, str] = {}

        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=FILTER_PREFIX,
            instances=FILTER_INSTANCES,
            on_all_recv=self._on_recv_all,
            get_tracker=self._get_tracker,
            state_path=f"tmp/eof/state.json"
        )
        self.coordinator.start()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{FILTER_QUEUE_PREFIX}_{ID}"
        )

        self.__post_init__()

    def __post_init__(self):
        # Reconstruct tracker totals by summing the deltas of every COMPLETED data batch.
        for msg_key in self._state.pending(COMPLETED_STAGE):
            try:
                _, req_id_str, _ = msg_key.split(":")
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed COMPLETED key: %s", msg_key)
                continue

            rec = self._state.get(msg_key)
            tracker = self._get_tracker(req_id)
            tracker.record_recv(rec.get("recv_delta", 0))
            tracker.record_sent(IMAGE_DOWNLOADER_PREFIX, rec.get("sent_delta", 0))

        # Replay any DATA batch that was ACKed but not yet COMPLETED.
        for msg_key in self._state.pending(PENDING_STAGE):
            try:
                _, req_id_str, batch_id_str = msg_key.split(":")
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
                    req_id, batch_id,
                )
                continue

            logging.info("Replaying PENDING batch req_id=%s batch_id=%s", req_id, batch_id)
            recv, sent = self._filter_message(batch_id, req_id, None, payload)

            self._state.put(msg_key, {
                "stage": COMPLETED_STAGE,
                "recv_delta": recv,
                "sent_delta": sent,
            })

            with self._trackers_lock:
                tracker = self._get_tracker(req_id)
                tracker.record_recv(recv)
                tracker.record_sent(IMAGE_DOWNLOADER_PREFIX, sent)

        # Re-trigger any EOF whose coordinator call did not complete before crash.
        for eof_key in self._state.pending(EOF_PENDING_STAGE):
            try:
                _, req_id_str, _ = eof_key.split(":")
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed EOF_PENDING key: %s", eof_key)
                continue

            rec = self._state.get(eof_key)

            logging.info("Replaying EOF_PENDING req_id=%s with expected_messages=%s", req_id, rec.get("expected", 0))
            self._pending_eof[req_id] = eof_key
            already_finished = self.coordinator.on_eof(req_id=req_id, expected_messages=rec.get("expected", 0))
            if already_finished:
                self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
                self._pending_eof.pop(req_id, None)


    def _new_tracker(self):
        tracker = entity.message_tracker.MessageTracker()
        tracker.record_sent(IMAGE_DOWNLOADER_PREFIX, 0)
        return tracker

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
            ("downloader publisher", self.publisher_downloader.stop),
            ("gateway publisher", self.publisher_gateway.stop),
        )
        for name, stop in components:
            if stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append(name)

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")


    def _publish_batch(self, req_id: int, dest: str, batch: list, message_id: int):
        if dest == GATEWAY_PREFIX:
            publisher = self.publisher_gateway
            queue = self._GATEWAY_QUEUE
        elif dest == IMAGE_DOWNLOADER_PREFIX:
            publisher = self.publisher_downloader
            queue = self.image_downloader_queue_names[message_id % IMAGE_DOWNLOADER_INSTANCES]
        else:
            raise ValueError(f"Unknown batch destination: {dest}")

        logging.debug(
            "Sending OPCODE_DATA batch of %s messages to %s for req_id=%s",
            len(batch), dest, req_id,
        )

        publisher.publish_queue(
            queue=queue,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(batch),
                message=batch,
            )
        )

    def _flush_batches(self, out: dict, req_id: int, batch_id: int):
        for dest, batch in out.items():
            if not batch:
                continue

            message_id = zlib.crc32(f"{FILTER_PREFIX}_{ID}_{batch_id}_{dest}".encode())
            self._publish_batch(req_id=req_id, dest=dest, batch=batch, message_id=message_id)

    def _filter_message(self, batch_id: int, req_id: int, _: int, messages: list[list]):
        logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")

        out = {
            GATEWAY_PREFIX: [],
            IMAGE_DOWNLOADER_PREFIX: [],
        }

        recv = sent = 0
        for message in messages:
            name, url, date_modified, image, in_language, sections = message
            recv += 1

            date_modified = datetime.strptime(date_modified, "%Y-%m-%d %H:%M:%S") if date_modified else None
            if date_modified is None:
                logging.warning(f"Received a message with an invalid date_modified value for req_id={req_id}: {message}")
                continue

            if in_language == "en" and date_modified.year <= 2020:
                out[GATEWAY_PREFIX].append([url, date_modified.strftime("%Y-%m-%d %H:%M:%S")])

            if date_modified.year <= 2020 and section_contains(sections, "biography", "biographie"):
                out[IMAGE_DOWNLOADER_PREFIX].append([name, url, image])
                sent += 1

        self._flush_batches(out=out, req_id=req_id, batch_id=batch_id)
        return recv, sent


    def _on_recv_all(self, req_id: int, sent_messages: list):
        # sent_messages = [(key, sent), ..., (key, sent)]
        key, sent = sent_messages[0]

        if key != IMAGE_DOWNLOADER_PREFIX:
            raise ValueError(f"Unknown key; expected: {IMAGE_DOWNLOADER_PREFIX}, received: {key}")

        # Retrieve the batch_id that was stored in _state when the EOF arrived.
        eof_key  = self._pending_eof[req_id]
        batch_id = self._state.get(eof_key)["batch_id"]

        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        message_id = zlib.crc32(f"{FILTER_PREFIX}_{ID}_{batch_id}_{GATEWAY_PREFIX}".encode())
        self.publisher_gateway.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id
            )
        )

        logging.debug(f"Sending OPCODE_EOF message to {IMAGE_DOWNLOADER_PREFIX} for req_id={req_id}; expected messages: {sent}")

        message_id = zlib.crc32(f"{FILTER_PREFIX}_{ID}_{batch_id}_{IMAGE_DOWNLOADER_PREFIX}".encode())
        self.publisher_downloader.publish_queue(
            queue=IMAGE_DOWNLOADER_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=f"{FILTER_PREFIX}_{ID}_{message_id}",
                req_id=req_id,
                batches=1,
                message=sent
            )
        )

        # Mark this EOF as fully completed and clean up all state for req_id.
        fault_tolerance.crash(at="AFTER_EOF_COMPLETED")
        self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
        fault_tolerance.crash(at="BEFORE_EOF_COMPLETED")

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

        # Persist payload + mark PENDING in one atomic write before ACK.
        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": messages})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        recv, sent = self._filter_message(*args)
        fault_tolerance.crash(at="AFTER_DATA_FILTER")

        self._state.put(msg_key, {
            "stage": COMPLETED_STAGE,
            "recv_delta": recv,
            "sent_delta": sent,
        })
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

        with self._trackers_lock:
            tracker = self._get_tracker(req_id)
            tracker.record_recv(recv)
            tracker.record_sent(IMAGE_DOWNLOADER_PREFIX, sent)

    def _handle_eof(self, ack: Callable, *args):
        batch_id, req_id, _, expected_messages = args
        eof_key = f"eof:{req_id}:{batch_id}"

        stage = self._state.get(eof_key).get("stage")
        if stage is not None: # It is either on EOF_COMPLETED_STAGE or EOF_PENDING_STAGE
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        # Persist EOF intent before ACK so we can re-trigger coordinator after a crash.
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

        self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        # *args = batch_id, req_id, batches, payload
        try:
            if opcode == message_protocol.OPCODE_DATA:
                self._handle_data(ack, *args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._handle_eof(ack, *args)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()


    def start(self):
        try:
            logging.info("Waiting to receive messages...")
            self.input_queue.start_consuming(self._process_message)
        except:
            if self._is_running:
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
        Filter().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())