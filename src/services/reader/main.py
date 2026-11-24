from collections.abc import Callable
import os
import re
import threading
import time
import signal
import logging

from datetime import datetime
import zlib
from stopwords import STOPWORDS
from common import entity, fault_tolerance, middleware, message_protocol, hashing, rabbitmq, storage

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
WORD_READER_INSTANCES = int(os.environ.get("WORD_READER_INSTANCES", 1))
WORD_READER_PREFIX = os.environ.get("WORD_READER_PREFIX", "word_reader")
WORD_READER_QUEUE = os.environ.get("WORD_READER_QUEUE", "word_reader_queue")
SUM_COUNT_INSTANCES = int(os.environ.get("SUM_COUNT_INSTANCES", 1))
SUM_COUNT_QUEUE = os.environ.get("SUM_COUNT_QUEUE", "sum_count_queue")
SUM_AMOUNT_INSTANCES = int(os.environ.get("SUM_AMOUNT_INSTANCES", 1))
SUM_AMOUNT_QUEUE = os.environ.get("SUM_AMOUNT_QUEUE", "sum_amount_queue")
JOIN_AVERAGE_INSTANCES = int(os.environ.get("JOIN_AVERAGE_INSTANCES", 1))
JOIN_AVERAGE_QUEUE = os.environ.get("JOIN_AVERAGE_QUEUE", "join_average_queue")

_CUTOFF_YEAR = 2021
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

WORD_REGEX = r'\b\w+\b'
NUMBER_REGEX = r'[+-]?((\d+\.?\d*)|(\.\d+))'
_WORD_RE = re.compile(WORD_REGEX, re.UNICODE)
_NUMBER_RE = re.compile(NUMBER_REGEX)

READER_STATE_PATH = os.environ.get("READER_STATE_PATH", f"tmp/{WORD_READER_PREFIX}/{ID}/state.json")

PENDING_STAGE = "PENDING"
COMPLETED_STAGE = "COMPLETED"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_COMPLETED_STAGE = "EOF_COMPLETED"

def tokenize(abstract: str, language: str | None) -> list[str]:
    words = _WORD_RE.findall(abstract.lower())
    return [ w for w in words if not _NUMBER_RE.fullmatch(w) and w not in STOPWORDS ]

def language_counts(language: str | None) -> tuple[int, int] | None:
    if language == "en":
        return 1, 0
    if language == "fr":
        return 0, 1
    return None

class WordReader:
    _TIMEOUT = 5.0

    @staticmethod
    def _collect_unique_words(req_id: int, words: list, language: str | None, words_by_shard: dict):
        counts = language_counts(language)
        if counts is None:
            return

        en_count, fr_count = counts
        for word in set(words):
            shard = hashing.shard_for(SUM_COUNT_INSTANCES, req_id, word)
            entry = words_by_shard.setdefault(shard, {}).setdefault(word, [0, 0])
            entry[0] += en_count
            entry[1] += fr_count

    @staticmethod
    def _collect_word_count(req_id: int, url: str, amount: int, date_modified: str | None, join_batches: dict, amount_batch: list):
        if date_modified is None:
            logging.warning(f"The article {url} does not have a date_modified field. Skipping it...")
            return
 
        year = datetime.strptime(date_modified, _DATE_FORMAT).year
        if year >= _CUTOFF_YEAR:
            shard = hashing.shard_for(JOIN_AVERAGE_INSTANCES, req_id, url)
            join_batches.setdefault(shard, []).append([url, amount])
        else:
            amount_batch.append([amount])

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
 
        self.is_running = True

        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        self._state = storage.AtomicStore(READER_STATE_PATH)

        self._pending_eof: dict[int, str] = {}

        self.sum_count_queue_names = [f"{SUM_COUNT_QUEUE}_{i}" for i in range(SUM_COUNT_INSTANCES)]
        self.join_average_queue_names = [f"{JOIN_AVERAGE_QUEUE}_{i}" for i in range(JOIN_AVERAGE_INSTANCES)]
        self.sum_amount_queue_names = [f"{SUM_AMOUNT_QUEUE}_{i}" for i in range(SUM_AMOUNT_INSTANCES)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, 
            queue_name=f"{WORD_READER_QUEUE}_{ID}"
        )

        self.count_publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.sum_count_queue_names],
        )

        self.amount_publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.sum_amount_queue_names],
        )

        self.join_publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.join_average_queue_names],
        )

        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=WORD_READER_PREFIX,
            instances=WORD_READER_INSTANCES,
            on_all_recv=self._on_recv_all,
            get_tracker=self._get_tracker,
        )

        self.__post_init__()

    def __post_init__(self):
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
            tracker.record_sent(SUM_COUNT_QUEUE, rec.get("sent_delta_sum_count", 0))
            tracker.record_sent(SUM_AMOUNT_QUEUE, rec.get("sent_delta_sum_amount", 0))
            tracker.record_sent(JOIN_AVERAGE_QUEUE, rec.get("sent_delta_join", 0))

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
            batches = rec.get("batches")
            payload = rec.get("payload")
            if payload is None:
                logging.error(
                    "Cannot replay batch req_id=%s batch_id=%s — payload missing",
                    req_id, batch_id,
                )
                continue

            logging.info("Replaying PENDING batch req_id=%s batch_id=%s", req_id, batch_id)
            sent_count, sent_amount, sent_join = self._handle_articles(batch_id, req_id, payload)

            tracker = self._get_tracker(req_id)
            tracker.record_recv(batches)
            tracker.record_sent(SUM_COUNT_QUEUE, sent_count)
            tracker.record_sent(SUM_AMOUNT_QUEUE, sent_amount)
            tracker.record_sent(JOIN_AVERAGE_QUEUE, sent_join)

            self._state.put(msg_key, {
                "stage": COMPLETED_STAGE,
                "recv_delta": batches,
                "sent_delta_sum_count": sent_count,
                "sent_delta_sum_amount": sent_amount,
                "sent_delta_join": sent_join,
            })

        # Re-trigger any EOF whose coordinator call did not complete before crash.
        for eof_key in self._state.pending(EOF_PENDING_STAGE):
            try:
                _, req_id_str, _ = eof_key.split(":")
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed EOF_PENDING key: %s", eof_key)
                continue

            rec = self._state.get(eof_key)

            logging.info("Replaying EOF_PENDING req_id=%s", req_id)
            self._pending_eof[req_id] = eof_key
            self.coordinator.on_eof(req_id=req_id, expected_messages=rec.get("expected", 0))

    def _new_tracker(self):
        tracker = entity.message_tracker.MessageTracker()
        tracker.record_sent(SUM_COUNT_QUEUE, 0)
        tracker.record_sent(SUM_AMOUNT_QUEUE, 0)
        tracker.record_sent(JOIN_AVERAGE_QUEUE, 0)
        return tracker

    def _get_tracker(self, req_id: int):
        with self._trackers_lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = self._new_tracker()
            return tracker

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumer...")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping the consumer")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info("Waiting up to %s seconds for active threads to stop", self._TIMEOUT)

        timed_out = []
        components = (
            ("coordinator", self.coordinator.stop),
            ("count publisher", self.count_publisher.stop),
            ("amount publisher", self.amount_publisher.stop),
            ("join publisher", self.join_publisher.stop),
        )
        for name, stop in components:
            if stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append(name)

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("Error closing the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _handle_articles(self, batch_id: int, req_id: int, articles: list[list]):
        words_by_shard: dict[int, dict[str, list[int]]] = {}
        join_batches: dict[int, list] = {}
        amount_batch = []

        for article in articles:
            abstract, url, date_modified, in_language = article
            words = tokenize(abstract or "", in_language)
 
            self._collect_unique_words(req_id, words, in_language, words_by_shard)
            self._collect_word_count(req_id, url, len(words), date_modified, join_batches, amount_batch)
 
        sent_count = self._send_unique_words(batch_id, req_id, words_by_shard)
        sent_amount, sent_join = self._send_word_count(batch_id, req_id, join_batches, amount_batch)

        return sent_count, sent_amount, sent_join

    def _handle_data(self, ack: Callable, *args):
        batch_id, req_id, batches, messages = args
        msg_key = f"data:{req_id}:{batch_id}"

        stage = self._state.get(msg_key).get("stage")
        if stage is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "batches": batches, "payload": messages})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        sent_count, sent_amount, sent_join = self._handle_articles(batch_id, req_id, messages)
        fault_tolerance.crash(at="AFTER_DATA_READER")

        with self._trackers_lock:
            tracker = self._get_tracker(req_id)
            tracker.record_recv(batches)
            tracker.record_sent(SUM_COUNT_QUEUE, sent_count)
            tracker.record_sent(SUM_AMOUNT_QUEUE, sent_amount)
            tracker.record_sent(JOIN_AVERAGE_QUEUE, sent_join)
            
        self._state.put(msg_key, {
            "stage": COMPLETED_STAGE,
            "recv_delta": batches,
            "sent_delta_sum_count": sent_count,
            "sent_delta_sum_amount": sent_amount,
            "sent_delta_join": sent_join,
        })
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

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

        self.coordinator.on_eof(req_id, expected_messages)

    def _send_unique_words(self, batch_id: int, req_id: int, words_by_shard: dict):
        sent = 0
        for shard, shard_words in words_by_shard.items():
            message = [[word, en, fr] for word, (en, fr) in shard_words.items()]
            logging.debug(f"Send OPCODE_DATA message to {self.sum_count_queue_names[shard]} for req_id={req_id}")
            message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_count_{shard}".encode())
            self.count_publisher.publish_queue(
                queue=self.sum_count_queue_names[shard],
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    message_id=message_id,
                    req_id=req_id, 
                    batches=len(message), 
                    message=message,
                ),
            )
            sent += 1
        return sent

    def _send_word_count(self, batch_id: int, req_id: int, join_batches: dict, amount_batch: list):
        sent_amount = sent_join = 0
        for shard, batch in join_batches.items():
            logging.debug(f"Send OPCODE_DATA message to {self.join_average_queue_names[shard]} for req_id={req_id}")
            message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_join_{shard}".encode())
            self.join_publisher.publish_queue(
                queue=self.join_average_queue_names[shard],
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    message_id=message_id,
                    req_id=req_id, 
                    batches=len(batch), 
                    message=batch,
                ),
            )
            sent_join += 1
    
        if amount_batch:
            logging.debug(f"Send OPCODE_DATA message to {SUM_AMOUNT_QUEUE} for req_id={req_id}")
            message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_amount".encode())
            self.amount_publisher.publish_queue(
                queue=f"{SUM_AMOUNT_QUEUE}_{message_id % SUM_AMOUNT_INSTANCES}",
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    message_id=message_id,
                    req_id=req_id, batches=len(amount_batch), message=amount_batch,
                ),
            )
            sent_amount += 1
        return sent_amount, sent_join

    def _on_recv_all(self, req_id: int, sent_messages: list):
        logging.info(f"All articles have been received for req_id={req_id}; forwarding EOF...")

        eof_key  = self._pending_eof[req_id]
        batch_id = self._state.get(eof_key)["batch_id"]

        for key, sent in sent_messages:
            if key == SUM_COUNT_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {self.sum_count_queue_names[0]} for req_id={req_id}")
                message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_{key}".encode())
                self.count_publisher.publish_queue(
                    queue=self.sum_count_queue_names[0],
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        message_id=message_id,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            elif key == JOIN_AVERAGE_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {self.join_average_queue_names[0]} for req_id={req_id}")
                message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_{key}".encode())
                self.join_publisher.publish_queue(
                    queue=self.join_average_queue_names[0],
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        message_id=message_id,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            elif key == SUM_AMOUNT_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {SUM_AMOUNT_QUEUE} for req_id={req_id}")
                message_id = zlib.crc32(f"{WORD_READER_PREFIX}_{ID}_{batch_id}_{key}".encode())
                self.amount_publisher.publish_queue(
                    queue=f"{SUM_AMOUNT_QUEUE}_{message_id % SUM_AMOUNT_INSTANCES}",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        message_id=message_id,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            else:
                raise ValueError(f"Unknown key; received: {key}")

            fault_tolerance.crash(at="AFTER_EOF_COMPLETED")
            self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
            fault_tolerance.crash(at="BEFORE_EOF_COMPLETED")

            self._pending_eof.pop(req_id, None)
            with self._trackers_lock:
                self._trackers.pop(req_id, None)
            
    def process_message(self, message, ack, nack):
        try:
            opcode, *args = message_protocol.internal.deserialize(message)

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
            self.coordinator.start()
            self.input_queue.start_consuming(self.process_message)
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
        WordReader().start()
    except Exception:
        logging.exception("Error initializing or running WordReader")
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())