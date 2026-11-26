import os
import time
import heapq
import signal
import logging
import threading

from collections.abc import Callable
from common import entity, fault_tolerance, middleware, message_protocol, rabbitmq, storage

ID = int(os.environ["ID"])
COORDINATOR_ID = int(os.environ["COORDINATOR_ID"])
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
SUM_COUNT_INSTANCES = int(os.environ.get("SUM_COUNT_INSTANCES", 1))
SUM_COUNT_PREFIX = os.environ.get("SUM_COUNT_PREFIX", "sum_count")
SUM_COUNT_QUEUE = os.environ.get("SUM_COUNT_QUEUE", "sum_count_queue")
JOIN_TOP_INSTANCES = int(os.environ.get("JOIN_TOP_INSTANCES", 1))
JOIN_TOP_QUEUE = os.environ.get("JOIN_TOP_QUEUE", "join_top_queue")

# Same store as the Filter, two files:
#   STATE : per-message idempotency (small, written twice per message)
#       "data:{req_id}:{message_id}" -> PENDING {"payload": entries} | COMPLETED {}
#       "eof:{req_id}:{message_id}"  -> EOF_PENDING {"expected", "batch_id"} | EOF_COMPLETED {}
#   WORDS : the state of each request (big, written once per message)
#       "words:{req_id}" -> {"stage": WORDS, "words": {word: [en, fr]}, "recv": N, "last_applied": message_id}
# Keeping the words apart means a PENDING/COMPLETED write does not rewrite them.
SUM_COUNT_STATE_PATH = os.environ.get("SUM_COUNT_STATE_PATH", f"tmp/{SUM_COUNT_PREFIX}/{ID}/state.json")
SUM_COUNT_WORDS_PATH = os.environ.get("SUM_COUNT_WORDS_PATH", f"tmp/{SUM_COUNT_PREFIX}/{ID}/words.json")
COORDINATOR_STATE_PATH = os.environ.get("COORDINATOR_STATE_PATH", f"tmp/{SUM_COUNT_PREFIX}/{ID}/eof_state.json")

PENDING_STAGE = "PENDING"
COMPLETED_STAGE = "COMPLETED"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_COMPLETED_STAGE = "EOF_COMPLETED"
WORDS_STAGE = "WORDS"

class SumCount:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_3"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True

        # {req_id: {word: [en_count, fr_count]}}, rebuilt from _words_state on startup.
        self.words: dict[int, dict[str, list[int]]] = {}
        self._received: dict[int, int] = {}      # data messages applied per request
        self._last_applied: dict[int, str] = {}  # last message folded into words[req_id]

        # The consumer thread mutates `words` and the coordinator thread writes the store,
        # and a store write dumps every live dict: both go through this lock.
        self._words_lock = threading.RLock()

        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        self._state = storage.AtomicStore(SUM_COUNT_STATE_PATH)
        self._words_state = storage.AtomicStore(SUM_COUNT_WORDS_PATH)

        # Maps req_id -> eof_key. Written by the consumer thread before coordinator.on_eof();
        # read by the coordinator thread in _on_recv_all.
        self._pending_eof: dict[int, str] = {}

        self.join_top_queue_names = [f"{JOIN_TOP_QUEUE}_{i}" for i in range(JOIN_TOP_INSTANCES)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{SUM_COUNT_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE, *self.join_top_queue_names],
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=SUM_COUNT_PREFIX,
            instances=SUM_COUNT_INSTANCES,
            on_all_recv=self._on_recv_all,
            on_commit=self._on_commit,
            get_tracker=self._get_tracker,
            state_path=COORDINATOR_STATE_PATH,
        )

        self.__post_init__()

    def __post_init__(self):
        # Words and message count come straight from the persisted state of each request.
        for key in self._words_state.pending(WORDS_STAGE):
            req_id = int(key.split(":", 1)[1])
            rec = self._words_state.get(key)

            self.words[req_id] = rec["words"]
            self._received[req_id] = rec["recv"]
            self._last_applied[req_id] = rec["last_applied"]
            self._get_tracker(req_id).record_recv(rec["recv"])

        # Replay any DATA batch that was ACKed but not yet COMPLETED.
        for msg_key in self._state.pending(PENDING_STAGE):
            try:
                _, req_id_str, message_id = msg_key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed PENDING key: %s", msg_key)
                continue

            entries = self._state.get(msg_key).get("payload")
            if entries is None:
                logging.error("Cannot replay message %s req_id=%s — payload missing", message_id, req_id)
                continue

            logging.info("Replaying PENDING message %s req_id=%s", message_id, req_id)
            if self._last_applied.get(req_id) == message_id:
                # The words were saved but the crash came before COMPLETED: do not add it twice.
                logging.info("Message %s was already folded into the words; marking it COMPLETED", message_id)
            else:
                self._apply(req_id, message_id, entries)

            self._state.put(msg_key, {"stage": COMPLETED_STAGE})

        # Re-trigger any EOF whose coordinator call did not complete before crash.
        for eof_key in self._state.pending(EOF_PENDING_STAGE):
            try:
                _, req_id_str, _ = eof_key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed EOF_PENDING key: %s", eof_key)
                continue

            rec = self._state.get(eof_key)
            logging.info("Replaying EOF_PENDING req_id=%s with expected_messages=%s", req_id, rec.get("expected", 0))
            self._pending_eof[req_id] = eof_key
            self.coordinator.on_eof(req_id=req_id, expected_messages=rec.get("expected", 0))

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
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info("Waiting up to %s seconds for active threads to stop", self._TIMEOUT)

        timed_out = []
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            logging.warning("An error occurred while attempting to stop the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            logging.warning("An error occurred while attempting to stop the publisher")

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    # Output ids come from the content (never from an in-memory counter), so a replay
    # after a crash sends exactly the same ids and the receivers can drop the repeats.
    def _message_id(self, req_id: int, suffix: str) -> str:
        return f"{SUM_COUNT_PREFIX}_{ID}:{req_id}_{suffix}"

    def _on_recv_all(self, req_id: int, _: list):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        eof_key = self._pending_eof[req_id]
        batch_id = self._state.get(eof_key)["batch_id"]

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=self._message_id(req_id, f"{batch_id}_eof"),
                req_id=req_id
            )
        )

        fault_tolerance.crash(at="BEFORE_EOF_COMPLETED")
        self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
        fault_tolerance.crash(at="AFTER_EOF_COMPLETED")

        self._pending_eof.pop(req_id, None)

    def _on_commit(self, req_id: int):
        with self._words_lock:
            request_words = self.words.pop(req_id, {})
            self._received.pop(req_id, None)
            self._last_applied.pop(req_id, None)

        # send common words in English and French to the Gateway
        self._send_common_words(req_id, request_words)

        # send the partial top of words to the Top Process
        self._send_partial_top(req_id, request_words)
        fault_tolerance.crash(at="AFTER_COMMIT_SENT")

        # Everything was sent: the persisted state of the request is no longer needed.
        with self._words_lock:
            self._words_state.delete(f"words:{req_id}")
        self._state.delete_prefix(f"data:{req_id}:")
        with self._trackers_lock:
            self._trackers.pop(req_id, None)

    def _send_common_words(self, req_id: int, request_words: dict):
        chunk = []
        sent = index = 0

        # sorted: chunk `index` holds the same words after a replay
        for word, (en_count, fr_count) in sorted(request_words.items()):
            if en_count > 0 and fr_count > 0:
                chunk.append(word)

                if len(chunk) == CHUNK_SIZE:
                    self._send_chunk(req_id, index, chunk)
                    sent += len(chunk)
                    index += 1
                    chunk = []

        if chunk:
            self._send_chunk(req_id, index, chunk)
            sent += len(chunk)

        logging.info(f"{sent} common words were sent out of a total of {len(request_words)} words for req_id={req_id}")

    def _send_chunk(self, req_id: int, index: int, chunk: list[str]):
        logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                message_id=self._message_id(req_id, f"common_{index}"),
                req_id=req_id,
                batches=len(chunk),
                message=chunk,
            )
        )

    def _send_partial_top(self, req_id: int, request_words: dict):
        top = heapq.nsmallest(
            TOP_SIZE,
            ((-(en + fr), word) for word, (en, fr) in request_words.items()),
        )
        entries = [[word, -neg_total] for neg_total, word in top]

        top_process_queue_name = self.join_top_queue_names[req_id % JOIN_TOP_INSTANCES]
        logging.debug(f"Sending OPCODE_DATA message to {top_process_queue_name} for req_id={req_id}")

        self.publisher.publish_queue(
            queue=top_process_queue_name,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                message_id=self._message_id(req_id, "top"),
                req_id=req_id,
                batches=len(entries),
                message=[ID, entries],
            ),
        )

    def _accumulate(self, req_id: int, entries: list[list]):
        request_words = self.words.setdefault(req_id, {})

        for word, en_count, fr_count in entries:
            counts = request_words.setdefault(word, [0, 0])
            counts[0] += en_count
            counts[1] += fr_count

    def _apply(self, req_id: int, message_id, entries: list[list]):
        """Fold one message into the words and persist the new state in a single write."""
        with self._words_lock:
            self._accumulate(req_id, entries)
            self._received[req_id] = self._received.get(req_id, 0) + 1
            self._last_applied[req_id] = str(message_id)

            fault_tolerance.crash(at="BEFORE_SAVE_WORDS")
            self._words_state.put(f"words:{req_id}", {
                "stage": WORDS_STAGE,
                "words": self.words[req_id],
                "recv": self._received[req_id],
                "last_applied": str(message_id),
            })
            fault_tolerance.crash(at="AFTER_SAVE_WORDS")

        with self._trackers_lock:
            self._get_tracker(req_id).record_recv(1)

    def _handle_data(self, ack: Callable, message_id, req_id: int, entries: list[list]):
        msg_key = f"data:{req_id}:{message_id}"

        stage = self._state.get(msg_key).get("stage")
        if stage is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        # Persist payload + mark PENDING in one atomic write before ACK.
        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": entries})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        self._apply(req_id, message_id, entries)
        fault_tolerance.crash(at="AFTER_DATA_PROCESSED")

        self._state.put(msg_key, {"stage": COMPLETED_STAGE})
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

    def _handle_eof(self, ack: Callable, message_id, req_id: int, expected_messages: int):
        eof_key = f"eof:{req_id}:{message_id}"

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
            "batch_id": message_id,
        })
        fault_tolerance.crash(at="AFTER_SAVE_EOF")

        self._pending_eof[req_id] = eof_key

        ack()
        fault_tolerance.crash(at="AFTER_EOF_ACK")

        self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)

    def process_message(self, message, ack, nack):
        try:
            opcode, message_id, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received OPCODE_DATA message for req_id={req_id}")
                self._handle_data(ack, message_id, req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received OPCODE_EOF message for req_id={req_id}")
                self._handle_eof(ack, message_id, req_id, payload)
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
        SumCount().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())