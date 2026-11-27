import os
import time
import zlib
import signal
import logging
import threading
from articles_buffer import ArticleBuffer
from collections.abc import Callable
from common import entity, message_protocol, middleware, rabbitmq, storage, fault_tolerance

ID = int(os.environ["ID"])
COORDINATOR_ID = int(os.environ.get("COORDINATOR_ID", 0))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
JOIN_AVERAGE_INSTANCES = int(os.environ.get("JOIN_AVERAGE_INSTANCES", 1))
JOIN_AVERAGE_PREFIX = os.environ.get("JOIN_AVERAGE_PREFIX", "join_average")
JOIN_AVERAGE_QUEUE = os.environ.get("JOIN_AVERAGE_QUEUE", "join_average_queue")
EOF_SOURCES = 2

# Single store for per-message idempotency and crash recovery.  Keys:
#   "data:{req_id}:{batch_id}"  → DATA messages (articles or the average)
#       PENDING   : {"stage": PENDING,   "payload": [[url, amount], ...] | average}
#       BUFFERED  : {"stage": BUFFERED,  "segment": n, "recv_delta": 1}
#                   (articles received, waiting for the average; the payload is in the buffer, see below)
#       COMPLETED : {"stage": COMPLETED, "recv_delta": 1}
#   "avg:{req_id}"              → average received for the request
#       AVERAGE   : {"stage": AVERAGE, "average": float}
#   "eof:{req_id}:{eof_id}"     → each EOF message (coordinator only)
#       EOF_PENDING  : {"stage": EOF_PENDING, "expected": N}
#       EOF_ABSORBED : {"stage": EOF_ABSORBED}
#   "eofagg:{req_id}"           → EOFs aggregated (coordinator only)
#       EOF_AGGREGATING : {"stage": ..., "recv": [eof_id, ...], "expected": N}
#       EOF_READY       : EOFs from both sides received; coordinator.on_eof must be (re)triggered
#       EOF_DONE        : final EOF already sent to the gateway
JOIN_AVERAGE_STATE_PATH = os.environ.get("JOIN_AVERAGE_STATE_PATH", "tmp/state.json")

# The articles waiting for the average do NOT live in the store above (every write there would rewrite
# all of them). They go to a buffer of segment files, "{req_id}_{n}.json", of at most BUFFER_SEGMENT_SIZE
# batches each: a write rewrites only the segment being filled and sealed segments are never touched again.
#       "{msg_key}" -> {"stage": BUFFERED, "payload": [[url, amount], ...]}
JOIN_AVERAGE_BUFFER_DIR = os.environ.get("JOIN_AVERAGE_BUFFER_DIR", "tmp/buffer")
BUFFER_SEGMENT_SIZE = int(os.environ.get("BUFFER_SEGMENT_SIZE", 20))

PENDING_STAGE = "PENDING"
BUFFERED_STAGE = "BUFFERED"
COMPLETED_STAGE = "COMPLETED"
AVERAGE_STAGE = "AVERAGE"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_ABSORBED_STAGE = "EOF_ABSORBED"
EOF_AGGREGATING_STAGE = "EOF_AGGREGATING"
EOF_READY_STAGE = "EOF_READY"
EOF_DONE_STAGE = "EOF_DONE"

class JoinAverage:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_5"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self._state = storage.AtomicStore(JOIN_AVERAGE_STATE_PATH)

        self._lock = threading.RLock()
        self._averages: dict[int, float] = {}
        self._buffer = ArticleBuffer(JOIN_AVERAGE_BUFFER_DIR, BUFFER_SEGMENT_SIZE)
        self._trackers: dict[int, entity.MessageTracker] = {}

        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=[self._GATEWAY_QUEUE])
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=JOIN_AVERAGE_PREFIX,
            instances=JOIN_AVERAGE_INSTANCES,
            on_all_recv=self._on_recv_all,
            on_commit=self._on_commit,
            get_tracker=self._get_tracker,
            state_path="tmp/eof/state.json",
        )
        self.coordinator.start()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_AVERAGE_QUEUE}_{ID}"
        )

        self.__post_init__()

    def __post_init__(self):
        for key in self._state.pending(AVERAGE_STAGE):
            try:
                req_id = int(key.split(":")[1])
                self._averages[req_id] = self._state.get(key)["average"]
            except (ValueError, IndexError, KeyError):
                logging.warning("Skipping malformed AVERAGE key: %s", key)

        self._buffer.recover()

        for stage in (COMPLETED_STAGE, BUFFERED_STAGE):
            for key in self._state.pending(stage):
                try:
                    req_id = int(key.split(":")[1])
                except (ValueError, IndexError):
                    logging.warning("Skipping malformed %s key: %s", stage, key)
                    continue
                self._get_tracker(req_id).record_recv(self._state.get(key).get("recv_delta", 0))

        for key in self._state.pending(PENDING_STAGE):
            try:
                _, req_id_str, batch_id = key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed PENDING key: %s", key)
                continue

            payload = self._state.get(key).get("payload")
            if payload is None:
                logging.error("Cannot replay %s: payload missing", key)
                continue

            logging.info("Replaying PENDING message %s", key)
            self._process_data(key, batch_id, req_id, payload)

        for req_id in self._buffer.requests():
            if req_id in self._averages:
                self._flush_buffered(req_id)

        for eof_key in self._state.pending(EOF_PENDING_STAGE):
            try:
                _, req_id_str, eof_id = eof_key.split(":", 2)
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed EOF_PENDING key: %s", eof_key)
                continue

            logging.info("Replaying EOF_PENDING %s", eof_key)
            self._absorb_eof(req_id, eof_id, self._state.get(eof_key).get("expected", 0))
            self._state.put(eof_key, {"stage": EOF_ABSORBED_STAGE})

        for agg_key in self._state.pending(EOF_READY_STAGE):
            try:
                req_id = int(agg_key.split(":")[1])
            except (ValueError, IndexError):
                logging.warning("Skipping malformed EOF_READY key: %s", agg_key)
                continue

            logging.info("Replaying EOF_READY req_id=%s", req_id)
            if self._trigger_eof(req_id):
                self._state.put(agg_key, {"stage": EOF_DONE_STAGE})

    def _get_tracker(self, req_id: int):
        with self._lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = entity.message_tracker.MessageTracker()
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
        failed = []
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            failed.append("coordinator")
            logging.warning("An error occurred while attempting to stop the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            failed.append("publisher")
            logging.warning("An error occurred while attempting to stop the publisher")

        try:
            self.input_queue.close()
        except Exception:
            failed.append("input queue")
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out or failed:
            if timed_out:
                logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
            if failed:
                logging.warning("Shutdown failed for components: %s", ", ".join(failed))
        else:
            logging.info("All resources have been released; goodbye")

    def _on_commit(self, req_id: int):
        with self._lock:
            self._averages.pop(req_id, None)
            self._buffer.forget(req_id)
            self._trackers.pop(req_id, None)
            self._state.delete(f"avg:{req_id}")
            self._state.delete_prefix(f"data:{req_id}:")
        logging.info(f"Finished request for req_id={req_id}")

    def _on_recv_all(self, req_id: int, _sent_messages=None):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        message_id = zlib.crc32(f"{JOIN_AVERAGE_PREFIX}_{ID}_{req_id}_{GATEWAY_PREFIX}_eof".encode())
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=message_id,
                req_id=req_id,
            ),
        )

        if ID == COORDINATOR_ID:
            fault_tolerance.crash(at="AFTER_EOF_SENT")
            self._state.put(f"eofagg:{req_id}", {"stage": EOF_DONE_STAGE})

    def _send_above_average(self, req_id: int, batch_id, articles: list, average: float):
        result = [[url, amount] for url, amount in articles if amount > average]
        for n, i in enumerate(range(0, len(result), CHUNK_SIZE)):
            chunk = result[i:i + CHUNK_SIZE]
            message_id = zlib.crc32(
                f"{JOIN_AVERAGE_PREFIX}_{ID}_{req_id}_{batch_id}_{n}_{GATEWAY_PREFIX}".encode()
            )

            logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=self._GATEWAY_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    message_id=message_id,
                    req_id=req_id,
                    batches=len(chunk),
                    message=chunk,
                ),
            )

    def _flush_buffered(self, req_id: int):
        with self._lock:
            average = self._averages[req_id]
            for segment in self._buffer.segments(req_id):
                batches = self._buffer.load(req_id, segment)
                batches = {k: v for k, v in batches.items() if self._state.get(k).get("stage") == BUFFERED_STAGE}

                for msg_key, articles in batches.items():
                    batch_id = msg_key.split(":", 2)[2]
                    self._send_above_average(req_id, batch_id, articles, average)
                fault_tolerance.crash(at="AFTER_BUFFERED_SENT")

                self._state.put_many({k: {"stage": COMPLETED_STAGE, "recv_delta": 1} for k in batches})
                self._buffer.release(req_id, segment)
            self._buffer.forget(req_id)

    def _process_articles(self, msg_key: str, batch_id, req_id: int, articles: list):
        with self._lock:
            average = self._averages.get(req_id)
            if average is None:
                segment = self._buffer.add(req_id, msg_key, articles)
                self._state.put(msg_key, {"stage": BUFFERED_STAGE, "segment": segment, "recv_delta": 1})
            else:
                self._send_above_average(req_id, batch_id, articles, average)
                fault_tolerance.crash(at="AFTER_DATA_PROCESSED")
                self._state.put(msg_key, {"stage": COMPLETED_STAGE, "recv_delta": 1})
            fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

            self._get_tracker(req_id).record_recv(1)

    def _process_average(self, msg_key: str, req_id: int, average: float):
        with self._lock:
            logging.debug(f"Received an average of {average} for req_id={req_id}")

            self._state.put(f"avg:{req_id}", {"stage": AVERAGE_STAGE, "average": average})
            fault_tolerance.crash(at="AFTER_AVERAGE_SAVED")
            self._averages[req_id] = average

            self._flush_buffered(req_id)

            self._state.put(msg_key, {"stage": COMPLETED_STAGE, "recv_delta": 1})
            fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

            self._get_tracker(req_id).record_recv(1)

    def _process_data(self, msg_key: str, batch_id, req_id: int, payload):
        if isinstance(payload, list):
            self._process_articles(msg_key, batch_id, req_id, payload)
        else:
            self._process_average(msg_key, req_id, payload)

    def _handle_data(self, ack: Callable, *args):
        batch_id, req_id, _, payload = args
        msg_key = f"data:{req_id}:{batch_id}"

        if self._state.get(msg_key).get("stage") is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": payload})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        self._process_data(msg_key, batch_id, req_id, payload)

    def _absorb_eof(self, req_id: int, eof_id, expected: int) -> bool:
        """Adds an EOF to the persisted aggregate. Returns True if this EOF made it complete."""
        agg_key = f"eofagg:{req_id}"
        rec = self._state.get(agg_key)
        if rec.get("stage") in (EOF_READY_STAGE, EOF_DONE_STAGE):
            return False

        recv = set(rec.get("recv", []))
        if str(eof_id) in recv:
            return False

        recv.add(str(eof_id))
        total = rec.get("expected", 0) + expected
        ready = len(recv) >= EOF_SOURCES

        logging.debug(f"Received OPCODE_EOF for req_id={req_id}: {len(recv)}/{EOF_SOURCES}; expected so far: {total}")
        self._state.put(agg_key, {
            "stage": EOF_READY_STAGE if ready else EOF_AGGREGATING_STAGE,
            "recv": sorted(recv),
            "expected": total,
        })
        return ready

    def _trigger_eof(self, req_id: int):
        rec = self._state.get(f"eofagg:{req_id}")
        logging.info(f"All joins finished for req_id={req_id}, notifying the coordinator")
        return self.coordinator.on_eof(
            req_id=req_id,
            expected_messages=rec["expected"] + JOIN_AVERAGE_INSTANCES,
        )

    def _handle_eof(self, ack: Callable, *args):
        eof_id, req_id, _, expected_messages = args

        if ID != COORDINATOR_ID:
            logging.warning(f"EOF was received by a non-coordinator join_{ID}")
            ack()
            return

        eof_key = f"eof:{req_id}:{eof_id}"
        if self._state.get(eof_key).get("stage") is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        fault_tolerance.crash(at="BEFORE_SAVE_EOF")
        self._state.put(eof_key, {"stage": EOF_PENDING_STAGE, "expected": expected_messages})
        fault_tolerance.crash(at="AFTER_SAVE_EOF")

        ack()
        fault_tolerance.crash(at="AFTER_EOF_ACK")

        ready = self._absorb_eof(req_id, eof_id, expected_messages)
        self._state.put(eof_key, {"stage": EOF_ABSORBED_STAGE})
        fault_tolerance.crash(at="AFTER_EOF_ABSORBED")

        if ready:
            self._trigger_eof(req_id)

    def _process_message(self, message, ack, nack):
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
        JoinAverage().start()
    except Exception:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())