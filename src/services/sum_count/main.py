import os
import time
import heapq
import signal
import logging
import threading
import zlib

from common import entity, middleware, message_protocol, rabbitmq, storage, fault_tolerance

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
SUM_COUNT_STATE_PATH = os.environ.get("SUM_COUNT_STATE_PATH", f"tmp/{SUM_COUNT_PREFIX}/{ID}/state.json")


class SumCount:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_3"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True

        self.words: dict[int, dict[str, list[int]]] = {}
        self.recv: dict[int, int] = {}
        self._finished: set[int] = set()
        self._lock = threading.RLock()

        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        self._store = storage.AtomicStoreReader(SUM_COUNT_STATE_PATH)

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
            get_tracker=self._get_tracker,
            on_commit=self._on_commit,
        )

        self.__post_init__()

    def __post_init__(self):
        for req_id, snapshot in self._store.snapshots().items():
            self.words[req_id] = snapshot.get("words", {})
            self.recv[req_id] = snapshot.get("recv", 0)
            self._get_tracker(req_id).record_recv(self.recv[req_id])

        for req_id, producer, message_id, _, entries in self._store.pending():
            logging.info("Replaying PENDING message req_id=%s message_id=%s", req_id, message_id)
            self._apply_data(req_id, producer, message_id, entries)

        # Re-trigger any EOF whose coordinator call did not complete before crash.
        for req_id, _, expected in self._store.pending_eofs():
            logging.info("Replaying EOF_PENDING req_id=%s", req_id)
            self._apply_eof(req_id, expected)

    def _new_tracker(self):
        return entity.message_tracker.MessageTracker()

    def _get_tracker(self, req_id: int):
        with self._trackers_lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = self._new_tracker()
            return tracker

    def _snapshot(self, req_id: int) -> dict:
        return {"recv": self.recv.get(req_id, 0), "words": self.words.get(req_id, {})}

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

    def _has_pending_eof(self, req_id: int) -> bool:
        producers = self._store.get(req_id).get("producers", {})
        return any(p.startswith("eof:") for p in producers)
    
    def _cleanup(self, req_id: int):
        self._store.delete(req_id)
        with self._lock:
            self.words.pop(req_id, None)
            self.recv.pop(req_id, None)
        with self._trackers_lock:
            self._trackers.pop(req_id, None)

    def _on_recv_all(self, req_id: int, _: list):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                message_id=f"{SUM_COUNT_PREFIX}_{ID}_{req_id}_eof",
                req_id=req_id,
            ),
        )

        # recién ahora el request terminó para el coordinador: se borra también el EOF PENDING
        fault_tolerance.crash(at="BEFORE_EOF_CLEANUP")
        self._cleanup(req_id)
        fault_tolerance.crash(at="AFTER_EOF_CLEANUP")

    def _on_commit(self, req_id: int):
        with self._lock:
            request_words = dict(self.words.get(req_id, {}))

        self._send_common_words(req_id, request_words)
        self._send_partial_top(req_id, request_words)
        fault_tolerance.crash(at="AFTER_COMMIT_SENT")

        # La instancia con el EOF PENDING conserva el estado hasta _on_recv_all:
        # si crashea, el replay del EOF retoma el COMMIT y reenvía con los mismos ids.
        if not self._has_pending_eof(req_id):
            self._cleanup(req_id)

    def _send_common_words(self, req_id: int, request_words: dict):
        chunk = []
        sent = 0
        n_chunks = 0

        for word, (en_count, fr_count) in sorted(request_words.items()):
            if en_count > 0 and fr_count > 0:
                chunk.append(word)
                if len(chunk) == CHUNK_SIZE:
                    self._send_chunk(req_id, n_chunks, chunk)
                    n_chunks += 1
                    sent += len(chunk)
                    chunk = []

        if chunk:
            self._send_chunk(req_id, n_chunks, chunk)
            sent += len(chunk)

        logging.info(f"{sent} common words were sent out of a total of {len(request_words)} words for req_id={req_id}")

    def _send_chunk(self, req_id: int, n: int, chunk: list[str]):
        logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                message_id=zlib.crc32(f"{SUM_COUNT_PREFIX}_{ID}_{req_id}_chunk_{n}".encode()),
                req_id=req_id,
                batches=len(chunk),
                message=chunk,
            ),
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
                message_id=f"{SUM_COUNT_PREFIX}_{ID}:_{req_id}_top",
                req_id=req_id,
                batches=len(entries),
                message=[ID, entries],
            ),
        )

    def _accumulate(self, req_id: int, producer: str, message_id: str, entries: list[list]):
        logging.info("Accumulate data for req_id=%s message_id=%s", req_id, message_id)
        #with self._lock:
        request_words = self.words.setdefault(req_id, {})
        for word, en_count, fr_count in entries:
            counts = request_words.setdefault(word, [0, 0])
            counts[0] += en_count
            counts[1] += fr_count
        self.recv[req_id] = self.recv.get(req_id, 0) + 1

        self._store.apply(req_id, producer, message_id, self._snapshot(req_id))

        logging.info("Accumulate data ENDED for req_id=%s message_id=%s", req_id, message_id)
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")
        with self._trackers_lock:
            self._get_tracker(req_id).record_recv(1)

    def _apply_eof(self, req_id: int, expected):
        self.coordinator.on_eof(req_id, expected)
        fault_tolerance.crash(at="AFTER_EOF_FORWARDED")

    def _handle_data(self, ack, *args):
        logging.info("Recibido por aca")
        message_id, req_id, batches, messages = args
        producer = message_id.split(":")[0]

        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        if not self._store.begin(req_id, producer, message_id, 1, messages):
            logging.warning(f"Duplicate redelivery for req_id={req_id} message_id={message_id}; discarding")
            ack()
            return
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")

        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        self._accumulate(req_id, producer, message_id, messages)

    def _handle_eof(self, ack, *args):
        message_id, req_id, _, expected = args
        fault_tolerance.crash(at="BEFORE_SAVE_EOF")
        if not self._store.begin_eof(req_id, message_id, expected):
            logging.warning(f"Duplicate EOF redelivery for req_id={req_id}; discarding")
            ack()
            return
        fault_tolerance.crash(at="AFTER_SAVE_EOF")

        ack()
        fault_tolerance.crash(at="AFTER_EOF_ACK")

        self._apply_eof(req_id, expected)

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
        SumCount().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())
    