import itertools
import os
import time
import signal
import logging
import threading
from typing import Callable

from common import entity, fault_tolerance, middleware, message_protocol, rabbitmq, storage

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
SUM_AMOUNT_INSTANCES = int(os.environ.get("SUM_AMOUNT_INSTANCES", 1))
SUM_AMOUNT_PREFIX = os.environ.get("SUM_AMOUNT_PREFIX", "sum_amount")
SUM_AMOUNT_QUEUE = os.environ.get("SUM_AMOUNT_QUEUE", "sum_amount_queue")
AVERAGE_INSTANCES = int(os.environ.get("AVERAGE_INSTANCES", 1))
AVERAGE_QUEUE = os.environ.get("AVERAGE_QUEUE", "average_queue")

AMOUNT_STATE_PATH = os.environ.get("AMOUNT_STATE_PATH", f"tmp/state.json")

PENDING_STAGE = "PENDING"
COMPLETED_STAGE = "COMPLETED"
EOF_PENDING_STAGE = "EOF_PENDING"
EOF_COMPLETED_STAGE = "EOF_COMPLETED"

class SumAmount:
    _TIMEOUT = 5.0
    
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.partials: dict[int, list[int]] = {}
        self.state_lock = threading.Lock()
        self.average_queue_names = [f"{AVERAGE_QUEUE}_{i}" for i in range(AVERAGE_INSTANCES)]

        self._trackers: dict[int, entity.MessageTracker] = {}
        self._trackers_lock = threading.RLock()

        self._state = storage.AtomicStore(AMOUNT_STATE_PATH)

        self._pending_eof: dict[int, str] = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{SUM_AMOUNT_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=self.average_queue_names
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=SUM_AMOUNT_PREFIX,
            instances=SUM_AMOUNT_INSTANCES,
            on_all_recv=self._on_all_recv,
            on_commit=self._flush,
            get_tracker=self._get_tracker,
        )

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
            self.partials[req_id] = [rec.get("amount_delta", 0), rec.get("recv_delta", 0)]

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
            partial = self._accumulate(req_id, payload)
            tracker = self._get_tracker(req_id)
            tracker.record_recv(batches)

            self._state.put(msg_key, {
                "stage": COMPLETED_STAGE,
                "recv_delta": batches,
                "amount_delta": partial,
            })
    
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
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            logging.warning("An error occurred while trying to stop the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            logging.warning("An error occurred while trying to stop the publisher")

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while trying to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _accumulate(self, req_id: int, entries: list[list[int]]):
        partial = 0
        with self.state_lock:
            acc = self.partials.setdefault(req_id, [0, 0])
            for (words,) in entries:
                partial += words
                acc[1] += 1
            acc[0] += partial
        return partial

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
        partial = self._accumulate(req_id, messages)
        fault_tolerance.crash(at="AFTER_DATA_READER")

        with self._trackers_lock:
            tracker = self._get_tracker(req_id)
            tracker.record_recv(1)

        self._state.put(msg_key, {
            "stage": COMPLETED_STAGE,
            "amount_delta": partial, 
            "recv_delta": 1,
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

    def _new_tracker(self):
        tracker = entity.message_tracker.MessageTracker()
        return tracker

    def _get_tracker(self, req_id: int):
        with self._trackers_lock:
            tracker = self._trackers.get(req_id)
            if tracker is None:
                tracker = self._trackers[req_id] = self._new_tracker()
            return tracker

    def _on_all_recv(self, req_id: int, _sent_by_key=None):
        eof_key  = self._pending_eof[req_id]
        fault_tolerance.crash(at="AFTER_EOF_COMPLETED")
        self._state.put(eof_key, {"stage": EOF_COMPLETED_STAGE})
        fault_tolerance.crash(at="BEFORE_EOF_COMPLETED")

        self._pending_eof.pop(req_id, None)
        with self._trackers_lock:
            self._trackers.pop(req_id, None)
            
    def _flush(self, req_id: int):
        with self.state_lock:
            words, articles = self.partials.pop(req_id, [0, 0])

        logging.info(f"{words} words from {articles} articles are being submitted to calculate the average for req_id={req_id}")

        average_queue_name = self.average_queue_names[req_id % AVERAGE_INSTANCES]
        logging.debug(f"Sending OPCODE_DATA message to {average_queue_name} for req_id={req_id}")
        
        self.publisher.publish_queue(
            queue=average_queue_name,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                message_id=f"{SUM_AMOUNT_PREFIX}_{ID}",
                req_id=req_id,
                batches=1,
                message=[ID, words, articles],
            ),
        )
        fault_tolerance.crash(at="AFTER_COMMIT_SENT")

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
            logging.info(f"Waiting to receive messages...")
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
        SumAmount().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())