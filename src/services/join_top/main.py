import os
import time
import zlib
import signal
import logging
import threading

from collections.abc import Callable
from common import fault_tolerance, middleware, message_protocol, rabbitmq, storage

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
SUM_COUNT_INSTANCES = int(os.environ.get("SUM_COUNT_INSTANCES", 1))
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))
JOIN_TOP_QUEUE_PREFIX = os.environ.get("JOIN_TOP_QUEUE_PREFIX", "join_top_queue")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
JOIN_TOP_STATE_PATH = os.environ.get("JOIN_TOP_STATE_PATH", "tmp/state.json")

# Single store for per-message idempotency and crash recovery.  Keys:
#   "batch:{req_id}:{batch_id}" → DATA batches received but not yet aggregated
#       PENDING     : {"stage": PENDING, "payload": [sum_id, [[word, count], ...]]}
#   "agg:{req_id}"              → running aggregate of a request
#       AGGREGATING : {"stage": AGGREGATING, "recv": [sum_id, ...], "counts": {word: count}}
PENDING_STAGE = "PENDING"
AGGREGATE_STAGE = "AGGREGATING"


class JoinTop:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_4"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self._is_running = True
        self._state = storage.AtomicStore(JOIN_TOP_STATE_PATH)

        # req_id -> (sum_ids recibidos, {word: count})
        self._requests: dict[int, tuple[set[int], list[list]]] = {}
        self._requests_lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{JOIN_TOP_QUEUE_PREFIX}_{ID}",
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=[self._GATEWAY_QUEUE]
        )

        self.__post_init__()

    def __post_init__(self):
        for key in self._state.pending(AGGREGATE_STAGE):
            try:
                req_id = int(key.split(":")[1])
                rec = self._state.get(key)
                self._requests[req_id] = (set(rec["recv"]), [list(wc) for wc in rec["top"]])
            except (ValueError, IndexError, KeyError, TypeError):
                logging.warning("Skipping malformed aggregate key: %s", key)

        for key in self._state.pending(PENDING_STAGE):
            try:
                _, req_id_str, _ = key.split(":")
                req_id = int(req_id_str)
            except ValueError:
                logging.warning("Skipping malformed PENDING key: %s", key)
                continue

            payload = (self._state.get(key) or {}).get("payload")
            if payload is None:
                logging.error("Cannot replay %s: payload missing", key)
                continue

            logging.info("Replaying PENDING batch %s", key)
            self._add_partial(req_id, payload)  # idempotent thanks to "recv"
            self._state.delete(key)

        with self._requests_lock:
            complete = [
                req_id for req_id, (recv, _) in self._requests.items()
                if len(recv) == SUM_COUNT_INSTANCES
            ]
        for req_id in complete:
            self._finish_request(req_id)

    def _finish_request(self, req_id: int):
        self._send_top(req_id)
        self._state.delete(f"agg:{req_id}")
        with self._requests_lock:
            self._requests.pop(req_id, None)

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, proceed to shutdown the service...")
        self._is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to finish")

        remaining = max(0.0, deadline - time.monotonic())
        if remaining > 0:
            if self.publisher.stop(timeout=remaining):
                logging.warning("Shutdown timed out with still active threads")
        else:
            logging.warning("Shutdown timed out with still active threads")

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close a queue")

        logging.info("All resources have been released; goodbye")

    def _merge_top(self, current: list, incoming: list) -> list:
        merged = [*current, *incoming]
        merged.sort(key=lambda wc: (-wc[1], wc[0]))
        return merged[:TOP_SIZE]

    def _add_partial(self, req_id: int, messages: list) -> int:
        logging.debug(f"Received an OPCODE_DATA message from req_id={req_id}")

        sum_id, top = messages
        with self._requests_lock:
            recv, current = self._requests.setdefault(req_id, (set(), []))

            if sum_id in recv:
                logging.warning(f"Duplicated partial from sum_{sum_id} for req_id={req_id}")
                return len(recv)

            new_recv = recv | {sum_id}
            new_top = self._merge_top(current, top)

            self._state.put(f"agg:{req_id}", {
                "stage": AGGREGATE_STAGE,
                "recv": sorted(new_recv),
                "top": new_top,
            })
            self._requests[req_id] = (new_recv, new_top)
            return len(new_recv)

    def _send_top(self, req_id: int):
        with self._requests_lock:
            _, top = self._requests[req_id]
            top = list(top)

        logging.info(f"Final top for req_id={req_id}: {top}")

        message = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_DATA,
            message_id=zlib.crc32(f"{JOIN_TOP_QUEUE_PREFIX}_{ID}_{req_id}".encode()),
            req_id=req_id,
            batches=len(top),
            message=[[w, c] for w, c in top],
        )
        eof = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_EOF,
            message_id=zlib.crc32(f"{JOIN_TOP_QUEUE_PREFIX}_{ID}_{req_id}_eof".encode()),
            req_id=req_id,
            batches=1,
            message=0,
        )
        self.publisher.publish_queue(queue=self._GATEWAY_QUEUE, message=message)
        self.publisher.publish_queue(queue=self._GATEWAY_QUEUE, message=eof)

    def _handle_data(self, ack: Callable, *args):
        batch_id, req_id, batches, messages = args
        msg_key = f"batch:{req_id}:{batch_id}"

        if self._state.get(msg_key).get("stage") is not None:  # fix: era .get(stage)
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return
        
        fault_tolerance.crash(at="BEFORE_SAVE_PENDING")
        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": messages})
        fault_tolerance.crash(at="AFTER_SAVE_PENDING")
        ack()
        fault_tolerance.crash(at="AFTER_DATA_ACK")

        recv = self._add_partial(req_id, messages)
        fault_tolerance.crash(at="AFTER_DATA_PROCESSED")
        self._state.delete(msg_key)
        fault_tolerance.crash(at="AFTER_DATA_COMPLETED")

        if recv == SUM_COUNT_INSTANCES:
            self._finish_request(req_id)

    def _process_message(self, message, ack, nack):
        try:
            opcode, *args = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._handle_data(ack, *args)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        try:
            logging.info(f"Waiting to receive messages...")
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
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    logging.getLogger("pika").propagate = False
    try:
        JoinTop().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())