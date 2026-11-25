import os
import time
import zlib
import signal
import logging
import threading

from collections.abc import Callable
from common import middleware, message_protocol, rabbitmq, storage, fault_tolerance

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
SUM_AMOUNT_INSTANCES = int(os.environ.get("SUM_AMOUNT_INSTANCES", 1))
JOIN_AVERAGE_INSTANCES = int(os.environ.get("JOIN_AVERAGE_INSTANCES", 1))
JOIN_AVERAGE_QUEUE_PREFIX = os.environ.get("JOIN_AVERAGE_QUEUE_PREFIX", "join_average_queue")
AVERAGE_QUEUE_PREFIX = os.environ.get("AVERAGE_QUEUE_PREFIX", "average_queue")

# Single store for per-message idempotency and crash recovery.  Keys:
#   "batch:{req_id}:{batch_id}" → DATA batches received but not yet aggregated
#       PENDING     : {"stage": PENDING, "payload": [sum_id, words, articles]}
#   "agg:{req_id}"              → running aggregate of a request
#       AGGREGATING : {"stage": AGGREGATING, "recv": [sum_id, ...], "words": N, "articles": N}
AVERAGE_STATE_PATH = os.environ.get("AVERAGE_STATE_PATH", f"tmp/state.json")

PENDING_STAGE = "PENDING"
AGGREGATE_STAGE = "AGGREGATING"

class Average:
    _TIMEOUT = 5.0

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self._is_running = True
        self._state = storage.AtomicStore(AVERAGE_STATE_PATH)

        self._requests: dict[int, tuple[set[int], list[int]]] = {}
        self._requests_lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, 
            queue_name=f"{AVERAGE_QUEUE_PREFIX}_{ID}"
        )
        
        self.join_average_queue_names = [f"{JOIN_AVERAGE_QUEUE_PREFIX}_{i}" for i in range(JOIN_AVERAGE_INSTANCES)]
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, 
            queues=self.join_average_queue_names
        )

        self.__post_init__()
    
    def __post_init__(self):
        for key in self._state.pending(AGGREGATE_STAGE):
            try:
                req_id = int(key.split(":")[1])
                rec = self._state.get(key)
                self._requests[req_id] = (set(rec["recv"]), [rec["words"], rec["articles"]])
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
                if len(recv) == SUM_AMOUNT_INSTANCES
            ]
        for req_id in complete:
            self._finish_request(req_id)

    def _finish_request(self, req_id: int):
        self._send_average(req_id)
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
                logging.warning(f"Shutdown timed out with still active threads")
        else:
            logging.warning(f"Shutdown timed out with still active threads")
        
        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close a queue")

        logging.info("All resources have been released; goodbye")


    def _add_partial(self, req_id: int, messages: list) -> int:
        logging.debug(f"Received an OPCODE_DATA message from req_id={req_id}")

        sum_id, words, articles = messages
        with self._requests_lock:
            recv, totals = self._requests.setdefault(req_id, (set(), [0, 0]))

            if sum_id in recv:
                logging.warning(f"Duplicated partial from sum_{sum_id} for req_id={req_id}")
                return len(recv)

            new_recv = recv | {sum_id}
            new_totals = [totals[0] + words, totals[1] + articles]

            # Persist first, then update memory.
            self._state.put(f"agg:{req_id}", {
                "stage": AGGREGATE_STAGE,
                "recv": sorted(new_recv),
                "words": new_totals[0],
                "articles": new_totals[1],
            })
            self._requests[req_id] = (new_recv, new_totals)
            return len(new_recv)

    def _send_average(self, req_id: int):
        message_id = zlib.crc32(f"{AVERAGE_QUEUE_PREFIX}_{ID}_{req_id}_{JOIN_AVERAGE_QUEUE_PREFIX}".encode())
        
        with self._requests_lock:
            _, (words, articles) = self._requests.get(req_id)

        average = words / articles if articles > 0 else 0.0
        logging.info(f"The average calculated for the client with req_id={req_id} is {words}/{articles} = {average}")

        logging.debug(f"Sending OPCODE_DATA message to all {JOIN_AVERAGE_QUEUE_PREFIX} for req_id={req_id}")
        message = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_DATA,
            message_id=message_id,
            req_id=req_id,
            batches=1, 
            message=average,
        )
        for name in self.join_average_queue_names:
            self.publisher.publish_queue(
                queue=name,
                message=message
            )

        logging.debug(f"Sending OPCODE_EOF message to all {JOIN_AVERAGE_QUEUE_PREFIX} for req_id={req_id}")
        eof = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_EOF,
            message_id=message_id,
            req_id=req_id,
            batches=1,
            message=0
        )
        for name in self.join_average_queue_names:
            self.publisher.publish_queue(
                queue=name, 
                message=eof
            )


    def _handle_data(self, ack: Callable, *args):
        batch_id, req_id, batches, messages = args
        msg_key = f"batch:{req_id}:{batch_id}"

        stage = self._state.get(msg_key).get(stage)
        if stage is not None:
            logging.warning(f"Duplicate redelivery from broker for req_id={req_id}; acknowledge and discard")
            ack()
            return

        self._state.put(msg_key, {"stage": PENDING_STAGE, "payload": messages})
        ack()

        recv = self._add_partial(req_id, messages)
        self._state.delete(msg_key)

        if recv == SUM_AMOUNT_INSTANCES:
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
        Average().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())