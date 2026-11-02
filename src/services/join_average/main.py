import os
import time
import signal
import logging
import threading
from dataclasses import dataclass, field

from common import middleware, message_protocol, rabbitmq
from common.rabbitmq.coordinator import RabbitCoordinator

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
JOIN_AVERAGE_QUEUE = os.environ["JOIN_AVERAGE_QUEUE"]
JOIN_PREFIX = os.environ["JOIN_PREFIX"]
JOIN_COUNT = int(os.environ["JOIN_COUNT"])
COORDINATOR_ID = int(os.environ.get("COORDINATOR_ID", 0))
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))

@dataclass
class _RequestState:
    pending: list = field(default_factory=list)
    average: float | None = None
class JoinAverage:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_5"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.is_running = True

        self.requests: dict[int, _RequestState] = {}
        self.lock = threading.Lock()

        self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_AVERAGE_QUEUE}_{ID}"
        )

        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=[self._GATEWAY_QUEUE])
        self.coordinator = RabbitCoordinator(
            id=ID, host=MOM_HOST, prefix=JOIN_PREFIX, instances=JOIN_COUNT,
            on_all_recv=self._send_final_eof,
            on_commit=self._on_commit,
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM received, shutting down...")
        self.is_running = False
        try:
            self.data_queue.stop_consuming()
        except Exception:
            logging.warning("Error stopping the data consumer")

    def _send_above_average(self, req_id: int, articles: list, average: float):
        result = [[url, amount] for url, amount in articles if amount > average]
        for i in range(0, len(result), CHUNK_SIZE):
            chunk = result[i:i + CHUNK_SIZE]
            self.publisher.publish_queue(
                queue=self._GATEWAY_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(chunk), message=chunk,
                ),
            )

    def process_data(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                if isinstance(payload, list):
                    self._handle_articles(req_id, payload)
                else:
                    self._handle_average(req_id, payload)
                self.coordinator.record_recv(req_id)
            elif opcode == message_protocol.OPCODE_EOF:
                if ID != COORDINATOR_ID:
                    logging.warning(f"EOF received by non-coordinator join {ID}")
                else:
                    self.coordinator.on_eof(req_id, payload + JOIN_COUNT)
            else:
                logging.warning(f"Unexpected OPCODE={opcode} for req_id={req_id}")
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _handle_articles(self, req_id: int, articles: list):
        with self.lock:
            state = self.requests.setdefault(req_id, _RequestState())
            if state.average is None:
                state.pending.extend(articles)
            else:
                self._send_above_average(req_id, articles, state.average)

    def _handle_average(self, req_id: int, average: float):
        with self.lock:
            state = self.requests.setdefault(req_id, _RequestState())
            state.average = average
            logging.info(f"Average {average} received for req_id={req_id}")
            pending, state.pending = state.pending, []
            self._send_above_average(req_id, pending, average)

    def _on_commit(self, req_id: int):
        with self.lock:
            self.requests.pop(req_id, None)
        logging.info(f"Shard finished for req_id={req_id}")

    def _send_final_eof(self, req_id: int, _sent_by_key=None):
        logging.info(f"All joins finished, sending EOF for req_id={req_id}")
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF, req_id=req_id,
            ),
        )

    def start(self):
        try:
            logging.info("Waiting to receive messages...")
            self.coordinator.start()
            self.data_queue.start_consuming(self.process_data)
        except Exception:
            if self.is_running:
                logging.critical("An error has occurred in the service")
                raise
        finally:
            self._close()

    def _close(self):
        deadline = time.time() + self._TIMEOUT
        logging.info("Releasing service resources...")

        if self.coordinator.stop(timeout=max(0.1, deadline - time.time())):
            logging.warning("Coordinator threads did not stop in time")
        if self.publisher.stop(timeout=max(0.1, deadline - time.time())):
            logging.warning("Publisher thread did not stop in time")

        try:
            self.data_queue.close()
        except Exception:
            logging.warning("Error closing the data queue")

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