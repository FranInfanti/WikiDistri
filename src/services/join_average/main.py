import os
import time
import signal
import logging
import threading

from dataclasses import dataclass, field
from common import middleware, message_protocol, rabbitmq

ID = int(os.environ["ID"])
COORDINATOR_ID = int(os.environ.get("COORDINATOR_ID", 0))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
GATEWAY_PREFIX = os.environ.get("GATEWAY_PREFIX", "gateway")
JOIN_AVERAGE_INSTANCES = int(os.environ.get("JOIN_AVERAGE_INSTANCES", 1))
JOIN_AVERAGE_PREFIX = os.environ.get("JOIN_AVERAGE_PREFIX", "join_average")
JOIN_AVERAGE_QUEUE = os.environ.get("JOIN_AVERAGE_QUEUE", "join_average_queue")

@dataclass
class _RequestState:
    average: float | None = None
    pending: list = field(default_factory=list)

class JoinAverage:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_5"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.eof_recv = {}
        self.state_lock = threading.Lock()
        self.requests: dict[int, _RequestState] = {}
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_AVERAGE_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=[self._GATEWAY_QUEUE])
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID, host=MOM_HOST, prefix=JOIN_AVERAGE_PREFIX, instances=JOIN_AVERAGE_INSTANCES,
            on_all_recv=self._on_recv_all,
            on_commit=self._on_commit,
        )

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
        with self.state_lock:
            self.requests.pop(req_id, None)
        logging.info(f"Finished request for req_id={req_id}")

    def _on_recv_all(self, req_id: int, _sent_by_key=None):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF, req_id=req_id,
            ),
        )

    def _send_above_average(self, req_id: int, articles: list, average: float):
        result = [[url, amount] for url, amount in articles if amount > average]
        for i in range(0, len(result), CHUNK_SIZE):
            chunk = result[i:i + CHUNK_SIZE]

            logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=self._GATEWAY_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(chunk), message=chunk,
                ),
            )

    def _recv_articles(self, req_id: int, articles: list):
        with self.state_lock:
            state = self.requests.setdefault(req_id, _RequestState())
            if state.average is None:
                state.pending.extend(articles)
            else:
                self._send_above_average(req_id, articles, state.average)

    def _recv_average(self, req_id: int, average: float):
        with self.state_lock:
            state = self.requests.setdefault(req_id, _RequestState())
            state.average = average
            logging.debug(f"Received an average of {average} for req_id={req_id}")
            
            pending, state.pending = state.pending, []
            self._send_above_average(req_id, pending, average)

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received OPCODE_DATA message for req_id={req_id}")
                if isinstance(payload, list):
                    self._recv_articles(req_id, payload)
                else:
                    self._recv_average(req_id, payload)
                self.coordinator.record_recv(req_id)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received OPCODE_EOF message for req_id={req_id}")
                if ID != COORDINATOR_ID:
                    logging.warning(f"EOF was received by a non-coordinator join_{ID}")
                else:
                    eof_recv, expected = self.eof_recv.get(req_id, (0, 0))
                    eof_recv += 1
                    expected += payload

                    logging.debug(f"Received total OPCODE_EOF message for req_id={req_id}: {eof_recv}; expected: {payload}")
                    if eof_recv == 2:
                        logging.info(f"All joins finished for req_id={req_id}, sending final EOF to gateway")
                        self.coordinator.on_eof(req_id, expected + JOIN_AVERAGE_INSTANCES)
                        self.eof_recv.pop(req_id, None)
                    else:
                        self.eof_recv[req_id] = (eof_recv, expected)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
            ack()
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
        JoinAverage().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())