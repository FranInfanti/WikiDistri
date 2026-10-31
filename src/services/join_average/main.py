import os
import signal
import logging
import threading
from dataclasses import dataclass, field

from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
JOIN_QUEUE = os.environ["JOIN_QUEUE"]
JOIN_AVERAGE_QUEUE = os.environ["JOIN_AVERAGE_QUEUE"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))

GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_5"  

@dataclass
class _RequestState:
    articles: list = field(default_factory=list)
    eof_received: bool = False
    average: float | None = None

class Join:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.is_running = True

        self.requests: dict[int, _RequestState] = {}
        self.lock = threading.Lock()

        self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_QUEUE}_{ID}"
        )
        self.average_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_AVERAGE_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=[GATEWAY_QUEUE])
        self.average_consumer: threading.Thread | None = None

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumers...")
        self.is_running = False
        for queue in (self.data_queue, self.average_queue):
            try:
                queue.stop_consuming_threadsafe()
            except Exception:
                logging.exception("Error stopping the consumer")

    def process_data(self, message, ack, nack):
        try:
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            with self.lock:
                state = self.requests.setdefault(req_id, _RequestState())
                if opcode == message_protocol.OPCODE_DATA:
                    state.articles.extend(payload)
                elif opcode == message_protocol.OPCODE_EOF:
                    logging.info(f"req_id={req_id}: EOF received")
                    state.eof_received = True
                else:
                    logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")
                self._try_finish(req_id)

            ack()
        except Exception:
            logging.exception("Error processing data message")
            nack()

    def process_average(self, message, ack, nack):
        try:
            _opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            with self.lock:
                state = self.requests.setdefault(req_id, _RequestState())
                state.average = payload[0]
                logging.info(f"req_id={req_id}: average received: {state.average}")
                self._try_finish(req_id)

            ack()
        except Exception:
            logging.exception("Error processing average message")
            nack()

    def _try_finish(self, req_id: int):
        """Must be called with self.lock held."""
        state = self.requests.get(req_id)
        if state is None or not state.eof_received or state.average is None:
            return

        del self.requests[req_id]

        result = [[url, amount] for url, amount in state.articles if amount > state.average]
        logging.info(f"req_id={req_id}: {len(result)} of {len(state.articles)} articles above {state.average}")

        for i in range(0, len(result), CHUNK_SIZE):
            chunk = result[i:i + CHUNK_SIZE]
            self.publisher.publish_queue(
                GATEWAY_QUEUE,
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(chunk), message=chunk,
                ),
            )
        self.publisher.publish_queue(
            GATEWAY_QUEUE,
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF, req_id=req_id,
            ),
        )

    def start(self):
        try:
            logging.info(f"Join {ID} waiting for messages...")
            self.average_consumer = threading.Thread(
                target=self.average_queue.start_consuming,
                args=(self.process_average,),
                name="JoinAverage",
            )
            self.average_consumer.start()
            self.data_queue.start_consuming(self.process_data)
        except Exception:
            if self.is_running:
                raise
        finally:
            self._close()

    def _close(self):
        logging.info("Closing queues...")
        try:
            self.average_queue.stop_consuming_threadsafe()
        except Exception:
            pass
        if self.average_consumer is not None:
            self.average_consumer.join()
        self.publisher.stop()
        for queue in (self.data_queue, self.average_queue):
            try:
                queue.close()
            except Exception:
                logging.exception("Error closing queue")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    try:
        Join().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())