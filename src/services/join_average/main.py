import os
import time
import signal
import logging
import threading

from dataclasses import dataclass, field
from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
JOIN_QUEUE = os.environ["JOIN_QUEUE"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
JOIN_AVERAGE_QUEUE = os.environ["JOIN_AVERAGE_QUEUE"]

@dataclass
class _RequestState:
    articles: list = field(default_factory=list)
    eof_received: bool = False
    average: float | None = None

class Join:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_5"  

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.requests: dict[int, _RequestState] = {}
        self.state_lock = threading.Lock()
        self.data_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_QUEUE}_{ID}"
        )
        self.average_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_AVERAGE_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=[self._GATEWAY_QUEUE])
        self.average_consumer: threading.Thread | None = None

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, proceed to shutdown the service...")
        self.is_running = False
        try:
            self.data_queue.stop_consuming()
        except:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

    def _close(self):
        deadline = time.time() + self._TIMEOUT

        logging.info("Releasing service resources...")
        try:
            self.average_queue.stop_consuming_threadsafe()
        except Exception:
            logging.warning("An error occurred while attempting to stop consumption from a queue")

        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to finish")
        if self.average_consumer:
            remaining = max(0.0, deadline - time.time())
            if remaining > 0: 
                self.average_consumer.join(timeout=remaining)

            if self.average_consumer.is_alive():
                logging.warning(f"Shutdown timed out with still active threads")

        remaining = max(0.0, deadline - time.time())
        if remaining > 0:
            if self.publisher.stop(timeout=remaining):
                logging.warning(f"Shutdown timed out with still active threads")
        else:
            logging.warning(f"Shutdown timed out with still active threads")

        for queue in (self.data_queue, self.average_queue):
            try:
                queue.close()
            except Exception:
                logging.warning("An error occurred while attempting to close a queue")

        logging.info("All resources have been released; goodbye")

    def _try_finish(self, req_id: int):
        """Must be called with self.state_lock held."""
        state = self.requests.get(req_id)
        if state is None or not state.eof_received or state.average is None:
            logging.debug(f"The processing for the client with req_id={req_id} has not finished...")
            return
        self.requests.pop(req_id, None)

        result = [ [url, amount] for url, amount in state.articles if amount > state.average ]
        logging.info(
            f"Sending over %s of %s articles above average %s for client with req_id=%s",
            len(result),
            len(state.articles),
            state.average,
            req_id
        )

        for i in range(0, len(result), CHUNK_SIZE):
            chunk = result[i:i + CHUNK_SIZE]
            self.publisher.publish_queue(
                queue=self._GATEWAY_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, 
                    batches=len(chunk), 
                    message=chunk,
                ),
            )

        logging.info(f"Finish processing for client with req_id={req_id}")
        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF, 
                req_id=req_id,
            ),
        )

    def process_data(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            with self.state_lock:
                state = self.requests.setdefault(req_id, _RequestState())
                if opcode == message_protocol.OPCODE_DATA:
                    logging.debug(f"Received an OPCODE_DATA message from req_id={req_id}")
                    state.articles.extend(payload)
                elif opcode == message_protocol.OPCODE_EOF:
                    logging.debug(f"Received an OPCODE_EOF message from req_id={req_id}")
                    state.eof_received = True
                else:
                    logging.warning(f"Unexpected OPCODE={opcode} received for req_id={req_id}")
                self._try_finish(req_id)
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def process_average(self, message, ack, nack):
        try:
            _, req_id, _, payload = message_protocol.internal.deserialize(message)

            with self.state_lock:
                state = self.requests.setdefault(req_id, _RequestState())
                state.average = payload[0]
                logging.info(f"Received the average {state.average} for client with req_id={req_id}")
                self._try_finish(req_id)
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        try:
            logging.info(f"Waiting to receive messages...")
            self.average_consumer = threading.Thread(
                target=self.average_queue.start_consuming,
                args=(self.process_average,),
                name="JoinAverage",
            )
            self.average_consumer.start()
            self.data_queue.start_consuming(self.process_data)
        except Exception:
            if self.is_running:
                logging.critical(f"An error has occurred in the service")
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
        Join().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())
