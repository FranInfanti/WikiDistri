import os
import time
import signal
import logging
import threading

from common import middleware, message_protocol, rabbitmq

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
SUM_AMOUNT_INSTANCES = int(os.environ.get("SUM_AMOUNT_INSTANCES", 1))
SUM_AMOUNT_PREFIX = os.environ.get("SUM_AMOUNT_PREFIX", "sum_amount")
SUM_AMOUNT_QUEUE = os.environ.get("SUM_AMOUNT_QUEUE", "sum_amount_queue")
AVERAGE_INSTANCES = int(os.environ.get("AVERAGE_INSTANCES", 1))
AVERAGE_QUEUE = os.environ.get("AVERAGE_QUEUE", "average_queue")

class SumAmount:
    _TIMEOUT = 5.0
    
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.partials: dict[int, list[int]] = {}
        self.state_lock = threading.Lock()
        self.average_queue_names = [f"{AVERAGE_QUEUE}_{i}" for i in range(AVERAGE_INSTANCES)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=SUM_AMOUNT_QUEUE
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=self.average_queue_names
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=SUM_AMOUNT_PREFIX,
            instances=SUM_AMOUNT_INSTANCES,
            on_all_recv=None,
            on_commit=self._flush, 
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
        with self.state_lock:
            acc = self.partials.setdefault(req_id, [0, 0])
            for (words,) in entries:
                acc[0] += words
                acc[1] += 1

        self.coordinator.record_recv(req_id)

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
                req_id=req_id,
                batches=1,
                message=[ID, words, articles],
            ),
        )

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")
                self._accumulate(req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received an OPCODE_EOF message for req_id={req_id}")
                self.coordinator.on_eof(req_id, payload)
            else:
                logging.warning(f"Unexpected OPCODE received: {opcode}")
            ack()
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