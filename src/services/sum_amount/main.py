import os
import time
import signal
import logging
import threading

from common import middleware, message_protocol, rabbitmq, eof

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
SUM_AMOUNT_QUEUE = os.environ["SUM_AMOUNT_QUEUE"]
SUM_AMOUNT_PREFIX = os.environ["SUM_AMOUNT_PREFIX"]
SUM_AMOUNT_COUNT = int(os.environ["SUM_AMOUNT_COUNT"])
AVERAGE_QUEUE = os.environ["AVERAGE_QUEUE"]
AVERAGE_COUNT = int(os.environ["AVERAGE_COUNT"])

class SumAmount:
    _TIMEOUT = 5.0
    
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.partials: dict[int, list[int]] = {}
        self.state_lock = threading.Lock()
        self.average_queue_names = [f"{AVERAGE_QUEUE}_{i}" for i in range(AVERAGE_COUNT)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=SUM_AMOUNT_QUEUE
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=self.average_queue_names
        )
        self.barrier = eof.EofBarrier(
            host=MOM_HOST,
            prefix=SUM_AMOUNT_PREFIX,
            instance_id=ID,
            instances=SUM_AMOUNT_COUNT,
            on_all_received=self._on_all_received,
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
        deadline = time.time() + self._TIMEOUT

        logging.info("Releasing service resources...")
        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to finish")

        remaining = max(0.0, deadline - time.time())
        self.barrier.close(timeout=remaining)

        remaining = max(0.0, deadline - time.time())
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

    def _accumulate(self, req_id: int, entries: list[list[int]]):
        with self.state_lock:
            acc = self.partials.setdefault(req_id, [0, 0])
            for (words,) in entries:
                acc[0] += words
                acc[1] += 1

        self.barrier.record_articles(req_id, len(entries))

    def _flush(self, req_id: int):
        with self.state_lock:
            words, articles = self.partials.pop(req_id, [0, 0])

        self.publisher.publish_queue(
            self.average_queue_names[req_id % AVERAGE_COUNT],
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=1,
                message=[ID, words, articles],
            ),
        )
        logging.debug(f"req_id={req_id}: partial sent ({words} words, {articles} articles)")

    def _on_all_received(self, req_id: int, _: int = 0):
        self._flush(req_id)

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received an OPCODE_DATA message from req_id={req_id}")
                self._accumulate(req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received an OPCODE_EOF message from req_id={req_id}")
                self.barrier.on_eof(req_id, payload)
            else:
                logging.warning(f"Unexpected OPCODE={opcode} received for req_id={req_id}")
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        try:
            logging.info(f"Waiting to receive messages...")
            self.barrier.start()
            self.input_queue.start_consuming(self.process_message)
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
        SumAmount().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())