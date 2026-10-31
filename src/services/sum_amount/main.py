import os
import signal
import logging
import threading

from common import middleware, message_protocol, rabbitmq
from common.eof.eof_barrier import EofBarrier

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
SUM_AMOUNT_QUEUE = os.environ["SUM_AMOUNT_QUEUE"]
SUM_AMOUNT_PREFIX = os.environ["SUM_AMOUNT_PREFIX"]
SUM_AMOUNT_COUNT = int(os.environ["SUM_AMOUNT_COUNT"])
AVERAGE_QUEUE = os.environ["AVERAGE_QUEUE"]
AVERAGE_COUNT = int(os.environ["AVERAGE_COUNT"])


class SumAmount:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.is_running = True

        self.partials: dict[int, list[int]] = {}
        self.lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=SUM_AMOUNT_QUEUE
        )
        self.average_queue_names = [f"{AVERAGE_QUEUE}_{i}" for i in range(AVERAGE_COUNT)]
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=self.average_queue_names
        )

        self.barrier = EofBarrier(
            host=MOM_HOST,
            prefix=SUM_AMOUNT_PREFIX,
            instance_id=ID,
            instances=SUM_AMOUNT_COUNT,
            on_all_received=self._on_all_received,
            on_commit=self._flush,
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumers...")
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping the consumer")
        self.barrier.stop_consuming()

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._accumulate(req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.info(f"Received EOF for req_id: {req_id}, expected entries: {payload}")
                self.barrier.on_eof(req_id, payload)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _accumulate(self, req_id: int, entries: list[list[int]]):
        with self.lock:
            acc = self.partials.setdefault(req_id, [0, 0])
            for (words,) in entries:
                acc[0] += words
                acc[1] += 1

        self.barrier.record_articles(req_id, len(entries))

    def _flush(self, req_id: int):
        with self.lock:
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
        logging.info(f"req_id={req_id}: partial sent ({words} words, {articles} articles)")

    def _on_all_received(self, req_id: int, _sent_total: int = 0):
        self._flush(req_id)

    def start(self):
        try:
            logging.info(f"SumAmount {ID} waiting for messages...")
            self.barrier.start()
            self.input_queue.start_consuming(self.process_message)
        except Exception:
            if self.is_running:
                raise
        finally:
            self._close()

    def _close(self):
        logging.info("Closing queues...")
        self.barrier.close()
        self.publisher.stop()
        try:
            self.input_queue.close()
        except Exception:
            logging.exception("Error closing queue")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    try:
        SumAmount().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())