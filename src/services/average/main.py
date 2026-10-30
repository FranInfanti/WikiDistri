import os
import signal
import logging

from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
AVERAGE_QUEUE = os.environ["AVERAGE_QUEUE"]
SUM_AMOUNT_COUNT = int(os.environ["SUM_AMOUNT_COUNT"])

JOIN_AVERAGE_QUEUE = os.environ["JOIN_AVERAGE_QUEUE"]
JOIN_COUNT = int(os.environ["JOIN_COUNT"])


class Average:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.is_running = True

        self.requests: dict[int, tuple[set[int], list[int]]] = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{AVERAGE_QUEUE}_{ID}"
        )

        self.join_queue_names = [f"{JOIN_AVERAGE_QUEUE}_{i}" for i in range(JOIN_COUNT)]
        self.publisher = rabbitmq.RabbitPublisher(host=MOM_HOST, queues=self.join_queue_names)

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumer...")
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping the consumer")

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._handle_partial(req_id, payload)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _handle_partial(self, req_id: int, payload):
        sum_id, words, articles = payload
        reported, totals = self.requests.setdefault(req_id, (set(), [0, 0]))

        if sum_id in reported:
            logging.warning(f"req_id={req_id}: duplicate partial from sum_amount {sum_id}")
            return

        reported.add(sum_id)
        totals[0] += words
        totals[1] += articles

        if len(reported) == SUM_AMOUNT_COUNT:
            self._finish_request(req_id)

    def _finish_request(self, req_id: int):
        _, (words, articles) = self.requests.pop(req_id)
        average = words / articles if articles > 0 else 0.0
        logging.info(f"req_id={req_id}: {words} words / {articles} articles = {average}")

        message = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_DATA,
            req_id=req_id, batches=1, message=[average],
        )
        for name in self.join_queue_names:
            self.publisher.publish_queue(name, message)
    
    def start(self):
        try:
            logging.info(f"Average {ID} waiting for messages...")
            self.input_queue.start_consuming(self.process_message)
        except Exception:
            if self.is_running:
                raise
        finally:
            self._close()

    def _close(self):
        logging.info("Closing queues...")
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
        Average().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())