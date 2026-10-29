import os
import signal
import logging
import heapq

from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
JOIN_TOP_QUEUE = os.environ["JOIN_TOP_QUEUE"]
SUM_WORDS_COUNT = int(os.environ["SUM_WORDS_COUNT"])
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))

GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_4"


class JoinTop:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self.is_running = True

        self.requests: dict[int, tuple[set[int], list]] = {}

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_TOP_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=[GATEWAY_QUEUE]
        )

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
                self._handle_partial_top(req_id, payload)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _handle_partial_top(self, req_id: int, payload):
        sum_id, entries = payload
        reported, all_entries = self.requests.setdefault(req_id, (set(), []))

        if sum_id in reported:  # redelivered message
            logging.warning(f"req_id={req_id}: duplicate partial top from sum {sum_id}")
            return

        reported.add(sum_id)
        all_entries.extend(entries)

        if len(reported) == SUM_WORDS_COUNT:
            self._finish_request(req_id)

    def _finish_request(self, req_id: int):
        _, all_entries = self.requests.pop(req_id)

        top = heapq.nsmallest(TOP_SIZE, all_entries, key=lambda e: (-e[1], e[0]))
        words = [word for word, _total in top]
        logging.info(f"req_id={req_id}: global top {len(words)} words")

        self.publisher.publish_queue(
            GATEWAY_QUEUE,
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(words),
                message=words,
            ),
        )
        self.publisher.publish_queue(
            GATEWAY_QUEUE,
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
            ),
        )

    def start(self):
        try:
            logging.info(f"JoinTop {ID} waiting for messages...")
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
        JoinTop().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())