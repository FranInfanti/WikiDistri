import os
import time
import heapq
import signal
import logging

from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
JOIN_TOP_QUEUE = os.environ["JOIN_TOP_QUEUE"]
SUM_WORDS_COUNT = int(os.environ["SUM_WORDS_COUNT"])
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))

class JoinTop:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_4"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.requests: dict[int, tuple[set[int], list]] = {}
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{JOIN_TOP_QUEUE}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST, queues=[self._GATEWAY_QUEUE]
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

    def _handle_partial_top(self, req_id: int, payload: list):
        sum_id, entries = payload
        reported, all_entries = self.requests.setdefault(req_id, (set(), []))

        if sum_id in reported:  # redelivered message
            logging.warning(f"Duplicated partial top from Sum instance {sum_id} for client with req_id={req_id}")
            return

        reported.add(sum_id)
        all_entries.extend(entries)

        if len(reported) == SUM_WORDS_COUNT:
            self._finish_request(req_id)

    def _finish_request(self, req_id: int):
        _, all_entries = self.requests.pop(req_id)
        top = heapq.nsmallest(TOP_SIZE, all_entries, key=lambda e: (-e[1], e[0]))
        words = [word for word, _total in top]

        logging.info(f"The global top calculated for the client with req_id={req_id} has {len(words)} words")

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(words),
                message=words,
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

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received an OPCODE_DATA message from req_id={req_id}")
                self._handle_partial_top(req_id, payload)
            else:
                logging.warning(f"Unexpected OPCODE={opcode} received for req_id={req_id}")
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        try:
            logging.info(f"Waiting to receive messages...")
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
        JoinTop().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())