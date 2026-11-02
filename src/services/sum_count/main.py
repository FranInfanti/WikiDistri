import os
import time
import heapq
import signal
import logging

from common import middleware, message_protocol, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
TOP_SIZE = int(os.environ.get("TOP_SIZE", 20))
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
JOIN_TOP_QUEUE = os.environ["JOIN_TOP_QUEUE"]
JOIN_TOP_COUNT = int(os.environ["JOIN_TOP_COUNT"])
SUM_WORDS_COUNT = int(os.environ["SUM_WORDS_COUNT"])
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))
SUM_COUNT_PREFIX = os.environ["SUM_COUNT_PREFIX"]
COORDINATOR_ID = int(os.environ.get("COORDINATOR_ID", 0))
SUM_WORDS_QUEUE_PREFIX = os.environ["SUM_WORDS_QUEUE_PREFIX"]

class SumCount:
    _TIMEOUT = 5.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_3"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.words: dict[int, dict[str, list[int]]] = {}
        self.join_top_queue_names = [f"{JOIN_TOP_QUEUE}_{i}" for i in range(JOIN_TOP_COUNT)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{SUM_WORDS_QUEUE_PREFIX}_{ID}"
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE, *self.join_top_queue_names],
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=SUM_COUNT_PREFIX,
            instances=SUM_WORDS_COUNT,
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
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            logging.warning("An error occurred while attempting to stop the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            logging.warning("An error occurred while attempting to stop the publisher")

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _on_recv_all(self, req_id: int, _: list):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id
            )
        )

    def _on_commit(self, req_id: int):
        request_words = self.words.pop(req_id, {})

        # send common words in English and French to the Gateway
        self._send_common_words(req_id, request_words)

        # send the partial top of words to the Top Process
        self._send_partial_top(req_id, request_words)

    def _send_common_words(self, req_id: int, request_words: dict):
        chunk = []
        sent = 0

        for word, (en_count, fr_count) in request_words.items():
            if en_count > 0 and fr_count > 0:
                chunk.append(word)

                if len(chunk) == CHUNK_SIZE:
                    self._send_chunk(req_id, chunk)
                    sent += len(chunk)
                    chunk = []

        if chunk:
            self._send_chunk(req_id, chunk)
            sent += len(chunk)

        logging.info(f"{sent} common words were sent out of a total of {len(request_words)} words for req_id={req_id}")

    def _send_chunk(self, req_id: int, chunk: list[str]):
        logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")

        self.publisher.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(chunk),
                message=chunk,
            )
        )

    def _send_partial_top(self, req_id: int, request_words: dict):
        top = heapq.nsmallest(
            TOP_SIZE,
            ((-(en + fr), word) for word, (en, fr) in request_words.items()),
        )
        entries = [[word, -neg_total] for neg_total, word in top]

        top_process_queue_name = self.join_top_queue_names[req_id % JOIN_TOP_COUNT]
        logging.debug(f"Sending OPCODE_DATA message to {top_process_queue_name} for req_id={req_id}")
        
        self.publisher.publish_queue(
            queue=top_process_queue_name,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(entries),
                message=[ID, entries],
            ),
        )

    def _accumulate(self, req_id: int, entries: list[list]):
        request_words = self.words.setdefault(req_id, {})

        for word, en_count, fr_count in entries:
            counts = request_words.setdefault(word, [0, 0])
            counts[0] += en_count
            counts[1] += fr_count

        self.coordinator.record_recv(req_id=req_id)

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received OPCODE_DATA message for req_id={req_id}")
                self._accumulate(req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received OPCODE_EOF message for req_id={req_id}")
                self.coordinator.on_eof(req_id, payload)
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
        SumCount().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())
    