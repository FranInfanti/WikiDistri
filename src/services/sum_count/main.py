import os
import signal
import logging
import threading

from common import middleware
from common import message_protocol
from common.eof.eof_barrier import EofBarrier

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

ID = int(os.environ["ID"])
SUM_WORDS_QUEUE_PREFIX = os.environ["SUM_WORDS_QUEUE_PREFIX"]
SUM_WORDS_COUNT = int(os.environ["SUM_WORDS_COUNT"])

SUM_BARRIER_PREFIX = os.environ["SUM_BARRIER_PREFIX"]
COORDINATOR_ID = int(os.environ.get("COORDINATOR_ID", 0))

GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 100))

class Sum:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.is_running = True
        self.words: dict[int, dict[str, list[int]]] = {}

        self.gateway_lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=f"{SUM_WORDS_QUEUE_PREFIX}_{ID}"
        )
        self.gateway_queue = self._new_gateway_queue()

        self.barrier = EofBarrier(
            host=MOM_HOST,
            prefix=SUM_BARRIER_PREFIX,
            instance_id=ID,
            instances=SUM_WORDS_COUNT,
            on_all_received=self._send_final_eof,
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumers...")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping the consumer")

        self.barrier.stop_consuming()

    def _new_gateway_queue(self):
        return middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, 
            queue_name=f"{GATEWAY_PREFIX}_3"
        )

    def _gateway_send(self, data: bytes):
        """
        The Sum publishes nothing while it accumulates, so this connection can
        sit idle for minutes and the broker may close it (missed heartbeats).
        If the send fails, reconnect once and retry.
        """
        with self.gateway_lock:
            try:
                self.gateway_queue.send(data)
            except Exception:
                logging.warning("Connection to the gateway queue lost, reconnecting")
                try:
                    self.gateway_queue.close()
                except Exception:
                    pass
                self.gateway_queue = self._new_gateway_queue()
                self.gateway_queue.send(data)

    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                self._accumulate(req_id, payload)
            elif opcode == message_protocol.OPCODE_EOF:
                logging.info(f"Received EOF for req_id: {req_id}")
                self._finish_request(req_id)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _accumulate(self, req_id: int, entries: list[list]):
        request_words = self.words.setdefault(req_id, {})

        for word, en_count, fr_count in entries:
            counts = request_words.setdefault(word, [0, 0])
            counts[0] += en_count
            counts[1] += fr_count

    def _finish_request(self, req_id: int):
        request_words = self.words.pop(req_id, {})

        sent = self._send_common_words(req_id, request_words)
        logging.info(f"req_id={req_id}: {sent} common words out of {len(request_words)}")

        self.barrier.record_articles(req_id, 1)

        if ID == COORDINATOR_ID:
            self.barrier.on_eof(req_id, SUM_WORDS_COUNT)

    def _send_common_words(self, req_id: int, request_words: dict) -> int:
        """Streams the words seen in both languages in chunks of CHUNK_SIZE."""
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

        return sent

    def _send_chunk(self, req_id: int, chunk: list[str]):
        self._gateway_send(
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DATA,
                req_id=req_id,
                batches=len(chunk),
                message=chunk,
            )
        )

    def _send_final_eof(self, req_id: int):
        """Called by the barrier, in the coordinator only, once every Sum finished req_id."""
        logging.info(f"req_id={req_id}: all Sums finished, sending the EOF to the gateway")
        self._gateway_send(
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
            )
        )

    def start(self):
        try:
            logging.info(f"Sum {ID} waiting for messages...")
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

        for queue in (self.input_queue, self.gateway_queue):
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
        Sum().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())