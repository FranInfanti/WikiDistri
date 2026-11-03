import os
import re
import signal
import logging
import time

from datetime import datetime
from stopwords import STOPWORDS
from common import middleware, message_protocol, hashing, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

INPUT_QUEUE = os.environ["WORD_READER_QUEUE"]

ID = int(os.environ["ID"])
WORD_READER_PREFIX = os.environ["WORD_READER_PREFIX"]
WORD_READER_AMOUNT = int(os.environ.get("WORD_READER_AMOUNT", 1))

SUM_WORDS_QUEUE_PREFIX = os.environ["SUM_WORDS_QUEUE_PREFIX"]
SUM_WORDS_COUNT = int(os.environ.get("SUM_WORDS_COUNT", 1))

JOIN_AVERAGE_QUEUE = os.environ["JOIN_AVERAGE_QUEUE"]
JOIN_COUNT = int(os.environ["JOIN_COUNT"])
SUM_AMOUNT_QUEUE = os.environ["SUM_AMOUNT_QUEUE"]

CUTOFF_YEAR = 2021
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def tokenize(abstract: str, language: str | None) -> list[str]:
    stopwords = STOPWORDS.get(language or "", set())
    return [w for w in WORD_RE.findall(abstract.lower()) if w not in stopwords]

def language_counts(language: str | None) -> tuple[int, int] | None:
    if language == "en":
        return 1, 0
    if language == "fr":
        return 0, 1
    return None

class WordReader:
    _TIMEOUT = 5.0

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
 
        self.is_running = True

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=INPUT_QUEUE
        )
        self.sum_words_queue_names = [
            f"{SUM_WORDS_QUEUE_PREFIX}_{i}" for i in range(SUM_WORDS_COUNT)
        ]

        self.join_queue_names = [f"{JOIN_AVERAGE_QUEUE}_{i}" for i in range(JOIN_COUNT)]
        
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.sum_words_queue_names, *self.join_queue_names, SUM_AMOUNT_QUEUE],
        )

        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=WORD_READER_PREFIX,
            instances=WORD_READER_AMOUNT,
            on_all_recv=self._on_recv_all
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
                self._handle_articles(req_id, payload)
                logging.debug("req_id=%s: processed %s articles", req_id, len(payload))
            elif opcode == message_protocol.OPCODE_EOF:
                logging.info(f"Received EOF for req_id: {req_id}, articles sent: {payload}")
                self.coordinator.on_eof(req_id, payload)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()
 
    def _handle_articles(self, req_id: int, articles: list[list]):
        words_by_shard: dict[int, dict[str, list[int]]] = {}
        join_batches: dict[int, list] = {}
        amount_batch = []

        for article in articles:
            abstract, url, date_modified, in_language = article
 
            words = tokenize(abstract or "", in_language)
 
            self._collect_unique_words(req_id, words, in_language, words_by_shard)
            self._collect_word_amount(req_id, url, len(words), date_modified, join_batches, amount_batch)
 
        self._send_unique_words(req_id, words_by_shard)
        self._send_word_amounts(req_id, join_batches, amount_batch)

        for _ in articles:
            self.coordinator.record_recv(req_id=req_id)

    @staticmethod
    def _collect_unique_words(req_id, words, language, words_by_shard):
        counts = language_counts(language)
        if counts is None:
            return

        en_count, fr_count = counts
        for word in set(words):
            shard = hashing.shard_for(SUM_WORDS_COUNT, req_id, word)
            entry = words_by_shard.setdefault(shard, {}).setdefault(word, [0, 0])
            entry[0] += en_count
            entry[1] += fr_count

    @staticmethod
    def _collect_word_amount(req_id, url, amount, date_modified, join_batches, amount_batch):
        if date_modified is None:
            logging.warning(f"Article without date_modified: {url}")
            return
 
        year = datetime.strptime(date_modified, DATE_FORMAT).year
        if year >= CUTOFF_YEAR:
            shard = hashing.shard_for(JOIN_COUNT, req_id, url)
            join_batches.setdefault(shard, []).append([url, amount])
        else:
            amount_batch.append([amount])

    def _send_unique_words(self, req_id, words_by_shard):
        for shard, shard_words in words_by_shard.items():
            message = [[word, en, fr] for word, (en, fr) in shard_words.items()]
            self.publisher.publish_queue(
                self.sum_words_queue_names[shard],
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(message), message=message,
                ),
            )
            self.coordinator.record_sent(who=SUM_WORDS_QUEUE_PREFIX, req_id=req_id)

    def _send_word_amounts(self, req_id, join_batches, amount_batch):
        for shard, batch in join_batches.items():
            self.publisher.publish_queue(
                self.join_queue_names[shard],
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(batch), message=batch,
                ),
            )
            logging.debug("req_id=%s -> %s: %s articles", req_id, JOIN_AVERAGE_QUEUE, len(join_batches))
            self.coordinator.record_sent(who=JOIN_AVERAGE_QUEUE, req_id=req_id) 

        if amount_batch:
            self.publisher.publish_queue(
                SUM_AMOUNT_QUEUE,
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(amount_batch), message=amount_batch,
                ),
            )
            self.coordinator.record_sent(who=SUM_AMOUNT_QUEUE, req_id=req_id)


    def _on_recv_all(self, req_id: int, sent_messages: list):
        logging.info(f"All articles received for req_id: {req_id}. Forwarding EOF to next stages...")

        for key, sent in sent_messages:
            if key == SUM_WORDS_QUEUE_PREFIX:
                self.publisher.publish_queue(
                    self.sum_words_queue_names[0],
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )

            elif key == JOIN_AVERAGE_QUEUE:
                self.publisher.publish_queue(
                    self.join_queue_names[0],
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )

            elif key == SUM_AMOUNT_QUEUE:
                self.publisher.publish_queue(
                    SUM_AMOUNT_QUEUE,
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            else:
                raise ValueError(f"Unknown key; received: {key}")
            
            logging.info("req_id=%s: EOF(%s) -> %s", req_id, sent, key)

    def start(self):
        try:
            logging.info("Word Reader waiting for messages...")
            self.coordinator.start()
            self.input_queue.start_consuming(self.process_message)
        except Exception:
            if self.is_running:
                raise
        finally:
            self._close()

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info("Waiting up to %s seconds for active threads to stop", self._TIMEOUT)

        timed_out = []
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            logging.exception("Error stopping the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            logging.exception("Error stopping the publisher")

        try:
            self.input_queue.close()
        except Exception:
            logging.exception("Error closing the input queue")

        if timed_out:
            logging.error("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")
 
def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    logging.getLogger("pika").propagate = False

    try:
        WordReader().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())