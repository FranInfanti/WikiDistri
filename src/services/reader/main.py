import os
import re
import signal
import logging

from datetime import datetime
from stopwords import STOPWORDS
from common import middleware, message_protocol, hashing, eof, rabbitmq

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

INPUT_QUEUE = os.environ["WORD_READER_OUTPUT_QUEUE"]

ID = int(os.environ["ID"])
WORD_READER_PREFIX = os.environ["WORD_READER_PREFIX"]
WORD_READER_AMOUNT = int(os.environ.get("WORD_READER_AMOUNT", 1))

SUM_WORDS_QUEUE_PREFIX = os.environ["SUM_WORDS_QUEUE_PREFIX"]
SUM_WORDS_COUNT = int(os.environ.get("SUM_WORDS_COUNT", 1))

JOIN_QUEUE = os.environ["JOIN_QUEUE"]
SUM_AMOUNT_QUEUE = os.environ["SUM_AMOUNT_QUEUE"]

CUTOFF_YEAR = 2021
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
WORD_RE = re.compile(r"\w+", re.UNICODE)


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
    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
 
        self.is_running = True

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=INPUT_QUEUE
        )
        self.sum_words_queue_names = [
            f"{SUM_WORDS_QUEUE_PREFIX}_{i}" for i in range(SUM_WORDS_COUNT)
        ]

        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.sum_words_queue_names, JOIN_QUEUE, SUM_AMOUNT_QUEUE],
        )

        self.barrier = eof.EofBarrier(
            host=MOM_HOST,
            prefix=WORD_READER_PREFIX,
            instance_id=ID,
            instances=WORD_READER_AMOUNT,
            on_all_received=self._forward_eof,
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumer...")
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
                self._handle_articles(req_id, payload)
                logging.debug("req_id=%s: processed %s articles", req_id, len(payload))
            elif opcode == message_protocol.OPCODE_EOF:
                logging.info(f"Received EOF for req_id: {req_id}, articles sent: {payload}")
                self.barrier.on_eof(req_id, payload)
            else:
                logging.warning(f"Unexpected opcode for req_id {req_id}: {opcode}")

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()
 
    def _handle_articles(self, req_id: int, articles: list[list]):
        words_by_shard: dict[int, dict[str, list[int]]] = {}
        join_batch = []
        amount_batch = []

        for article in articles:
            abstract, url, date_modified, in_language = article
 
            words = tokenize(abstract or "", in_language)
 
            self._collect_unique_words(req_id, words, in_language, words_by_shard)
            self._collect_word_amount(url, len(words), date_modified, join_batch, amount_batch)
 
        self._send_unique_words(req_id, words_by_shard)
        self._send_word_amounts(req_id, join_batch, amount_batch)

        self.barrier.record_articles(req_id, len(articles), sent=len(amount_batch))

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
    def _collect_word_amount(url, amount, date_modified, join_batch, amount_batch):
        if date_modified is None:
            logging.warning(f"Article without date_modified: {url}")
            return
 
        year = datetime.strptime(date_modified, DATE_FORMAT).year
        if year >= CUTOFF_YEAR:
            join_batch.append([url, amount])
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

    def _send_word_amounts(self, req_id, join_batch, amount_batch):
        if join_batch:
            self.publisher.publish_queue(
                JOIN_QUEUE,
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(join_batch), message=join_batch,
                ),
            )
            logging.debug("req_id=%s -> %s: %s articles", req_id, JOIN_QUEUE, len(join_batch))

        if amount_batch:
            self.publisher.publish_queue(
                SUM_AMOUNT_QUEUE,
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(amount_batch), message=amount_batch,
                ),
            )

    def _forward_eof(self, req_id: int, sent_total: int = 0):
        """
        sent_total = entries sent to sum_amount by ALL the replicas. Only
        sum_amount needs it, so the other EOFs carry no payload.
        """
        logging.info(f"All articles received for req_id: {req_id}. Forwarding EOF to next stages...")

        plain_eof = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_EOF,
            req_id=req_id,
        )
        for name in [*self.sum_words_queue_names, JOIN_QUEUE]:
            self.publisher.publish_queue(name, plain_eof)
            logging.info("req_id=%s: EOF -> %s", req_id, name)

        self.publisher.publish_queue(
            SUM_AMOUNT_QUEUE,
            message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
                batches=1,
                message=sent_total,
            ),
        )
        logging.info("req_id=%s: EOF(%s) -> %s", req_id, sent_total, SUM_AMOUNT_QUEUE)

    def start(self):
        try:
            logging.info("Word Reader waiting for messages...")
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
        WordReader().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0

if __name__ == "__main__":
    main()