import os
import re
import signal
import logging
import threading

from datetime import datetime
from stopwords import STOPWORDS
from common import middleware, message_protocol, hashing, eof


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

        self.send_lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=INPUT_QUEUE
        )
        self.sum_words_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(
                host=MOM_HOST, queue_name=f"{SUM_WORDS_QUEUE_PREFIX}_{i}"
            )
            for i in range(SUM_WORDS_COUNT)
        ]
        self.join_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=JOIN_QUEUE
        )
        self.sum_amount_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, queue_name=SUM_AMOUNT_QUEUE
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
            logging.info(f"req_id= {message}")        
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message) # Despues a ver si funcionaria con batches
 
            if opcode == message_protocol.OPCODE_DATA:
                self._handle_articles(req_id, payload)
                self.barrier.record_articles(req_id, len(payload))
                logging.debug("req_id=%s: processed %s articles", req_id, len(payload))
                # process_message, rama DATA, después de procesar el batch
                
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
        # shard -> {word: [en_count, fr_count]}
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


    @staticmethod
    def _collect_unique_words(req_id, words, language, words_by_shard):
        counts = language_counts(language)
        if counts is None:
            return
 
        en_count, fr_count = counts
        for word in set(words):
            shard_words = words_by_shard.setdefault(hashing.shard_for(req_id, word), {})
            entry = shard_words.setdefault(word, [0, 0])
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

    def _send_unique_words(self, req_id: int, words_by_shard: dict):
        with self.send_lock:
            for shard, shard_words in words_by_shard.items():
                message = [[word, en, fr] for word, (en, fr) in shard_words.items()]
                self.sum_words_queues[shard].send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=len(message),
                        message=message,
                    )
                )



    def _send_word_amounts(self, req_id: int, join_batch: list, amount_batch: list):
        with self.send_lock:
            if join_batch:
                self.join_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=len(join_batch),
                        message=join_batch,
                    )
                )
                logging.debug("req_id=%s -> %s: %s articles", req_id, JOIN_QUEUE, len(join_batch))
 
            if amount_batch:
                self.sum_amount_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=len(amount_batch),
                        message=amount_batch,
                    )
                )
                logging.debug("req_id=%s -> %s: %s articles", req_id, SUM_AMOUNT_QUEUE, len(amount_batch))


    def _forward_eof(self, req_id: int):
        logging.info(f"All articles received for req_id: {req_id}. Forwarding EOF to next stages...")
 
        # TODO: define the EOF payload for sum and job 5 
        eof = message_protocol.internal.serialize(
            opcode=message_protocol.OPCODE_EOF,
            req_id=req_id,
        )
 
        with self.send_lock:
            for i, queue in enumerate(self.sum_words_queues):
                queue.send(eof)
                logging.info("req_id=%s: EOF -> %s_%s", req_id, SUM_WORDS_QUEUE_PREFIX, i)
            self.join_queue.send(eof)
            logging.info("req_id=%s: EOF -> %s", req_id, JOIN_QUEUE)
            self.sum_amount_queue.send(eof)
            logging.info("req_id=%s: EOF -> %s", req_id, SUM_AMOUNT_QUEUE)

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
 
        queues = [self.input_queue, self.join_queue, self.sum_amount_queue, *self.sum_words_queues]
        for queue in queues:
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
        WordReader().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2
    return 0

if __name__ == "__main__":
    main()