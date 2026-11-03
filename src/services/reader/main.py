import os
import re
import time
import signal
import logging

from datetime import datetime
from stopwords import STOPWORDS
from common import middleware, message_protocol, hashing, rabbitmq

ID = int(os.environ["ID"])
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
WORD_READER_INSTANCES = int(os.environ.get("WORD_READER_INSTANCES", 1))
WORD_READER_PREFIX = os.environ.get("WORD_READER_PREFIX", "word_reader")
WORD_READER_QUEUE = os.environ.get("WORD_READER_QUEUE", "word_reader_queue")
SUM_COUNT_INSTANCES = int(os.environ.get("SUM_COUNT_INSTANCES", 1))
SUM_COUNT_QUEUE = os.environ.get("SUM_COUNT_QUEUE", "sum_count_queue")
SUM_AMOUNT_QUEUE = os.environ.get("SUM_AMOUNT_QUEUE", "sum_amount_queue")
JOIN_AVERAGE_INSTANCES = int(os.environ.get("JOIN_AVERAGE_INSTANCES", 1))
JOIN_AVERAGE_QUEUE = os.environ.get("JOIN_AVERAGE_QUEUE", "join_average_queue")

_CUTOFF_YEAR = 2021
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

def tokenize(abstract: str, language: str | None) -> list[str]:
    stopwords = STOPWORDS.get(language or "", set())
    return [w for w in _WORD_RE.findall(abstract.lower()) if w not in stopwords]

def language_counts(language: str | None) -> tuple[int, int] | None:
    if language == "en":
        return 1, 0
    if language == "fr":
        return 0, 1
    return None

class WordReader:
    _TIMEOUT = 5.0

    @staticmethod
    def _collect_unique_words(req_id: int, words: list, language: str | None, words_by_shard: dict):
        counts = language_counts(language)
        if counts is None:
            return

        en_count, fr_count = counts
        for word in set(words):
            shard = hashing.shard_for(SUM_COUNT_INSTANCES, req_id, word)
            entry = words_by_shard.setdefault(shard, {}).setdefault(word, [0, 0])
            entry[0] += en_count
            entry[1] += fr_count

    @staticmethod
    def _collect_word_count(req_id: int, url: str, amount: int, date_modified: str | None, join_batches: dict, amount_batch: list):
        if date_modified is None:
            logging.warning(f"The article {url} does not have a date_modified field. Skipping it...")
            return
 
        year = datetime.strptime(date_modified, _DATE_FORMAT).year
        if year >= _CUTOFF_YEAR:
            shard = hashing.shard_for(JOIN_AVERAGE_INSTANCES, req_id, url)
            join_batches.setdefault(shard, []).append([url, amount])
        else:
            amount_batch.append([amount])

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
 
        self.is_running = True
        self.sum_count_queue_names = [f"{SUM_COUNT_QUEUE}_{i}" for i in range(SUM_COUNT_INSTANCES)]
        self.join_average_queue_names = [f"{JOIN_AVERAGE_QUEUE}_{i}" for i in range(JOIN_AVERAGE_INSTANCES)]
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST, 
            queue_name=WORD_READER_QUEUE
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[*self.sum_count_queue_names, *self.join_average_queue_names, SUM_AMOUNT_QUEUE],
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=WORD_READER_PREFIX,
            instances=WORD_READER_INSTANCES,
            on_all_recv=self._on_recv_all
        )

    def _sigterm_handler(self, _signum, _frame):
        logging.info("Shutdown signal received, stopping consumer...")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("Error stopping the consumer")

    def _close(self):
        deadline = time.monotonic() + self._TIMEOUT
        logging.info("Waiting up to %s seconds for active threads to stop", self._TIMEOUT)

        timed_out = []
        try:
            if self.coordinator.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("coordinator")
        except Exception:
            logging.warning("Error stopping the coordinator")

        try:
            if self.publisher.stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append("publisher")
        except Exception:
            logging.warning("Error stopping the publisher")

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("Error closing the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _handle_articles(self, req_id: int, articles: list[list]):
        words_by_shard: dict[int, dict[str, list[int]]] = {}
        join_batches: dict[int, list] = {}
        amount_batch = []

        for article in articles:
            abstract, url, date_modified, in_language = article
            words = tokenize(abstract or "", in_language)
 
            self._collect_unique_words(req_id, words, in_language, words_by_shard)
            self._collect_word_count(req_id, url, len(words), date_modified, join_batches, amount_batch)
 
        self._send_unique_words(req_id, words_by_shard)
        self._send_word_count(req_id, join_batches, amount_batch)

        for _ in articles:
            # Por que aca afuera XD
            self.coordinator.record_recv(req_id=req_id)

    def _send_unique_words(self, req_id: int, words_by_shard: dict):
        for shard, shard_words in words_by_shard.items():
            message = [[word, en, fr] for word, (en, fr) in shard_words.items()]
            logging.debug(f"Send OPCODE_DATA message to {self.sum_count_queue_names[shard]} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=self.sum_count_queue_names[shard],
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, 
                    batches=len(message), 
                    message=message,
                ),
            )
            self.coordinator.record_sent(who=SUM_COUNT_QUEUE, req_id=req_id)

    def _send_word_count(self, req_id: int, join_batches: dict, amount_batch: list):
        for shard, batch in join_batches.items():
            logging.debug(f"Send OPCODE_DATA message to {self.join_average_queue_names[shard]} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=self.join_average_queue_names[shard],
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, 
                    batches=len(batch), 
                    message=batch,
                ),
            )
            self.coordinator.record_sent(who=JOIN_AVERAGE_QUEUE, req_id=req_id) 

        if amount_batch:
            logging.debug(f"Send OPCODE_DATA message to {SUM_AMOUNT_QUEUE} for req_id={req_id}")
            self.publisher.publish_queue(
                queue=SUM_AMOUNT_QUEUE,
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id, batches=len(amount_batch), message=amount_batch,
                ),
            )
            self.coordinator.record_sent(who=SUM_AMOUNT_QUEUE, req_id=req_id)

    def _on_recv_all(self, req_id: int, sent_messages: list):
        logging.info(f"All articles have been received for req_id={req_id}; forwarding EOF...")

        for key, sent in sent_messages:
            if key == SUM_COUNT_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {self.sum_count_queue_names[0]} for req_id={req_id}")
                self.publisher.publish_queue(
                    queue=self.sum_count_queue_names[0],
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            elif key == JOIN_AVERAGE_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {self.join_average_queue_names[0]} for req_id={req_id}")
                self.publisher.publish_queue(
                    queue=self.join_average_queue_names[0],
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            elif key == SUM_AMOUNT_QUEUE:
                logging.debug(f"Send OPCODE_EOF message to {SUM_AMOUNT_QUEUE} for req_id={req_id}")
                self.publisher.publish_queue(
                    queue=SUM_AMOUNT_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=sent,
                    ),
                )
            else:
                raise ValueError(f"Unknown key; received: {key}")
            
    def process_message(self, message, ack, nack):
        try:
            opcode, req_id, _batches, payload = message_protocol.internal.deserialize(message)

            if opcode == message_protocol.OPCODE_DATA:
                logging.debug(f"Received OPCODE_DATA message for req_id={req_id}")
                self._handle_articles(req_id, payload)
                logging.debug("req_id=%s: processed %s articles", req_id, len(payload))
            elif opcode == message_protocol.OPCODE_EOF:
                logging.debug(f"Received OPCODE_EOF message for req_id: {req_id}; expected messages: {payload}")
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
        WordReader().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())