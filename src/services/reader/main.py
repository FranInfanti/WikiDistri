import os
import re
import logging
import hashlib
from datetime import datetime

from common import middleware
from common import message_protocol

from stopwords import STOPWORDS

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")

INPUT_QUEUE = os.environ["WORD_READER_OUTPUT_QUEUE"]

SUM_WORDS_QUEUE_PREFIX = os.environ["SUM_WORDS_QUEUE_PREFIX"]
SUM_WORDS_COUNT = int(os.environ.get("SUM_WORDS_COUNT", 1))

JOIN_QUEUE = os.environ["JOIN_QUEUE"]
SUM_AMOUNT_QUEUE = os.environ["SUM_AMOUNT_QUEUE"]

CUTOFF_YEAR = 2021
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
WORD_RE = re.compile(r"\w+", re.UNICODE)

DEFAULT_REQ_ID = 0


def tokenize(abstract: str, language: str | None) -> list[str]:
    stopwords = STOPWORDS.get(language or "", set())
    return [w for w in WORD_RE.findall(abstract.lower()) if w not in stopwords]


def shard_for(req_id: int, word: str) -> int:
    digest = hashlib.md5(f"{req_id}:{word}".encode("utf-8")).hexdigest()
    return int(digest, 16) % SUM_WORDS_COUNT


class WordReader:
    def __init__(self):
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

    def _on_message(self, message: bytes, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)

            if isinstance(fields[0], int):
                self._handle_control(fields)
            else:
                self._handle_article(fields)
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def _handle_control(self, fields):
        opcode = fields[0]
        if opcode == message_protocol.OPCODE_EOF:
            logging.info("Received EOF. Forwarding to next stages...")
            self._forward_eof(DEFAULT_REQ_ID)
        else:
            logging.warning(f"Unexpected opcode: {opcode}")

    def _handle_article(self, fields):
        abstract, date_modified, _image, in_language, _name, _sections, url = fields
        req_id = DEFAULT_REQ_ID

        words = tokenize(abstract or "", in_language)

        self._send_unique_words(req_id, words, in_language)
        self._send_word_amount(req_id, url, len(words), date_modified)

    def _send_unique_words(self, req_id: int, words: list[str], language: str | None):
        if language == "en":
            en_count, fr_count = 1, 0
        elif language == "fr":
            en_count, fr_count = 0, 1
        else:
            return

        for word in set(words):
            msg = message_protocol.internal.serialize([req_id, word, en_count, fr_count])
            self.sum_words_queues[shard_for(req_id, word)].send(msg)


    def _send_word_amount(self, req_id: int, url: str, amount: int, date_modified: str | None):
        if date_modified is None:
            logging.warning(f"Article without date_modified: {url}")
            return

        year = datetime.strptime(date_modified, DATE_FORMAT).year
        if year >= CUTOFF_YEAR:
            self.join_queue.send(message_protocol.internal.serialize([req_id, url, amount]))
        else:
            self.sum_amount_queue.send(message_protocol.internal.serialize([req_id, amount]))

    def _forward_eof(self, req_id: int):
        eof = message_protocol.internal.serialize([message_protocol.OPCODE_EOF, req_id, ""])
        for queue in self.sum_words_queues:
            queue.send(eof)
        self.join_queue.send(eof)
        self.sum_amount_queue.send(eof)

    def start(self):
        logging.info("Word Reader waiting for messages...")
        self.input_queue.start_consuming(self._on_message)


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        WordReader().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        raise


if __name__ == "__main__":
    main()