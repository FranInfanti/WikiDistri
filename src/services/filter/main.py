import os
import signal
import logging
import threading
import request_state

from common import entity, message_protocol, middleware

MOM_HOST = os.environ["MOM_HOST"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])

ID = int(os.environ["ID"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_INPUT_QUEUE = os.environ["FILTER_INPUT_QUEUE"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
IMAGE_DOWNLOADER_OUTPUT_QUEUE = os.environ["IMAGE_DOWNLOADER_OUTPUT_QUEUE"] 

def define_exchanges(host, prefix, amount, exclude_id = None):
    exchanges = {}

    for i in range(amount):
        if i == exclude_id:
            continue

        exchanges[i] = middleware.MessageMiddlewareExchangeRabbitMQ(
            host, prefix, [f"{prefix}_{i}"]
        )

    return exchanges

class Filter:

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=FILTER_INPUT_QUEUE
        )

        self.gateway_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=f"{GATEWAY_PREFIX}_1"
        )

        self.image_downloader_ouput_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=IMAGE_DOWNLOADER_OUTPUT_QUEUE
        )

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=MOM_HOST,
            exchange_name=FILTER_PREFIX,
            routing_keys=f"{FILTER_PREFIX}_{ID}"
        )

        self.output_exchange = define_exchanges(
            host=MOM_HOST,
            prefix=FILTER_PREFIX,
            amount=FILTER_AMOUNT,
            exclude_id=ID
        )

        self.is_running = True
        self.exchange_consumer = None
        self.requests_states = {}

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag")
        self.is_running = False

        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            pass

    def _publish_message(self, message: bytes, filter_id: int = None):
        exchanges = [self.output_exchange[filter_id]] \
            if filter_id is not None else self.output_exchange
        
        for exchange in exchanges:
            exchange.send(message)

    def _process_data(self, req_id: int, _: int, message: list[list]):
        logging.info(f"Receive data with req_id: {req_id}")

        name, url, date_modified, image, in_language, sections = message[0]
        wikipedia_article = entity.WikipediaArticle(
            name=name,
            url=url,
            date_modified=date_modified,
            image=image,
            in_language=in_language,
            sections=sections
        )

        if wikipedia_article.is_language("en") and wikipedia_article <= 2020:
            logging.info(f"Sending result with req_id: {req_id}")

            filter_message = wikipedia_article.to_list(url=True, date_modified=True)
            self.gateway_output_queue.send(
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[filter_message]
                )
            )

        if wikipedia_article < 2020 and (
            wikipedia_article.section_contains("biography") or wikipedia_article.section_contains("biographie")
        ):
            logging.info(f"Sending to download image with req_id: {req_id}")

            image_downloader_message = wikipedia_article.to_list(name=True, url=True, image=True)
            self.image_downloader_ouput_queue.send(
                message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DATA,
                    req_id=req_id,
                    batches=1,
                    message=[image_downloader_message]
                )
            )

        self.requests_states.setdefault(req_id, request_state.RequestState()).record_article()

    def _process_eof(self, req_id: int, _: int, articles_sent: int):
        req_state = self.requests_states.setdefault(req_id, request_state.RequestState())
        req_state.record_eof(articles_sent)

        if FILTER_AMOUNT == 1:
            if req_state.all_articles_recv():
                self.gateway_output_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )
            else:
                logging.error(
                    "Article count mismatch for req_id %s: expected %s, received %s",
                    req_id,
                    req_state.expected_articles,
                    req_state.articles_recv
                )

            del self.requests_states[req_id]
            return

        self._publish_message(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                message=ID
            )
        )

    def _process_prepare(self, req_id: int, _: int, filter_id: int):
        req_state = self.requests_states.setdefault(req_id, request_state.RequestState())

        self._publish_message(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                message=req_state.articles_recv,
            ),
            filter_id=filter_id
        )

    def _process_ok(self, req_id: int, _: int, articles_recv: int):
        req_state = self.requests_states.setdefault(req_id, request_state.RequestState())
        req_state.record_peer_count(articles_recv)

        if req_state.all_peers(FILTER_AMOUNT):
            if req_state.all_articles_recv():

                self._publish_message(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id
                    )
                )

                self.gateway_output_queue.send(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                del self.requests_states[req_id]
            else:
                req_state.reset_peers_count()
                self._publish_message(
                    message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        message=ID
                    )
                )

    def _process_commit(self, req_id: int, *_):
        del self.requests_states[req_id]

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)

        try:         
            if opcode == message_protocol.OPCODE_DATA:
                self._process_data(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._process_eof(*args)
            elif opcode == message_protocol.OPCODE_PREPARE:
                self._process_prepare(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._process_ok(*args)
            elif opcode == message_protocol.OPCODE_COMMIT:
                self._process_commit(*args)

            ack()
        except:
            nack()

    def start(self):
        try:
            self.exchange_consumer = threading.Thread(
                target=self.input_exchange.start_consuming,
                args=(self._process_message,)
            )
            self.exchange_consumer.start()

            self.input_queue.start_consuming(self._process_message)
        except:
            if self.is_running:
                raise
        finally:
            self.input_queue.close()
            self.input_exchange.close()
            self.gateway_output_queue.close()
            self.image_downloader_ouput_queue.close()

            for output_exchange in self.output_exchange:
                output_exchange.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )

    try:
        Filter().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2

    return 0

if __name__ == "__main__":
    main()