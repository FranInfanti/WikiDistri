import os
import signal
import logging
import threading

from common import entity, message_protocol, middleware, rabbitmq

MOM_HOST = os.environ["MOM_HOST"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])

ID = int(os.environ["ID"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
FILTER_QUEUE = os.environ["FILTER_QUEUE"]
IMAGE_DOWNLOADER_QUEUE = os.environ["IMAGE_DOWNLOADER_QUEUE"] 

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

        self.publisher = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[FILTER_QUEUE, IMAGE_DOWNLOADER_QUEUE, f"{GATEWAY_PREFIX}_1"],
            exchanges={
                i: FILTER_PREFIX
                for i in range(FILTER_AMOUNT)
                if i != ID
            }
        )

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=FILTER_QUEUE
        )

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=MOM_HOST,
            exchange_name=FILTER_PREFIX,
            routing_keys=[f"{FILTER_PREFIX}_{ID}"]
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

    def _process_data(self, req_id: int, _: int, messages: list[list]):
        logging.info(f"Receive data with req_id: {req_id}")

        req_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())

        for message in messages:
            name, url, date_modified, image, in_language, sections = message
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
                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_1",
                    message=message_protocol.internal.serialize(
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
                self.publisher.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=1,
                        message=[image_downloader_message]
                    )
                )

                req_state.record_article_sent()

            req_state.record_article()

    def _process_eof(self, req_id: int, _: int, articles_sent: int):
        logging.info(f"Receive eof with req_id: {req_id}")
        
        req_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        req_state.record_eof(articles_sent)

        if FILTER_AMOUNT == 1:
            if req_state.all_articles_recv():
                logging.info("All articles have been filtered")
                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_1",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                self.publisher.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=req_state.articles_sent
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

        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                message=ID
            )
        )

    def _filter_messages(self, req_id: int, _: int, exchange_id: int):
        req_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())

        logging.info(f"Articles received: {req_state.articles_recv}")

        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                message=req_state.articles_recv,
            ),
            exchange_id=exchange_id
        )

    def _process_ok(self, req_id: int, _: int, articles_recv: int):
        req_state = self.requests_states.setdefault(req_id, entity.request_state.RequestState())
        req_state.record_peer_count(articles_recv)

        if req_state.all_peers(FILTER_AMOUNT):
            if req_state.all_articles_recv():
                logging.info("All articles have been filtered")

                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id
                    )
                )

                self.publisher.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_1",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                self.publisher.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=req_state.articles_sent
                    )
                )

                del self.requests_states[req_id]
            else:
                logging.info("There are still articles that have not been processed")
                req_state.reset_peers_count()
                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        message=ID
                    )
                )

    def _process_commit(self, req_id: int, *_):
        logging.info(f"Eliminating the client with req_id={req_id}")
        
        del self.requests_states[req_id]

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)

        try:         
            if opcode == message_protocol.OPCODE_DATA:
                self._process_data(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._process_eof(*args)
            elif opcode == message_protocol.OPCODE_PREPARE:
                self._filter_messages(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._process_ok(*args)
            elif opcode == message_protocol.OPCODE_COMMIT:
                self._process_commit(*args)

            ack()
        except Exception as e:
            logging.error(f"An error occurred {e}")
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
            self.publisher.stop()

            self.input_queue.close()
            self.input_exchange.close()

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