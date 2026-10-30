import os
import time
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

class Filter:
    _TIMEOUT = 5.0

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        
        self.publisher_downloader = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[IMAGE_DOWNLOADER_QUEUE],
        )
        self.publisher_gateway = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[f"{GATEWAY_PREFIX}_1"],
        )
        self.publisher_filter = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
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
        self.message_tracker = {}

    def _sigterm_handler(self, _signum, _frame):
        logging.info("SIGTERM signal received, setting shutdown flag")
        self.is_running = False
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

    def _wait_for_threads(self):
        deadline = time.time() + self._TIMEOUT

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            logging.warning("Failed to stop the input exchange consumer during shutdown", exc_info=True)

        logging.info(f"Waiting up to {self._TIMEOUT} seconds for one active thread to stop")
        if self.exchange_consumer:
            remaining = max(0.0, deadline - time.time())
            if remaining > 0:
                self.exchange_consumer.join(timeout=remaining)
            if self.exchange_consumer.is_alive():
                logging.warning(f"Shutdown timed out with still one active threads")
            self.input_exchange.close()

        finish = ( 
            self.publisher_downloader.stop(timeout=max(0.0, deadline - time.time())) or
            self.publisher_gateway.stop(timeout=max(0.0, deadline - time.time())) or
            self.publisher_filter.stop(timeout=max(0.0, deadline - time.time()))
        )
        if finish:
            logging.warning(f"Shutdown timed out with still active threads")
        else:
            logging.info("All active threads stopped")

    def _filter_message(self, req_id: int, _: int, messages: list[list]):
        logging.info(f"[{req_id}] - Receive articles")

        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
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
                logging.info(f"[{req_id}] - Valid article, sending to the Gateway")
                filter_message = wikipedia_article.to_list(url=True, date_modified=True)
                self.publisher_gateway.publish_queue(
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
                logging.info(f"[{req_id}] - Valid article, sending to download image")
                image_downloader_message = wikipedia_article.to_list(name=True, url=True, image=True)
                self.publisher_downloader.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=1,
                        message=[image_downloader_message]
                    )
                )
                tracker.record_sent()

            tracker.record_recv()

    def _process_eof(self, req_id: int, _: int, expected_messages: int):
        logging.info(f"[{req_id}] - Receive the end of the articles")
        
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        tracker.record_expected(expected_messages)

        if FILTER_AMOUNT == 1:
            if tracker.all_recv():
                logging.info(f"[{req_id}] - All articles have been received !!!")
                
                self.publisher_gateway.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_1",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                self.publisher_downloader.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=tracker.sent_messages
                    )
                )
            else:
                logging.error("[%s] - Article count mismatch: expected %s, received %s",
                    req_id,
                    tracker.expected_messages,
                    tracker.recv_messages
                )
            del self.message_tracker[req_id]
            return

        self.publisher_filter.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                batches=1,
                message=ID
            )
        )

    def _process_prepare(self, req_id: int, _: int, exchange_id: int):
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        logging.info(
            "[%s] - Send %s received and %s sent articles to filter_%s",
            req_id,
            tracker.recv_messages,
            tracker.sent_messages,
            exchange_id
        )

        self.publisher_filter.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                batches=1,
                message=[tracker.recv_messages, tracker.sent_messages],
            ),
            exchange_id=exchange_id
        )

    def _process_ok(self, req_id: int, _: int, messages: list):
        recv_messages, sent_messages = messages
        
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        tracker.record_peer(recv_messages, sent_messages)

        if tracker.all_peers(FILTER_AMOUNT - 1):
            if tracker.all_recv():
                logging.info(f"[{req_id}] - All articles have been received !!!")

                self.publisher_filter.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id
                    )
                )

                self.publisher_gateway.publish_queue(
                    queue=f"{GATEWAY_PREFIX}_1",
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id
                    )
                )

                self.publisher_downloader.publish_queue(
                    queue=IMAGE_DOWNLOADER_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_EOF,
                        req_id=req_id,
                        batches=1,
                        message=tracker.all_sent()
                    )
                )

                del self.message_tracker[req_id]
            else:
                logging.warning(f"[{req_id}] - There are still articles that have not been processed")
                tracker.reset_peers()
                self.publisher_filter.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        message=ID
                    )
                )

    def _process_commit(self, req_id: int, *_):
        logging.info(f"[{req_id}] - Eliminating the client")
        del self.message_tracker[req_id]

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        try:         
            if opcode == message_protocol.OPCODE_DATA:
                self._filter_message(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                self._process_eof(*args)
            elif opcode == message_protocol.OPCODE_PREPARE:
                self._process_prepare(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._process_ok(*args)
            elif opcode == message_protocol.OPCODE_COMMIT:
                self._process_commit(*args)
            ack()
        except Exception:
            logging.exception(f"An error occurred")
            nack()

    def start(self):
        try:
            self.exchange_consumer = threading.Thread(
                target=self.input_exchange.start_consuming,
                args=(self._process_message,)
            )
            self.exchange_consumer.start()

            logging.info("Waiting to receive messages")
            self.input_queue.start_consuming(self._process_message)
        except:
            if self.is_running:
                raise
            logging.warning("Interrupted by the SIGTERM")
        finally:
            self.input_queue.close()
            self._wait_for_threads()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )
    logging.getLogger("pika").propagate = False

    try:
        Filter().start()
    except Exception:
        logging.exception("An unexpected error has occurred")
        return 2

    return 0

if __name__ == "__main__":
    main()