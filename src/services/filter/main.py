import os
import time
import signal
import logging

from common import entity, message_protocol, middleware, rabbitmq

MOM_HOST = os.environ["MOM_HOST"]

ID = int(os.environ["ID"])
FILTER_QUEUE = os.environ["FILTER_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
IMAGE_DOWNLOADER_QUEUE = os.environ["IMAGE_DOWNLOADER_QUEUE"] 

class Filter:
    _TIMEOUT = 10.0
    _GATEWAY_QUEUE = f"{GATEWAY_PREFIX}_1"

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        
        self.is_running = True
        self.publisher_downloader = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[IMAGE_DOWNLOADER_QUEUE],
        )
        self.publisher_gateway = rabbitmq.RabbitPublisher(
            host=MOM_HOST,
            queues=[self._GATEWAY_QUEUE],
        )
        self.coordinator = rabbitmq.RabbitCoordinator(
            id=ID,
            host=MOM_HOST,
            prefix=FILTER_PREFIX,
            instances=FILTER_AMOUNT,
            on_all_recv=self._on_recv_all
        )
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=FILTER_QUEUE
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
        logging.info(f"Waiting up to {self._TIMEOUT} seconds for active threads to stop")

        timed_out = []
        components = (
            ("coordinator", self.coordinator.stop),
            ("downloader publisher", self.publisher_downloader.stop),
            ("gateway publisher", self.publisher_gateway.stop),
        )
        for name, stop in components:
            if stop(timeout=max(0.0, deadline - time.monotonic())):
                timed_out.append(name)

        try:
            self.input_queue.close()
        except Exception:
            logging.warning("An error occurred while attempting to close the input queue")

        if timed_out:
            logging.critical("Shutdown timed out with active components: %s", ", ".join(timed_out))
        else:
            logging.info("All resources have been released; goodbye")

    def _filter_message(self, req_id: int, _: int, messages: list[list]):
        logging.debug(f"Received an OPCODE_DATA message for req_id={req_id}")
        
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
                logging.debug(f"Sending OPCODE_DATA message to {self._GATEWAY_QUEUE} for req_id={req_id}")
                
                filter_message = wikipedia_article.to_list(url=True, date_modified=True)
                self.publisher_gateway.publish_queue(
                    queue=self._GATEWAY_QUEUE,
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_DATA,
                        req_id=req_id,
                        batches=1,
                        message=[filter_message]
                    )
                )
                
            if wikipedia_article <= 2020 and (
                wikipedia_article.section_contains("biography") or wikipedia_article.section_contains("biographie")
            ):
                
                logging.debug(f"Sending OPCODE_DATA message to {IMAGE_DOWNLOADER_QUEUE} for req_id={req_id}")
                
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
                self.coordinator.record_sent(who=IMAGE_DOWNLOADER_QUEUE, req_id=req_id)

            self.coordinator.record_recv(req_id=req_id)

    def _on_recv_all(self, req_id: int, sent_messages: list):
        logging.debug(f"Sending OPCODE_EOF message to {self._GATEWAY_QUEUE} for req_id={req_id}")
        self.publisher_gateway.publish_queue(
            queue=self._GATEWAY_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id
            )
        )

        if len(sent_messages) == 0:
            key, sent = IMAGE_DOWNLOADER_QUEUE, 0
        else:
            key, sent = sent_messages[0]

        if key != IMAGE_DOWNLOADER_QUEUE:
            raise ValueError(f"Unknown key; expected: {IMAGE_DOWNLOADER_QUEUE}, received: {key}")

        logging.debug(f"Sending OPCODE_EOF message to {IMAGE_DOWNLOADER_QUEUE} for req_id={req_id}")
        self.publisher_downloader.publish_queue(
            queue=IMAGE_DOWNLOADER_QUEUE,
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_EOF,
                req_id=req_id,
                batches=1,
                message=sent
            )
        )

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        try:
            if opcode == message_protocol.OPCODE_DATA:
                self._filter_message(*args)
            elif opcode == message_protocol.OPCODE_EOF:
                req_id, _, expected_messages = args
                self.coordinator.on_eof(req_id=req_id, expected_messages=expected_messages)
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
            self.input_queue.start_consuming(self._process_message)
        except:
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
        Filter().start()
    except Exception:
        return 2
    return 0

if __name__ == "__main__":
    SystemExit(main())