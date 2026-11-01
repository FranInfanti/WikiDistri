import os
import time
import logging
import threading

from typing import Callable
from common import entity, middleware, rabbitmq, message_protocol

class RabbitCoordinator:
    _RETRY_DELAY = 1

    def __init__(
        self, 
        id: int,
        host: str,
        prefix: str,
        instances: int,
        on_all_recv: Callable,
        on_commit: Callable | None = None
    ):
        self.id = id
        self.host = host
        self.prefix = prefix
        self.instances = instances
        self.on_all_recv = on_all_recv
        self.on_commit = on_commit
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=self.host,
            exchange_name=self.prefix,
            routing_keys=[f"{self.prefix}_{self.id}"]
        )
        self.publisher = rabbitmq.RabbitPublisher(
            host=self.host,
            exchanges={
                i: self.prefix
                for i in range(self.instances)
                if i != self.id
            }
        )
        self.consumer = None
        self.message_tracker = {}

    def _process_prepare(self, req_id: int, _: int, id: int):
        logging.debug(f"Receive OPCODE_PREPARE message for req_id={req_id}")

        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())
        logging.info(
            "Sent %s received messages to instance_%s for req_id=%s",
            tracker.recv_messages,
            id,
            req_id,
        )

        logging.debug(f"Sending OPCODE_OK message for req_id={req_id}")
        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_OK,
                req_id=req_id,
                batches=1,
                message=[tracker.recv_messages, tracker.sent_by_key()],
            ),
            exchange_id=id
        )
        
    def _process_ok(self, req_id: int, _: int, messages: list):
        logging.debug(f"Receive OPCODE_OK message for req_id={req_id}")

        recv_messages, sent_messages = messages
        tracker = self.message_tracker.setdefault(req_id, entity.message_tracker.MessageTracker())

        tracker.record_peer(recv_messages=recv_messages)
        for key, sent in sent_messages:
            tracker.record_peer(sent_messages=sent, key=key)

        if tracker.all_peers(self.instances - 1):
            if tracker.all_recv():
                logging.info(f"All messages have been processed for req_id={req_id}")
                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_COMMIT,
                        req_id=req_id,
                        batches=1,
                        message=self.id
                    )
                )
                
                self._flush(req_id)
            else:
                logging.warning(f"There are still messages that have not been processed for req_id={req_id}")
                tracker.reset_peers()
                time.sleep(self._RETRY_DELAY)
                self.publisher.publish_exchange(
                    message=message_protocol.internal.serialize(
                        opcode=message_protocol.OPCODE_PREPARE,
                        req_id=req_id,
                        message=self.id
                    )
                )

    def _process_commit(self, req_id: int, _: int, id: int):
        logging.debug(f"Receive OPCODE_COMMIT message for req_id={req_id}")
        self._flush(req_id)
        self.message_tracker.pop(req_id, None) 
        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_DONE,
                req_id=req_id,
                batches=1,
                message=self.id,
            ),
            exchange_id=id,
        )
    
    def _process_done(self, req_id: int, *_):
        logging.debug(f"Receive OPCODE_DONE message for req_id={req_id}")
        tracker = self.message_tracker.get(req_id)
        if tracker is None:
            return
        tracker.record_done()
        if tracker.all_done(self.instances - 1):
            self.message_tracker.pop(req_id, None)
            self.on_all_recv(req_id, tracker.sent_by_key())

    def _flush(self, req_id: int):
        if self.on_commit:
            self.on_commit(req_id)

    def _process_message(self, message, ack, nack):
        opcode, *args = message_protocol.internal.deserialize(message)
        try:         
            if opcode == message_protocol.OPCODE_PREPARE:
                self._process_prepare(*args)
            elif opcode == message_protocol.OPCODE_OK:
                self._process_ok(*args)
            elif opcode == message_protocol.OPCODE_COMMIT:
                self._process_commit(*args)
            elif opcode == message_protocol.OPCODE_DONE:
                self._process_done(*args)
            else:
                logging.warning(f"Unexpected OPCODE={opcode} received")
            ack()
        except Exception:
            logging.exception("An error occured while processing a message")
            nack()

    def start(self):
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
        )
        logging.getLogger("pika").propagate = False
        
        self.consumer = threading.Thread(
            target=self.input_exchange.start_consuming,
            args=(self._process_message,),
            name="RabbitCoordinator"
        )
        self.consumer.start()

    def stop(self, timeout: float):
        deadline = time.time() + timeout
        stopped = False

        try:
            self.input_exchange.stop_consuming_threadsafe()
        except Exception:
            logging.warning("Failed to stop consumig from the exchange during shutdown")

        if self.consumer:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                logging.warning(f"Shutdown timed out with still active threads")
            
            self.consumer.join(timeout=remaining)
            stopped |= self.consumer.is_alive()

        remaining = max(0.0, deadline - time.time())
        if remaining <= 0:
            logging.warning(f"Shutdown timed out with still active threads")

        stopped |= self.publisher.stop(timeout=remaining)

        try:
            self.input_exchange.close()
        except Exception:
            logging.warning("Failed to close the exchange during shutdown")

        return stopped

    def on_eof(self, req_id: int, expected_messages: int):
        logging.debug(f"Receive OPCODE_EOF message for req_id={req_id}")

        tracker = self.message_tracker.setdefault(req_id, entity.MessageTracker())
        tracker.record_expected(expected_messages=expected_messages)

        # there is only one instance
        if self.instances == 1:
            if tracker.all_recv():
                logging.info(f"All messages have been processed for req_id={req_id}")
                self._flush(req_id)
                self.message_tracker.pop(req_id, None)
                self.on_all_recv(req_id, tracker.sent_by_key())
                return
            else:
                logging.critical(f"Message count mismatch for req_id=%s; Expected %s, received %s",
                    req_id,
                    tracker.expected_messages,
                    tracker.recv_messages
                )
                self.message_tracker.pop(req_id, None)
                return

        # i'm the coordinator, so the messages go back to me
        logging.debug(f"Sending OPCODE_PREPARE message for req_id={req_id}")
        self.publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                batches=1,
                message=self.id
            )
        )

    def record_recv(self, req_id: int):
        self.message_tracker.setdefault(req_id, entity.MessageTracker()).record_recv()

    def record_sent(self, who: str, req_id: int):
        self.message_tracker.setdefault(req_id, entity.MessageTracker()).record_sent(key=who)
