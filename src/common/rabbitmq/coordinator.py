import os
import time
import logging
import threading

from typing import Callable
from common import middleware, rabbitmq, message_protocol

COMMITTING_PHASE = "COMMITTING"
COMMITED_PHASE = "COMMITTED"
FINISHED_PHASE = "FINISHED"

class RabbitCoordinator:
    _RETRY_DELAY = 1
    _TIMEOUT = 10.0

    def __init__(
        self,
        id: int,
        host: str,
        prefix: str,
        instances: int,
        state_path: str | None = None,
        on_commit: Callable | None = None,
        on_all_recv: Callable | None = None,
        *,
        get_tracker: Callable,
    ):
        self.id = id
        self.host = host
        self.prefix = prefix
        self.instances = instances
        self.on_commit = on_commit
        self.on_all_recv = on_all_recv
        self.get_tracker = get_tracker
        
        self._input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=self.host,
            exchange_name=self.prefix,
            routing_keys=[f"{self.prefix}_{self.id}"]
        )
        self._publisher = rabbitmq.RabbitPublisher(
            host=self.host,
            exchanges={
                i: self.prefix
                for i in range(self.instances)
                if i != self.id
            }
        )
        
        self._consumer = None
        self._phase_manager = rabbitmq.PhaseManager(state_path or f"{self.prefix}_{self.id}_state.json")

        self._locks: dict[int, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        
        self._timers: dict[tuple, threading.Timer] = {}
        self._timers_lock = threading.Lock()

        self.__post_init__()

    def __post_init__(self):
        for req_id in self._phase_manager.pending(COMMITTING_PHASE):
            logging.info(f"Resuming COMMIT for req_id={req_id} after restart")
            self._start_commit(req_id)

    def _req_lock(self, req_id: int):
        with self._locks_guard:
            return self._locks.setdefault(req_id, threading.RLock())

    def _tracker(self, req_id: int):
        return self.get_tracker(req_id)

    def _on_commit(self, req_id: int):
        if self.on_commit:
            self.on_commit(req_id)

    def _on_all_recv(self, req_id: int, sent_by_key: list):
        if self.on_all_recv:
            self.on_all_recv(req_id, sent_by_key)


    def _schedule(self, kind: str, fn: Callable, req_id: int):
        key = (kind, req_id)

        def run():
            with self._timers_lock:
                if self._timers.get(key) is timer:
                    del self._timers[key]
            try:
                fn(req_id)
            except Exception:
                logging.exception(f"Timer {kind} failed for req_id={req_id}")

        timer = threading.Timer(self._TIMEOUT, run)
        timer.daemon = True
        with self._timers_lock:
            old = self._timers.get(key)
            if old:
                old.cancel()
            self._timers[key] = timer
        timer.start()

    def _cancel(self, kind: str, req_id: int):
        with self._timers_lock:
            timer = self._timers.pop((kind, req_id), None)
        if timer:
            timer.cancel()


    def _publish_prepare(self, req_id: int):
        self._publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_PREPARE,
                req_id=req_id,
                batches=1,
                message=self.id
            )
        )

    def _publish_commit(self, req_id: int):
        self._publisher.publish_exchange(
            message=message_protocol.internal.serialize(
                opcode=message_protocol.OPCODE_COMMIT,
                req_id=req_id,
                batches=1,
                message=self.id
            )
        )


    def _start_commit(self, req_id: int):
        with self._req_lock(req_id):
            self._on_commit(req_id)
            self._publish_commit(req_id)
            self._schedule("commit", self._retry_commit, req_id)

    def _finish(self, req_id: int, result: list):
        self._on_all_recv(req_id, [tuple(kv) for kv in result])
        self._phase_manager.put(req_id, {"stage": FINISHED_PHASE})
        self._cancel("prepare", req_id)
        self._cancel("commit", req_id)


    def _retry_prepare(self, req_id: int):
        with self._req_lock(req_id):
            logging.debug(f"Timer to start COMMIT for req_id={req_id} expired")

            rec = self._phase_manager.get(req_id)
            if rec.get("stage") in (COMMITTING_PHASE, FINISHED_PHASE):
                logging.debug(f"The COMMIT phase has already started for req_id={req_id}; ignore...")
                return

            logging.warning(f"There are still messages that have not been processed for req_id={req_id}")
            self._tracker(req_id).reset_peers()
            self._publish_prepare(req_id)
            self._schedule("prepare", self._retry_prepare, req_id)

    def _retry_commit(self, req_id: int):
        with self._req_lock(req_id):
            logging.debug(f"Timer to finish COMMIT for req_id={req_id} expired")

            rec = self._phase_manager.get(req_id)
            if rec.get("stage") != COMMITTING_PHASE:
                logging.debug(f"The COMMIT phase is not pending for req_id={req_id}; ignore...")
                return

            logging.warning(f"There are still processes that have not done a COMMIT for req_id={req_id}")
            self._publish_commit(req_id)
            self._schedule("commit", self._retry_commit, req_id)


    def on_eof(self, req_id: int, expected_messages: int):
        with self._req_lock(req_id):
            logging.debug(f"Receive OPCODE_EOF message for req_id={req_id}")

            stage = self._phase_manager.get(req_id).get("stage")
            if stage == FINISHED_PHASE:
                logging.info(f"Duplicate EOF for finished req_id={req_id}; ignoring...")
                return True
            if stage == COMMITTING_PHASE:
                logging.info(f"Resuming COMMIT for req_id={req_id} after restart")
                self._start_commit(req_id)
                return

            tracker = self._tracker(req_id)
            tracker.record_expected(expected_messages)

            if self.instances == 1:
                if tracker.all_recv():
                    logging.info(f"All messages have been processed for req_id={req_id}")
                    self._on_commit(req_id)
                    self._on_all_recv(req_id, tracker.sent_by_key())
                else:
                    logging.critical(
                        "Message count mismatch for req_id=%s; Expected %s, received %s",
                        req_id,
                        tracker.expected_messages,
                        tracker.recv_messages
                    )
                self._phase_manager.put(req_id, {"stage": FINISHED_PHASE})
                return

            logging.debug(f"Sending OPCODE_PREPARE message for req_id={req_id}")
            self._publish_prepare(req_id)
            self._schedule("prepare", self._retry_prepare, req_id)


    def _process_prepare(self, req_id: int, _: int, id: int):
        with self._req_lock(req_id):
            logging.debug(f"Receive OPCODE_PREPARE message for req_id={req_id}")

            tracker = self._tracker(req_id)
            logging.info(
                "Sent %s received messages to instance_%s for req_id=%s",
                tracker.recv_messages,
                id,
                req_id,
            )

            logging.debug(f"Sending OPCODE_OK message for req_id={req_id}")
            self._publisher.publish_exchange(
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_OK,
                    req_id=req_id,
                    batches=1,
                    message=[tracker.recv_messages, tracker.sent_by_key()],
                ),
                exchange_id=id
            )

    def _process_ok(self, req_id: int, _: int, messages: list):
        with self._req_lock(req_id):
            logging.debug(f"Receive OPCODE_OK message for req_id={req_id}")

            if self._phase_manager.get(req_id).get("stage") in (COMMITTING_PHASE, FINISHED_PHASE):
                logging.debug(f"Ignoring stale OK for req_id={req_id}")
                return

            recv_messages, sent_messages = messages
            
            tracker = self._tracker(req_id)
            tracker.record_peer(recv_messages)
            for key, sent in sent_messages:
                tracker.record_peer(sent_messages=sent, key=key)

            if not tracker.all_peers(self.instances - 1):
                return

            if tracker.all_recv():
                logging.info(f"All messages have been processed for req_id={req_id}")
                self._phase_manager.update(
                    req_id,
                    stage=COMMITTING_PHASE,
                    result=[list(kv) for kv in tracker.sent_by_key()],
                )
                self._cancel("prepare", req_id)
                self._start_commit(req_id)
            else:
                self._schedule("prepare", self._retry_prepare, req_id)

    def _process_commit(self, req_id: int, _: int, coordinator_id: int):
        with self._req_lock(req_id):
            logging.debug(f"Receive OPCODE_COMMIT message for req_id={req_id}")

            if self._phase_manager.get(req_id).get("stage") != COMMITED_PHASE:
                self._on_commit(req_id)
                self._phase_manager.put(req_id, {"stage": COMMITED_PHASE})

            self._publisher.publish_exchange(
                message=message_protocol.internal.serialize(
                    opcode=message_protocol.OPCODE_DONE,
                    req_id=req_id,
                    batches=1,
                    message=self.id,
                ),
                exchange_id=coordinator_id,
            )

    def _process_done(self, req_id: int, _: int, peer_id: int):
        with self._req_lock(req_id):
            logging.debug(f"Receive OPCODE_DONE message for req_id={req_id}")

            rec = self._phase_manager.get(req_id)
            if rec.get("stage") != COMMITTING_PHASE:
                logging.debug(f"Ignoring stale/duplicate DONE for req_id={req_id}")
                return

            tracker = self._tracker(req_id)
            tracker.record_done(peer_id)
            if tracker.all_done(self.instances - 1):
                self._finish(req_id, rec["result"])

    def _process_message(self, message, ack, nack):
        opcode, _, *args = message_protocol.internal.deserialize(message)
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

        self._consumer = threading.Thread(
            target=self._input_exchange.start_consuming,
            args=(self._process_message,),
            name="RabbitCoordinator"
        )
        self._consumer.start()

    def stop(self, timeout: float):
        deadline = time.time() + timeout
        stopped = False

        with self._timers_lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()

        try:
            self._input_exchange.stop_consuming_threadsafe()
        except Exception:
            logging.warning("Failed to stop consumig from the exchange during shutdown")

        if self._consumer:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                logging.warning("Shutdown timed out with still active threads")

            self._consumer.join(timeout=remaining)
            stopped |= self._consumer.is_alive()

        remaining = max(0.0, deadline - time.time())
        if remaining <= 0:
            logging.warning("Shutdown timed out with still active threads")

        stopped |= self._publisher.stop(timeout=remaining)

        try:
            self._input_exchange.close()
        except Exception:
            logging.warning("Failed to close the exchange during shutdown")

        return stopped