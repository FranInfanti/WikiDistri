import time
import logging
import threading

from typing import Callable
from dataclasses import dataclass, field
from common import message_protocol, middleware, rabbitmq

RETRY_DELAY = 0.5

@dataclass
class _RequestState:
    expected_articles: int | None = None
    articles_recv: int = 0
    articles_sent: int = 0
    peer_counts: dict[int, tuple[int, int]] = field(default_factory=dict)
    retry_scheduled: bool = False

class EofBarrier:
    """
    Coordinates the EOF of a request between N nodes of the same stage
    using the PREPARE / OK / COMMIT protocol.

    The node that receives the EOF from the shared input queue asks its
    peers how many articles each one received (`recv`) and sent (`sent`).
    If the sum of `recv` matches the expected amount, it commits and
    `on_all_received(req_id, sent_total)` is called ONLY in that node, with
    the sum of `sent` of every replica. The rest of the replicas are
    notified through `on_commit(req_id)` (optional).
    Otherwise it waits `retry_delay` seconds and asks again.

    Usage:
        - call `record_articles(req_id, recv, sent)` AFTER finishing the
          processing (and the sending) of a data message, so a counted
          article is a fully forwarded article.
        - call `on_eof` when the EOF arrives from the input queue.
        - call `start` to begin consuming control messages and `close`
          at the end.
    """

    def __init__(
        self,
        host: str,
        prefix: str,
        instance_id: int,
        instances: int,
        on_all_received: Callable[[int, int], None],
        on_commit: Callable[[int], None] | None = None,
        retry_delay: float = RETRY_DELAY,
    ):
        self.instance_id = instance_id
        self.instances = instances
        self.on_all_received = on_all_received
        self.on_commit = on_commit
        self.retry_delay = retry_delay

        self._peer_ids = [i for i in range(instances) if i != instance_id]
        self._states: dict[int, _RequestState] = {}
        self._lock = threading.RLock()
        self._timers: set[threading.Timer] = set()
        self._closed = False
        self._consumer: threading.Thread | None = None

        self._input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=host,
            exchange_name=prefix,
            routing_keys=[f"{prefix}_{instance_id}"],
        )

        self._publisher = rabbitmq.RabbitPublisher(
            host=host,
            queues=[],
            exchanges={i: prefix for i in self._peer_ids},
        )

    def start(self):
        self._consumer = threading.Thread(
            target=self._input_exchange.start_consuming,
            args=(self._on_control,),
            name="EofBarrier",
        )
        self._consumer.start()

    def record_articles(self, req_id: int, amount: int = 1, sent: int = 0):
        with self._lock:
            state = self._state(req_id)
            state.articles_recv += amount
            state.articles_sent += sent

    def on_eof(self, req_id: int, expected_articles: int):
        with self._lock:
            self._state(req_id).expected_articles = expected_articles
            done, sent_total = self._start_round(req_id)

        if done:
            self.on_all_received(req_id, sent_total)

    def stop_consuming(self):
        """Safe to call from a signal handler: does not join or close anything."""
        with self._lock:
            self._closed = True
            for timer in self._timers:
                timer.cancel()

        try:
            self._input_exchange.stop_consuming_threadsafe()
        except Exception:
            pass

    def close(self, timeout: float = 5.0):
        deadline = time.time() + self._TIMEOUT

        self.stop_consuming()

        remaining = max(0.0, deadline - time.time())
        if self._consumer is not None:
            self._consumer.join(timeout=remaining)

        remaining = max(0.0, deadline - time.time())
        self._publisher.stop(timeout=remaining)

        try:
            self._input_exchange.close()
        except Exception:
            logging.exception("Error closing exchange")

    def _state(self, req_id: int) -> _RequestState:
        return self._states.setdefault(req_id, _RequestState())

    def _start_round(self, req_id: int) -> tuple[bool, int]:
        self._state(req_id).peer_counts = {}
        self._broadcast(message_protocol.OPCODE_PREPARE, req_id, self.instance_id)
        return self._evaluate(req_id)

    def _evaluate(self, req_id: int) -> tuple[bool, int]:
        """Returns (True, sent_total) if the request was committed (this replica must forward the EOF)."""
        state = self._states[req_id]

        if len(state.peer_counts) < len(self._peer_ids):
            return False, 0

        total_recv = state.articles_recv + sum(r for r, _ in state.peer_counts.values())
        if total_recv == state.expected_articles:
            total_sent = state.articles_sent + sum(s for _, s in state.peer_counts.values())
            self._broadcast(message_protocol.OPCODE_COMMIT, req_id)
            del self._states[req_id]
            return True, total_sent

        logging.info(
            "req_id=%s: received %s of %s articles, retrying in %ss",
            req_id, total_recv, state.expected_articles, self.retry_delay
        )
        self._schedule_retry(req_id)
        return False, 0

    def _schedule_retry(self, req_id: int):
        state = self._states[req_id]
        if self._closed or state.retry_scheduled:
            return

        state.retry_scheduled = True
        timer = threading.Timer(self.retry_delay, self._retry, args=(req_id,))
        timer.daemon = True
        self._timers.add(timer)
        timer.start()

    def _retry(self, req_id: int):
        try:
            with self._lock:
                self._timers.discard(threading.current_thread())

                if self._closed or req_id not in self._states:
                    return

                self._states[req_id].retry_scheduled = False
                done, sent_total = self._start_round(req_id)
        except Exception:
            logging.exception("Error retrying req_id=%s", req_id)
            return

        if done:
            self.on_all_received(req_id, sent_total)

    def _broadcast(self, opcode: int, req_id: int, message=None):
        data = self._serialize(opcode, req_id, message)
        for peer_id in self._peer_ids:
            self._publisher.publish_exchange(data, exchange_id=peer_id)

    def _send_to(self, peer_id: int, opcode: int, req_id: int, message=None):
        self._publisher.publish_exchange(
            self._serialize(opcode, req_id, message), exchange_id=peer_id
        )

    def _on_control(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)
            done = False
            committed = False
            sent_total = 0

            with self._lock:
                if opcode == message_protocol.OPCODE_PREPARE:
                    state = self._state(req_id)
                    logging.info(
                        "req_id=%s: PREPARE from %s, replying recv=%s sent=%s",
                        req_id, payload, state.articles_recv, state.articles_sent
                    )
                    self._send_to(
                        peer_id=payload,
                        opcode=message_protocol.OPCODE_OK,
                        req_id=req_id,
                        message=[self.instance_id, state.articles_recv, state.articles_sent],
                    )
                elif opcode == message_protocol.OPCODE_OK:
                    state = self._states.get(req_id)
                    if state is not None:
                        peer_id, recv, sent = payload
                        prev = state.peer_counts.get(peer_id)
                        if prev is None or recv >= prev[0]:
                            state.peer_counts[peer_id] = (recv, sent)
                        done, sent_total = self._evaluate(req_id)
                elif opcode == message_protocol.OPCODE_COMMIT:
                    logging.info("req_id=%s: COMMIT received", req_id)
                    self._states.pop(req_id, None)
                    committed = True
                else:
                    logging.warning("Unexpected control opcode: %s", opcode)

            if done:
                self.on_all_received(req_id, sent_total)
            elif committed and self.on_commit is not None:
                self.on_commit(req_id)

            ack()
        except Exception:
            logging.exception("Error processing control message")
            nack()

    @staticmethod
    def _serialize(opcode: int, req_id: int, message=None) -> bytes:
        if message is None:
            return message_protocol.internal.serialize(opcode=opcode, req_id=req_id)
        return message_protocol.internal.serialize(opcode=opcode, req_id=req_id, message=message)
