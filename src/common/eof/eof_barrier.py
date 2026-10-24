import logging
import threading
from dataclasses import dataclass, field
from typing import Callable

from common import message_protocol, middleware

RETRY_DELAY = 0.5

@dataclass
class _RequestState:
    expected_articles: int | None = None
    articles_recv: int = 0
    peer_counts: list[int] = field(default_factory=list)


class EofBarrier:
    """
    Coordinates the EOF of a request between N replicas of the same stage
    using the PREPARE / OK / COMMIT protocol.

    The replica that receives the EOF from the shared input queue asks its
    peers (through a direct exchange, one routing key per replica) how many
    articles each one processed. If the sum matches `articles_sent`, it
    commits and `on_all_received(req_id)` is called ONLY in that replica.
    Otherwise it waits `retry_delay` seconds and asks again.

    Usage:
        - call `record_articles` AFTER finishing the processing (and the
          sending) of a data message, so a counted article is a fully
          forwarded article.
        - call `on_eof` when the EOF arrives from the input queue.
        - call `start` to begin consuming the control messages and `close`
          at the end.
    """

    def __init__(
        self,
        host: str,
        prefix: str,
        instance_id: int,
        instances: int,
        on_all_received: Callable[[int], None],
        retry_delay: float = RETRY_DELAY,
    ):
        self.instance_id = instance_id
        self.instances = instances
        self.on_all_received = on_all_received
        self.retry_delay = retry_delay

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
        self._peers = {
            i: middleware.MessageMiddlewareExchangeRabbitMQ(
                host=host,
                exchange_name=prefix,
                routing_keys=[f"{prefix}_{i}"],
            )
            for i in range(instances)
            if i != instance_id
        }

    def start(self):
        self._consumer = threading.Thread(
            target=self._input_exchange.start_consuming,
            args=(self._on_control,),
            name="EofBarrier",
        )
        self._consumer.start()

    def record_articles(self, req_id: int, amount: int = 1):
        with self._lock:
            self._state(req_id).articles_recv += amount

    def on_eof(self, req_id: int, articles_sent: int):
        with self._lock:
            self._state(req_id).expected_articles = articles_sent
            done = self._start_round(req_id)

        if done:
            self.on_all_received(req_id)

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

    def close(self):
        self.stop_consuming()

        if self._consumer is not None:
            self._consumer.join()

        for exchange in [self._input_exchange, *self._peers.values()]:
            try:
                exchange.close()
            except Exception:
                logging.exception("Error closing exchange")

    def _state(self, req_id: int) -> _RequestState:
        return self._states.setdefault(req_id, _RequestState())

    def _start_round(self, req_id: int) -> bool:
        self._state(req_id).peer_counts = []
        self._broadcast(message_protocol.OPCODE_PREPARE, req_id, self.instance_id)
        return self._evaluate(req_id)

    def _evaluate(self, req_id: int) -> bool:
        """Returns True if the request was committed (this replica must forward the EOF)."""
        state = self._states[req_id]

        if len(state.peer_counts) < len(self._peers):
            return False

        total = sum(state.peer_counts) + state.articles_recv
        if total == state.expected_articles:
            self._broadcast(message_protocol.OPCODE_COMMIT, req_id)
            del self._states[req_id]
            return True

        logging.info(
            "req_id=%s: received %s of %s articles, retrying in %ss",
            req_id, total, state.expected_articles, self.retry_delay
        )
        self._schedule_retry(req_id)
        return False

    def _schedule_retry(self, req_id: int):
        if self._closed:
            return

        timer = threading.Timer(self.retry_delay, self._retry, args=(req_id,))
        timer.daemon = True
        self._timers.add(timer)
        timer.start()

    def _retry(self, req_id: int):
        with self._lock:
            self._timers.discard(threading.current_thread())

            if self._closed or req_id not in self._states:
                return

            done = self._start_round(req_id)

        if done:
            self.on_all_received(req_id)

    def _broadcast(self, opcode: int, req_id: int, message=None):
        data = self._serialize(opcode, req_id, message)
        for exchange in self._peers.values():
            exchange.send(data)

    def _send_to(self, peer_id: int, opcode: int, req_id: int, message=None):
        self._peers[peer_id].send(self._serialize(opcode, req_id, message))

    @staticmethod
    def _serialize(opcode: int, req_id: int, message=None) -> bytes:
        if message is None:
            return message_protocol.internal.serialize(opcode=opcode, req_id=req_id)
        return message_protocol.internal.serialize(opcode=opcode, req_id=req_id, message=message)


    def _on_control(self, message, ack, nack):
        try:
            opcode, req_id, _, payload = message_protocol.internal.deserialize(message)
            done = False

            with self._lock:
                if opcode == message_protocol.OPCODE_PREPARE:
                    self._send_to(
                        peer_id=payload,
                        opcode=message_protocol.OPCODE_OK,
                        req_id=req_id,
                        message=self._state(req_id).articles_recv,
                    )
                elif opcode == message_protocol.OPCODE_OK:
                    state = self._states.get(req_id)
                    if state is not None:
                        state.peer_counts.append(payload)
                        done = self._evaluate(req_id)
                elif opcode == message_protocol.OPCODE_COMMIT:
                    self._states.pop(req_id, None)
                else:
                    logging.warning("Unexpected control opcode: %s", opcode)

            if done:
                self.on_all_received(req_id)

            ack()
        except Exception:
            logging.exception("Error processing control message")
            nack()