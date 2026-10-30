import logging
import threading

from common import middleware
from queue import Queue, Empty

class RabbitPublisher:
    _POLL_INTERVAL = 5.0

    _FLAG_QUEUE = 0
    _FLAG_EXCHANGE = 1

    def __init__(
        self,
        host: str,
        queues: list[str] | None = None,
        exchanges: dict[int, str] | None = None
    ):
        self.host = host
        self.queue_conf = list(queues) if queues is not None else []
        self.exchange_conf = dict(exchanges) if exchanges is not None else {}

        self.requests = Queue()
        self.started = threading.Event()
        self.state_lock = threading.Lock()

        self.startup_error = None
        self.failure = None
        self.stopping = False

        self.thread = threading.Thread(
            target=self._run,
            name="Publisher"
        )

        self.thread.start()
        self.started.wait()
        
        if self.startup_error:
            self.thread.join()
            raise self.startup_error

    def stop(self):
        with self.state_lock:
            if self.stopping:
                return

            self.stopping = True
            self.requests.put(None)

        self.thread.join()

    def publish_queue(self, queue: str, message: bytes):
        completed = threading.Event()
        request = [completed, None, message, self._FLAG_QUEUE, queue]

        with self.state_lock:
            if self.stopping:
                raise RuntimeError("RabbitMQ publisher is shutting down")
            self.requests.put(request)

        completed.wait()
        if request[1]:
            raise request[1]

    def publish_exchange(self, message: bytes, exchange_id: int = None):
        completed = threading.Event()
        request = [completed, None, message, self._FLAG_EXCHANGE, exchange_id]

        with self.state_lock:
            if self.stopping:
                raise RuntimeError("RabbitMQ publisher is shutting down")
            self.requests.put(request)

        completed.wait()
        if request[1]:
            raise request[1]

    def _run(self):
        queues = {}
        exchanges = {}
        try:
            for queue_name in self.queue_conf:
                queues[queue_name] = middleware.MessageMiddlewareQueueRabbitMQ(
                    host=self.host,
                    queue_name=queue_name
                )
            for exchange_id, exchange_prefix in self.exchange_conf.items():
                exchanges[exchange_id] = middleware.MessageMiddlewareExchangeRabbitMQ(
                    host=self.host,
                    exchange_name=exchange_prefix,
                    routing_keys=[f"{exchange_prefix}_{exchange_id}"]
                )
        except Exception as err:
            self.startup_error = err
            return
        finally:
            self.started.set()

        while True:
            try:
                request = self.requests.get(timeout=self._POLL_INTERVAL)
            except Empty:
                for queue in queues.values():
                    queue.conn.process_data_events(time_limit=0)
                for exchange in exchanges.values():
                    exchange.conn.process_data_events(time_limit=0)
                continue

            if request is None:
                self.requests.task_done()
                break

            completed, _err, message, flag, dest_name  = request
            try:
                if flag == self._FLAG_QUEUE:
                    queues[dest_name].send(message)
                else:
                    dest_exchanges = [exchanges[dest_name]] \
                        if dest_name else exchanges.values()

                    for dest_exchange in dest_exchanges:
                        dest_exchange.send(message)
            except Exception as err:
                request[1] = err
            finally:
                completed.set()
                self.requests.task_done()
        self._close_conns()

    def _close_conns(self):
        for queue in self.queues.values():
            try:
                queue.close()
            except Exception:
                logging.warning("Failed to close RabbitMQ publisher queue connection", exc_info=True)

        for exchange in self.exchanges.values():
            try:
                exchange.close()
            except Exception:
                logging.warning("Failed to close RabbitMQ publisher exchange connection", exc_info=True)
