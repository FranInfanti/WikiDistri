import logging
import threading

from common import middleware
from queue import Queue, Empty

class RabbitPublisher:
    _POLL_INTERVAL = 10.0

    _FLAG_QUEUE = 0
    _FLAG_EXCHANGE = 1

    def __init__(self, host: str, queues: list, exchanges: dict = {}):
        self.host = host
        self.stopping = False
        self.state_lock = threading.Lock()

        self.queues = {
            queue_name: middleware.MessageMiddlewareQueueRabbitMQ(
                host=self.host,
                queue_name=queue_name
            )
            for queue_name in queues
        }

        self.exchanges = {
            exchange_id: middleware.MessageMiddlewareExchangeRabbitMQ(
                host=self.host,
                exchange_name=exchange_prefix,
                routing_keys=[f"{exchange_prefix}_{exchange_id}"]
            )
            for exchange_id, exchange_prefix in exchanges.items()
        }

        self.requests = Queue()
        self.thread = threading.Thread(
            target=self._run,
            name="Publisher"
        )

        self.thread.start()

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
        while True:
            try:
                request = self.requests.get(timeout=self._POLL_INTERVAL)
            except Empty:
                for queue in self.queues.values():
                    queue.conn.process_data_events(time_limit=0)
                for exchange in self.exchanges.values():
                    exchange.conn.process_data_events(time_limit=0)
                continue

            if request is None:
                self.requests.task_done()
                break

            completed, _err, message, flag, dest_name  = request
            try:
                if flag == self._FLAG_QUEUE:
                    self.queues[dest_name].send(message)
                else:
                    exchanges = [self.exchanges[dest_name]] \
                        if dest_name else self.exchanges.values()

                    for exchange in exchanges:
                        exchange.send(message)
            except Exception as err:
                request[1] = err
            finally:
                completed.set()
                self.requests.task_done()

        for queue in self.queues.values():
            try:
                queue.close()
            except:
                logging.warning("Failed to close RabbitMQ publisher connection")

        for exchange in self.exchanges.values():
            try:
                exchange.close()
            except:
                logging.warning("Failed to close RabbitMQ publisher connection")
