import pika

from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

class MessageMiddlewareCommonRabbitMQ:

    def stop_consuming(self):
        try:
            self.chan.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareCommonRabbitMQ, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name

        # creates a conn with RabbitMQ broker
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # defines a chan that allows to communicate with RabbitMQ broker
        self.chan = self.conn.channel()

        # both consumer and produced need the queue to exist
        self.chan.queue_declare(queue=self.queue_name)

    def start_consuming(self, on_message_callback):
        def callback(ch, method, _, body):
            on_message_callback(
                message=body, 
                ack=lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                nack=lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        try:
            self.chan.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            self.chan.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        super().stop_consuming()

    def send(self, message):
        try:
            self.chan.basic_publish(
                exchange='', # send to the default exchange
                routing_key=self.queue_name, # the routing key is the queue name
                body=message
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        super().close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareCommonRabbitMQ, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.chan = self.conn.channel()

        # both consumer and producer need the exchange to exist 
        self.chan.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def _declare_queue(self):
        result = self.chan.queue_declare(
            queue='',
            exclusive=True
        )

        queue = result.method.queue

        for routing_key in self.routing_keys:
            # binds the queue in the exchange with the routing key
            self.chan.queue_bind(
                queue=queue,
                exchange=self.exchange_name,
                routing_key=routing_key
            )

        return queue

    def start_consuming(self, on_message_callback):
        def callback(ch, method, _, body):
            on_message_callback(
                message=body, 
                ack=lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                nack=lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        try:
            # only the consumer needs to declare the queue
            queue = self._declare_queue()

            self.chan.basic_consume(
                queue=queue,
                on_message_callback=callback,
                auto_ack=False
            )

            self.chan.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        super().stop_consuming()

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                # the producer sends the message to the exchange, not the queue, with each routing key
                self.chan.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        super().close()