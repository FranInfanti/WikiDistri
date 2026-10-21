from abc import ABC, abstractmethod

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    @abstractmethod
    def stop_consuming(self):
        pass

    @abstractmethod
    def stop_consuming_threadsafe(self):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

class MessageMiddlewareExchange(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, exchange_name, route_keys):
        pass

class MessageMiddlewareQueue(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, queue_name):
        pass