import os
import socket
import logging
import threading

from common import middleware
from common import message_protocol
from common import entity

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 5000))

MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
FILTER_OUTPUT_QUEUE = os.environ["FILTER_OUTPUT_QUEUE"]
WORD_READER_OUTPUT_QUEUE = os.environ["WORD_READER_OUTPUT_QUEUE"]

class Gateway():
    def __init__(self, host: str, port: int):
        self.filter_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=FILTER_OUTPUT_QUEUE
        )

        self.word_reader_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            host=MOM_HOST,
            queue_name=WORD_READER_OUTPUT_QUEUE
        )

        self.server_socket = None
        self.server_host = host
        self.server_port = port

        self.conns = {}
        self.conns_lock = threading.Lock()

    def _process_message(self, req_id: int, opcode: int, _: int, message: str):
        if opcode == message_protocol.OPCODE_DATA:
            logging.info(f"Received DATA from client with req_id: {req_id}. Sending to queues...")

            batches = message_protocol.batching.unbatch_messages(message)
            for csv_line in batches:
                article = entity.wikipedia_article.WikipediaArticle.from_csv(csv_line)

                print(article)

                self.filter_output_queue.send_message(
                    message_protocol.internal.serialize(article.to_list())
                )

                self.word_reader_output_queue.send_message(
                    message_protocol.internal.serialize(article.to_list())
                )
        elif opcode == message_protocol.OPCODE_EOF:
            logging.info(f"Received EOF from client with req_id: {req_id}. Sending to queues...")

            self.filter_output_queue.send_message(
                message_protocol.internal.serialize([opcode, 0, ""])
            )

            self.word_reader_output_queue.send_message(
                message_protocol.internal.serialize([opcode, 0, ""])
            )
        elif opcode == message_protocol.OPCODE_ACK:
            raise NotImplementedError("ACK opcode not implemented")
        else:
            logging.error(f"Unexpected opcode received from client with req_id: {req_id}. Opcode: {opcode}")

    def _handle_client(self, client_socket: socket, req_id: int):
        logging.info(f"Handle client with req_id: {req_id}")

        try:
            while True:
                args = message_protocol.external.recv_message(client_socket)
                self._process_message(req_id, *args)
                if args[0] == message_protocol.OPCODE_EOF:
                    break
        except Exception as e:
            logging.error(f"Error receiving message from client with req_id: {req_id}. Error: {e}")
        finally:
            with self.conns_lock:
                client_socket.close()
                del self.conns[client_socket]

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()

            logging.info("Listening to connections...")
            while True:
                try:
                    client_socket, _ = self.server_socket.accept()

                    thread = threading.Thread(
                        target=self._handle_client, 
                        args=(client_socket, id(client_socket), ),
                        daemon=True
                    )

                    with self.conns_lock:
                        self.conns[client_socket] = thread

                    thread.start()
                except Exception as e:
                    raise e
            
def main():
    try:
        Gateway(HOST, PORT).start()
    except Exception as e:
        logging.error(f"An unexpected error has occurred {e}")
        raise e

if __name__ == "__main__":
    main()