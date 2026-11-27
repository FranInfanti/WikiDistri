import time
import socket
import threading
import itertools

from common import message_protocol

class ClientSendError(Exception):
    pass

class Client:

    def __init__(self, client_socket: socket.socket, jobs: int, client_id: int = None, queues = ()):
        self.client_id = client_id
        self.client_socket = client_socket
        self.send_lock = threading.Lock()
        self.started_at = time.perf_counter()
        self.finished_at = None
        self.articles_sent = 0
        self.eof_sent = False
        self.seq_num = {
            queue: itertools.count(0)
            for queue in queues
        }
        self.jobs = {
            job_id: False
            for job_id in range(1, jobs + 1)
        }
        self.seen_messages = set()
        self.state_lock = threading.Lock()

    def record_message(self, message_id) -> bool:
        """Returns True if the message was already processed (duplicate), False if it is new."""
        with self.state_lock:
            if message_id in self.seen_messages:
                return True
            self.seen_messages.add(message_id)
            return False

    def record_articles_sent(self, count: int):
        self.articles_sent += count

    def record_eof_sent(self) -> bool:
        """Returns True if EOF was already recorded, False if this is the first time."""
        with self.state_lock:
            if self.eof_sent:
                return True
            self.eof_sent = True
            return False

    def record_job_finished(self, job_id: int):
        with self.state_lock:
            self.jobs[job_id] = True
            finished = all(self.jobs.values())
            if finished and self.finished_at is None:
                self.finished_at = time.perf_counter()
            return finished

    def elapsed_time(self):
        with self.state_lock:
            end_time = self.finished_at or time.perf_counter()
            return end_time - self.started_at

    def send(self, opcode: int, batches: int, message: bytes | str):
        with self.send_lock:
            try:
                message_protocol.external.send_message(
                    self.client_socket,
                    message,
                    opcode,
                    batches
                )
            except Exception as e:
                raise ClientSendError(f"{e}")

    def reconnect(self, new_socket: socket.socket):
        """Replaces the client socket on reconnection and closes the previous socket."""
        with self.send_lock:
            old_socket = self.client_socket
            self.client_socket = new_socket
        if old_socket is not None and old_socket is not new_socket:
            try:
                old_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                old_socket.close()
            except Exception:
                pass

    def close(self):
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.client_socket.close()
        except Exception:
            pass