import socket
import threading

from common import message_protocol

class ClientSendError(Exception):
    pass

class Client:

    def __init__(self, client_socket: socket.socket, jobs: int):
        self.client_socket = client_socket
        self.send_lock = threading.Lock()
        self.articles_sent = 0
        self.jobs = {
            job_id: False
            for job_id in range(1, jobs + 1)
        }
        self.state_lock = threading.Lock()

    def record_articles_sent(self, count: int):
        self.articles_sent += count

    def record_job_finished(self, job_id: int):
        with self.state_lock:
            self.jobs[job_id] = True
            return all(self.jobs.values())

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

    def close(self):
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.client_socket.close()
        except Exception:
            pass