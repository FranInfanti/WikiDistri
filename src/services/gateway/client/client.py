import socket
import threading

class Client:

    def __init__(self, client_socket: socket.socket, jobs: int):
        self.client_socket = client_socket
        self.send_lock = threading.Lock()
        self.articles_sent = 0
        self.jobs = {
            job_id: False
            for job_id in range(1, jobs + 1)
        }

    def record_articles_sent(self, count: int):
        self.articles_sent += count

    def close(self):
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.client_socket.close()
        except Exception:
            pass

    def finish(self, job_id: int):
        self.jobs[job_id] = True
        return all(self.jobs.values())