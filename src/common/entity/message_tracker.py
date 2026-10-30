from dataclasses import dataclass, field

@dataclass
class MessageTracker:
    recv_messages: int = 0
    sent_messages: int = 0
    expected_messages: int | None = None
    peers_recv: list[int] = field(default_factory=list)
    peers_sent: list[int] = field(default_factory=list)

    def record_recv(self):
        self.recv_messages += 1

    def record_sent(self):
        self.sent_messages += 1

    def record_expected(self, expected_messages: int):
        self.expected_messages = expected_messages

    def record_peer(self, recv_messages: int, sent_messages: int = 0):
        self.peers_recv.append(recv_messages)
        self.peers_sent.append(sent_messages)

    def reset_peers(self):
        self.peers_recv = []
        self.peers_sent = []

    def all_peers(self, amount: int):
        return self.peers_recv.__len__() == amount

    def all_recv(self):
        return sum(self.peers_recv) + self.recv_messages == self.expected_messages

    def all_sent(self):
        return sum(self.peers_sent) + self.sent_messages