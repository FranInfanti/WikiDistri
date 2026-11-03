from dataclasses import dataclass, field

@dataclass
class MessageTracker:
    recv_messages: int = 0
    expected_messages: int | None = None
    peers_recv: list[int] = field(default_factory=list)

    sent_messages: dict[str, int] = field(default_factory=dict)
    peers_sent: dict[str, list[int]] = field(default_factory=dict)
    done_peers: int = 0

    def record_recv(self):
        self.recv_messages += 1

    def record_sent(self, key: str):
        self.sent_messages[key] = self.sent_messages.get(key, 0) + 1

    def record_expected(self, expected_messages: int):
        self.expected_messages = expected_messages

    def record_peer(self, recv_messages: int = None, sent_messages: int = None, key: str = None):
        if recv_messages is not None:
            self.peers_recv.append(recv_messages)

        if sent_messages is not None and key is not None:
            self.peers_sent.setdefault(key, []).append(sent_messages)

    def reset_peers(self):
        self.peers_recv = []
        self.peers_sent = {}

    def all_peers(self, amount: int):
        return self.peers_recv.__len__() == amount

    def all_recv(self):
        return sum(self.peers_recv) + self.recv_messages == self.expected_messages

    def all_sent(self, key):
        peer_sent = self.peers_sent.get(key, [])
        return sum(peer_sent) + self.sent_messages

    def sent_by_key(self):
        keys = dict.fromkeys((*self.peers_sent, *self.sent_messages))
        return [
            (
                key,
                self.sent_messages.get(key, 0)
                + sum(self.peers_sent.get(key, [])),
            )
            for key in keys
        ]

    def record_done(self):
        self.done_peers += 1

    def all_done(self, amount: int):
        return self.done_peers == amount
