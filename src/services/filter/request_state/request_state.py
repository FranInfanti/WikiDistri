from dataclasses import dataclass, field

@dataclass
class RequestState:
    expected_articles: int | None = None
    articles_recv: int = 0
    peer_counts: list[int] = field(default_factory=list)

    def record_article(self):
        self.articles_recv += 1

    def record_eof(self, expected_articles: int):
        self.expected_articles = expected_articles

    def record_peer_count(self, count: int):
        self.peer_counts.append(count)

    def reset_peers_count(self):
        self.peer_counts = []

    def all_peers(self, peers_amount: int):
        return self.peer_counts.__len__() == peers_amount - 1

    def all_articles_recv(self):
        return sum(self.peer_counts) + self.articles_recv == self.expected_articles
