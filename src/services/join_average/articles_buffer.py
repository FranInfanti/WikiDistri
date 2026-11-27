import os

from common import storage

BUFFERED_STAGE = "BUFFERED"

class ArticleBuffer:
    def __init__(self, directory: str, segment_size: int):
        self._dir = directory
        self._size = segment_size
        self._open: dict[int, list] = {} 
        self._index: dict[tuple[int, str], int] = {}
        os.makedirs(directory, exist_ok=True)

    def _path(self, req_id: int, segment: int) -> str:
        return os.path.join(self._dir, f"{req_id}_{segment}.json")

    def _files(self) -> list[tuple[int, int]]:
        found = []
        for name in os.listdir(self._dir):
            try:
                req_id, segment = name.removesuffix(".json").split("_")
                found.append((int(req_id), int(segment)))
            except ValueError:
                continue
        return sorted(found)

    def requests(self) -> set[int]:
        return {req_id for req_id, _ in self._files()}

    def segments(self, req_id: int) -> list[int]:
        return [segment for r, segment in self._files() if r == req_id]

    def recover(self):
        for req_id, segment in self._files():
            store = storage.AtomicStore(self._path(req_id, segment))
            for msg_key in store.pending(BUFFERED_STAGE):
                self._index[(req_id, msg_key)] = segment

    def add(self, req_id: int, msg_key: str, articles: list) -> int:
        segment = self._index.get((req_id, msg_key))
        if segment is not None:
            return segment

        current = self._open.get(req_id)
        if current is None or current[2] >= self._size:
            last = current[0] if current else max(self.segments(req_id), default=-1)
            current = self._open[req_id] = [last + 1, storage.AtomicStore(self._path(req_id, last + 1)), 0]

        segment, store, _ = current
        store.put(msg_key, {"stage": BUFFERED_STAGE, "payload": articles})
        current[2] += 1
        self._index[(req_id, msg_key)] = segment
        return segment

    def load(self, req_id: int, segment: int) -> dict[str, list]:
        store = storage.AtomicStore(self._path(req_id, segment))
        return {key: store.get(key)["payload"] for key in store.pending(BUFFERED_STAGE)}

    def release(self, req_id: int, segment: int):
        try:
            os.remove(self._path(req_id, segment))
        except FileNotFoundError:
            pass

    def forget(self, req_id: int):
        """Drop everything of a request (files and memory)."""
        for segment in self.segments(req_id):
            self.release(req_id, segment)
        self._open.pop(req_id, None)
        for key in [k for k in self._index if k[0] == req_id]:
            del self._index[key]
