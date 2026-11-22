import os
import copy
import json
import tempfile
import threading

class PhaseManager:

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        try:
            with open(path) as f:
                self._data = json.load(f)
        except FileNotFoundError:
            self._data = {}

    def get(self, req_id: int) -> dict:
        with self._lock:
            return copy.deepcopy(self._data.get(str(req_id), {}))

    def put(self, req_id: int, record: dict):
        """Replace the entire record."""
        with self._lock:
            self._data[str(req_id)] = record
            self._write()

    def update(self, req_id: int, **fields):
        """Merge fields in the record (perform an atomic read-modify-write)."""
        with self._lock:
            self._data.setdefault(str(req_id), {}).update(fields)
            self._write()

    def pending(self, stage: str) -> list[int]:
        with self._lock:
            return [int(k) for k, v in self._data.items() if v.get("stage") == stage]

    def _write(self):
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path) or ".")
        with os.fdopen(fd, "w") as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

