import os
import copy
import json
import tempfile
import threading

class AtomicStore:

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        parent_dir = os.path.dirname(self.path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
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

    def pending(self, stage: str) -> list[str]:
        """Return all keys whose record has the given stage."""
        with self._lock:
            return [k for k, v in self._data.items() if v.get("stage") == stage]

    def delete(self, key: str):
        """Remove a key entirely."""
        with self._lock:
            self._data.pop(str(key), None)
            self._write()

    def delete_prefix(self, prefix: str):
        """Remove every key that starts with `prefix` in a single atomic write."""
        with self._lock:
            keys = [k for k in self._data if k.startswith(prefix)]
            for key in keys:
                del self._data[key]
            if keys:
                self._write()

    def _write(self):
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory)

        with os.fdopen(fd, "w") as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp, self.path)