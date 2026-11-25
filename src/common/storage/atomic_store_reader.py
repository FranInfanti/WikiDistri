import os
import json
import copy
import tempfile
import threading

PENDING = "PENDING"
COMPLETED = "COMPLETED"


class AtomicStoreReader:
    """
    {req_id: {"data": snapshot,
              "producers": {producer_id: {"last_message_id", "status", ["batches", "payload"]}},
              "eof": {"message_id", "expected"}}}      # "eof" only while the EOF is pending
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        try:
            with open(path) as f:
                self._data = json.load(f)
        except FileNotFoundError:
            self._data = {}

    def get(self, req_id: int) -> dict:
        with self._lock:
            return copy.deepcopy(self._data.get(str(req_id), {}))

    def begin(self, req_id: int, producer_id: str, message_id, batches: int, payload) -> bool:
        """
        Persist the whole batch as PENDING (call it BEFORE ack).
        Returns False if it is the last message already seen from that producer
        (PENDING or COMPLETED): a duplicate redelivery.
        """
        with self._lock:
            client = self._client(req_id)
            last = client["producers"].get(producer_id)
            if last is not None and last["last_message_id"] == str(message_id):
                return False

            client["producers"][producer_id] = {
                "last_message_id": str(message_id),
                "status": PENDING,
                "batches": batches,
                "payload": payload,
            }
            self._write()
            return True

    def apply(self, req_id: int, producer_id: str, message_id, state: dict):
        """
        One atomic write: replace the snapshot AND mark the message COMPLETED
        (the payload is dropped).
        """
        with self._lock:
            client = self._client(req_id)
            client["data"] = copy.deepcopy(state)
            client["producers"][producer_id] = {
                "last_message_id": str(message_id),
                "status": COMPLETED,
            }
            self._write()

    def snapshots(self) -> dict[int, dict]:
        with self._lock:
            return {int(r): copy.deepcopy(c["data"]) for r, c in self._data.items()}

    def pending(self) -> list[tuple]:
        """(req_id, producer_id, message_id, batches, payload) of every PENDING batch."""
        with self._lock:
            return [
                (int(r), p, last["last_message_id"], last["batches"], copy.deepcopy(last["payload"]))
                for r, c in self._data.items()
                for p, last in c["producers"].items()
                if last["status"] == PENDING
            ]

    def begin_eof(self, req_id: int, message_id, expected) -> bool:
        """
        Persist the EOF as pending (call it BEFORE ack).
        Returns False if this request already has an EOF stored (duplicate redelivery).
        """
        with self._lock:
            client = self._client(req_id)
            if client.get("eof") is not None:
                return False

            client["eof"] = {"message_id": str(message_id), "expected": expected}
            self._write()
            return True

    def pending_eofs(self) -> list[tuple]:
        """(req_id, message_id, expected) of every EOF whose forwarding did not finish."""
        with self._lock:
            return [
                (int(r), c["eof"]["message_id"], c["eof"]["expected"])
                for r, c in self._data.items()
                if c.get("eof") is not None
            ]

    def delete(self, req_id: int):
        """The request is finished: drops its snapshot, last messages and EOF."""
        with self._lock:
            if self._data.pop(str(req_id), None) is not None:
                self._write()

    def _client(self, req_id: int) -> dict:
        return self._data.setdefault(str(req_id), {"data": {}, "producers": {}})

    def _write(self):
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)

        fd, tmp = tempfile.mkstemp(dir=directory)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self._data, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)