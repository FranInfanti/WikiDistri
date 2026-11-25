# common/storage.py (agregar)
import json
import os
import tempfile
import threading


class WalStore:
    """
    Log de solo agregado para SumCount. Registros (una línea JSON cada uno):
      {"t":"d","r":req,"m":message_id,"e":entries}   DATA recibido
      {"t":"eof","r":req,"x":expected}               EOF pendiente (hasta que el coordinator termina)
      {"t":"del","r":req}                            request terminado
      {"t":"snap","r":req,"recv":n,"words":{...},"seen":[...]}   solo lo escribe la compactación
    """

    def __init__(self, path: str, compact_bytes: int = 64 << 20):
        self.path = path
        self._compact_bytes = compact_bytes
        self._lock = threading.Lock()
        self._seen: dict[int, set] = {}
        self._eofs: dict[int, object] = {}

        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        records = self._read()
        deleted = {r["r"] for r in records if r["t"] == "del"}
        self._records = [r for r in records if r["t"] != "del" and r["r"] not in deleted]
        self._rewrite(self._records)  # descarta requests borrados y una línea final cortada por un crash

        for r in self._records:
            req = r["r"]
            if r["t"] == "snap":
                self._seen[req] = set(r["seen"])
            elif r["t"] == "d":
                self._seen.setdefault(req, set()).add(r["m"])
            elif r["t"] == "eof":
                self._eofs[req] = r["x"]
                self._seen.setdefault(req, set())

        self._f = open(path, "a")
        self._bytes = 0

    def take_records(self) -> list[dict]:
        """Registros vivos para reconstruir el estado al arrancar (se liberan después de entregarlos)."""
        records, self._records = self._records, []
        return records

    def pending_eofs(self) -> list[tuple]:
        with self._lock:
            return list(self._eofs.items())

    def has_eof(self, req_id: int) -> bool:
        with self._lock:
            return req_id in self._eofs

    def begin_data(self, req_id: int, message_id, entries) -> bool:
        """Persiste el DATA (llamar ANTES del ack). False si ya se vio ese message_id."""
        message_id = str(message_id)
        with self._lock:
            if message_id in self._seen.get(req_id, ()):
                return False
            self._append({"t": "d", "r": req_id, "m": message_id, "e": entries})
            self._seen.setdefault(req_id, set()).add(message_id)
            return True

    def begin_eof(self, req_id: int, expected) -> bool:
        with self._lock:
            if req_id in self._eofs:
                return False
            self._append({"t": "eof", "r": req_id, "x": expected})
            self._eofs[req_id] = expected
            self._seen.setdefault(req_id, set())
            return True

    def delete(self, req_id: int):
        with self._lock:
            if req_id in self._seen or req_id in self._eofs:
                self._append({"t": "del", "r": req_id})
                self._seen.pop(req_id, None)
                self._eofs.pop(req_id, None)

    def maybe_compact(self, snapshot_fn):
        """snapshot_fn() -> {req_id: {"recv": n, "words": {...}}}. Llamar desde el hilo consumidor."""
        with self._lock:
            if self._bytes < self._compact_bytes:
                return
            snaps = snapshot_fn()
            records = [
                {"t": "snap", "r": req, "recv": snaps[req]["recv"],
                 "words": snaps[req]["words"], "seen": list(seen)}
                for req, seen in self._seen.items() if req in snaps
            ]
            records += [{"t": "eof", "r": req, "x": x} for req, x in self._eofs.items()]
            self._f.close()
            self._rewrite(records)
            self._f = open(self.path, "a")
            self._bytes = 0

    def _append(self, record: dict):
        line = json.dumps(record, separators=(",", ":")) + "\n"
        self._f.write(line)
        self._f.flush()
        os.fsync(self._f.fileno())
        self._bytes += len(line)

    def _read(self) -> list[dict]:
        records = []
        try:
            with open(self.path) as f:
                for line in f:
                    try:
                        records.append(json.loads(line))
                    except ValueError:
                        break  # línea final cortada por un crash
        except FileNotFoundError:
            pass
        return records

    def _rewrite(self, records: list[dict]):
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory)
        try:
            with os.fdopen(fd, "w") as f:
                for r in records:
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)