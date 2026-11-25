import os
import json
import time
import hashlib
import tempfile
import logging

from common import message_protocol

class ImageCache:
    """Per-image disk cache used only for crash recovery.

    A replayed batch does not need to download again the images that were already
    fetched before the crash. Entries live in their own files (not in state.json)
    so AtomicStore writes stay small. Each entry is [thumbnail_url, base64, content_type].
    """

    def __init__(self, directory: str, ttl: float):
        self._directory = directory
        self._ttl = ttl

    def _path(self, image: str):
        key = hashlib.sha256(image.encode(message_protocol.ENCODING)).hexdigest()
        return os.path.join(self._directory, f"{key}.json")

    def get(self, image: str):
        try:
            with open(self._path(image), encoding="utf-8") as f:
                thumbnail_url, image_b64, image_type = json.load(f)
            return thumbnail_url, image_b64, image_type
        except FileNotFoundError:
            return None
        except (ValueError, OSError):
            logging.warning("Corrupt cache entry for %s; ignoring it", image)
            return None

    def put(self, image: str, value: tuple):
        """
        Atomically store a successful download.
        
        Format:
            [
                "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Foo.jpg/1024px-Foo.jpg", 
                "/9j/4AAQSkZJRgABAQ...(base64)...", 
                "image/jpeg"
            ]
        """
        os.makedirs(self._directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self._directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(list(value), f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path(image))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def delete(self, image: str):
        try:
            os.unlink(self._path(image))
        except FileNotFoundError:
            pass
        except OSError:
            logging.warning("Could not delete cache entry for %s", image)

    def delete_batch(self, messages: list):
        """Drop the entries of a batch of [name, url, image] messages."""
        for _, _, image in messages:
            if image:
                self.delete(image)

    def sweep(self):
        """Remove orphaned entries (e.g. crash between COMPLETED and cleanup) older than the TTL."""
        try:
            names = os.listdir(self._directory)
        except FileNotFoundError:
            return

        now = time.time()
        for name in names:
            path = os.path.join(self._directory, name)
            try:
                if now - os.path.getmtime(path) > self._ttl:
                    os.unlink(path)
            except OSError:
                pass
