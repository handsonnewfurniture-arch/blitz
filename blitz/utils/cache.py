import hashlib
import json
import os
import time


class FileCache:
    """Simple file-based cache with TTL for fetch results."""

    def __init__(self, cache_dir: str = ".blitz_cache", ttl: int = 3600):
        self.cache_dir = cache_dir
        self.ttl = ttl

    def get(self, key: str):
        path = self._path(key)
        if not os.path.exists(path):
            return None
        if time.time() - os.path.getmtime(path) > self.ttl:
            os.unlink(path)
            return None
        with open(path, "r") as f:
            return json.load(f)

    def set(self, key: str, value):
        os.makedirs(self.cache_dir, exist_ok=True)
        with open(self._path(key), "w") as f:
            json.dump(value, f)

    def clear(self):
        if os.path.exists(self.cache_dir):
            for f in os.listdir(self.cache_dir):
                os.unlink(os.path.join(self.cache_dir, f))

    def _path(self, key: str) -> str:
        h = hashlib.sha256(key.encode()).hexdigest()[:16]
        return os.path.join(self.cache_dir, f"{h}.json")
