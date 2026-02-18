"""Fetch result caching step.

Wraps the existing FileCache from utils/cache.py. Prevents re-fetching
identical data across pipeline runs — critical for development iteration
and rate-limited APIs.
"""

from __future__ import annotations

import string
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry
from blitztigerclaw.utils.cache import FileCache


@StepRegistry.register("cache")
class CacheStep(BaseStep):
    """Cache pipeline data with TTL support.

    YAML usage:
    - cache:
        key: "api_posts_page_{page}"
        ttl: 3600
        action: "auto"
        dir: ".blitztigerclaw_cache"

    Actions:
      auto  — Check cache first; return cached on hit, pass through on miss.
      read  — Only read from cache. Returns empty if miss.
      write — Force-write current context.data to cache.
      clear — Clear cache for this key.
    """

    meta = StepMeta(
        description="Fetch result caching with TTL — prevents re-fetching across runs",
        config_docs={
            "key": "string — cache key (supports variable expansion)",
            "ttl": "int — time-to-live in seconds (default 3600)",
            "action": "string — auto | read | write | clear (default auto)",
            "dir": "string — cache directory (default .blitztigerclaw_cache)",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        ttl = self.config.get("ttl", 3600)
        cache_dir = self.config.get("dir", ".blitztigerclaw_cache")
        action = self.config.get("action", "auto")

        cache = FileCache(cache_dir=cache_dir, ttl=ttl)
        cache_key = self._expand_key()

        if action == "clear":
            cache.clear()
            self.context.vars["_cache_hit"] = False
            return self.context.data

        if action == "write":
            cache.set(cache_key, self.context.data)
            self.context.vars["_cache_hit"] = False
            return self.context.data

        if action == "read":
            cached = cache.get(cache_key)
            if cached is not None:
                self.context.vars["_cache_hit"] = True
                return cached
            self.context.vars["_cache_hit"] = False
            return []

        # action == "auto" (default)
        cached = cache.get(cache_key)
        if cached is not None:
            self.context.vars["_cache_hit"] = True
            return cached

        # Cache miss — pass through current data and store it
        self.context.vars["_cache_hit"] = False
        data = self.context.data
        if data:
            cache.set(cache_key, data)
        return data

    def _expand_key(self) -> str:
        """Expand variables in the cache key using pipeline vars."""
        raw_key = self.config.get("key", "_default")
        try:
            template = string.Template(raw_key.replace("{", "${"))
            return template.safe_substitute(self.context.vars)
        except Exception:
            return raw_key
