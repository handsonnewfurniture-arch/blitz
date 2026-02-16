"""Streaming primitives for BlitzTigerClaw v0.2.0.

Provides:
- BatchBuffer: Collects rows into size-limited batches
- BackpressureChannel: Async queue with backpressure (bounded buffer)
- StreamAdapter: Convert list data to/from async iterables
- AdaptiveSemaphore: Dynamically adjusts concurrency based on error rate
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator

# Sentinel marking end of stream
STREAM_END = object()


class BatchBuffer:
    """Collects individual rows into fixed-size batches.

    Usage:
        buffer = BatchBuffer(size=1000)
        buffer.add(row)
        if buffer.full:
            batch = buffer.flush()
    """

    __slots__ = ("_items", "_size")

    def __init__(self, size: int = 1000):
        self._size = max(1, size)
        self._items: list[dict[str, Any]] = []

    def add(self, item: dict[str, Any]):
        self._items.append(item)

    def add_many(self, items: list[dict[str, Any]]):
        self._items.extend(items)

    @property
    def full(self) -> bool:
        return len(self._items) >= self._size

    @property
    def count(self) -> int:
        return len(self._items)

    def flush(self) -> list[dict[str, Any]]:
        batch = self._items
        self._items = []
        return batch

    def __len__(self) -> int:
        return len(self._items)


class BackpressureChannel:
    """Bounded async queue providing backpressure between producer and consumer.

    When the buffer is full, producers block until consumers drain items.
    This prevents unbounded memory growth in streaming pipelines.
    """

    def __init__(self, maxsize: int = 5000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._done = False
        self._total_in = 0
        self._total_out = 0

    async def put(self, item: dict[str, Any]):
        await self._queue.put(item)
        self._total_in += 1

    async def put_batch(self, items: list[dict[str, Any]]):
        for item in items:
            await self._queue.put(item)
            self._total_in += 1

    async def get(self) -> dict[str, Any] | None:
        item = await self._queue.get()
        if item is STREAM_END:
            return None
        self._total_out += 1
        return item

    async def close(self):
        self._done = True
        await self._queue.put(STREAM_END)

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    @property
    def stats(self) -> dict:
        return {
            "total_in": self._total_in,
            "total_out": self._total_out,
            "pending": self.pending,
        }

    async def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            item = await self.get()
            if item is None:
                break
            yield item


class AdaptiveSemaphore:
    """Semaphore that adjusts concurrency based on error rate.

    Starts at `initial` permits. On errors, reduces by half (min 1).
    On sustained success, increases by 1 (up to `max_concurrent`).
    """

    def __init__(self, initial: int = 10, max_concurrent: int = 50):
        self._current = initial
        self._max = max_concurrent
        self._semaphore = asyncio.Semaphore(initial)
        self._errors = 0
        self._successes = 0
        self._window = 20  # Evaluate every N completions
        self._lock = asyncio.Lock()

    async def acquire(self):
        await self._semaphore.acquire()

    def release(self, success: bool = True):
        self._semaphore.release()
        if success:
            self._successes += 1
        else:
            self._errors += 1

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info):
        success = exc_info[1] is None
        self.release(success)

    @property
    def current_limit(self) -> int:
        return self._current

    @property
    def error_rate(self) -> float:
        total = self._errors + self._successes
        return self._errors / total if total > 0 else 0.0


async def stream_from_list(data: list[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    """Adapter: Convert a list to an async iterator."""
    for item in data:
        yield item


async def collect_stream(stream: AsyncIterator[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adapter: Collect an async iterator into a list."""
    result = []
    async for item in stream:
        result.append(item)
    return result
