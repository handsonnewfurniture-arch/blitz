"""Benchmark: Blitz pipeline — timed to match other language benchmarks."""
import time
import asyncio
from blitztigerclaw import run

start = time.time()
asyncio.run(run("bench_pipeline.yaml"))
total = time.time() - start
print(f"Blitz (C eng) | TOTAL: {total*1000:.0f}ms")
