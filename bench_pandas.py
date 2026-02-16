"""Benchmark: Pandas — same operations as Blitz pipeline."""
import json
import sqlite3
import time
import os
import pandas as pd

start = time.time()

# 1. Read JSON
t0 = time.time()
df = pd.read_json("bench_data.json")
t_read = time.time() - t0

# 2. Filter + Compute
t0 = time.time()
df = df[(df["price"] > 50) & (df["rating"] > 2.0)]
df["total_value"] = df["price"] * df["quantity"]
df["price_tier"] = df["price"] > 500
t_filter = time.time() - t0

# 3. Select + Dedupe + Sort + Limit
t0 = time.time()
fields = ["id", "name", "price", "quantity", "category", "city", "total_value", "price_tier"]
df = df[fields]
df = df.drop_duplicates(subset=["id"])
df = df.sort_values("total_value", ascending=False)
df = df.head(50000)
t_transform = time.time() - t0

# 4. SQLite write
t0 = time.time()
try:
    os.remove("bench_pandas.db")
except FileNotFoundError:
    pass
conn = sqlite3.connect("bench_pandas.db")
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
df.to_sql("products", conn, if_exists="replace", index=False)
conn.close()
t_load = time.time() - t0

total = time.time() - start
print(f"Pandas        | Read: {t_read*1000:.0f}ms | Filter+Compute: {t_filter*1000:.0f}ms | Transform: {t_transform*1000:.0f}ms | SQLite: {t_load*1000:.0f}ms | TOTAL: {total*1000:.0f}ms | Rows: {len(df)}")
