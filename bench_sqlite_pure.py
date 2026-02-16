"""Benchmark: Pure SQLite — load JSON into SQLite then query."""
import json
import sqlite3
import time
import os

start = time.time()

# 1. Read JSON
t0 = time.time()
with open("bench_data.json") as f:
    data = json.load(f)
t_read = time.time() - t0

# 2-4. Load into temp SQLite, then query out
t0 = time.time()
try:
    os.remove("bench_sqlite_pure.db")
except FileNotFoundError:
    pass

conn = sqlite3.connect("bench_sqlite_pure.db")
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=-8000")

# Create and bulk load
conn.execute("""CREATE TABLE raw (
    id INTEGER, name TEXT, price REAL, quantity INTEGER,
    category TEXT, city TEXT, rating REAL, in_stock INTEGER, weight_kg REAL
)""")

sql = "INSERT INTO raw VALUES (?,?,?,?,?,?,?,?,?)"
batch_size = 5000
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    conn.executemany(sql, [
        (r["id"], r["name"], r["price"], r["quantity"], r["category"],
         r["city"], r["rating"], 1 if r["in_stock"] else 0, r["weight_kg"])
        for r in batch
    ])
conn.commit()
t_load_raw = time.time() - t0

# Query: filter, compute, select, sort, limit
t0 = time.time()
conn.execute("""CREATE TABLE products AS
    SELECT id, name, price, quantity, category, city,
           price * quantity AS total_value,
           CASE WHEN price > 500 THEN 1 ELSE 0 END AS price_tier
    FROM raw
    WHERE price > 50 AND rating > 2.0
    GROUP BY id
    ORDER BY total_value DESC
    LIMIT 50000
""")
conn.commit()
row_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
conn.close()
t_query = time.time() - t0

total = time.time() - start
print(f"Pure SQLite   | Read: {t_read*1000:.0f}ms | Bulk Load: {t_load_raw*1000:.0f}ms | Query: {t_query*1000:.0f}ms | TOTAL: {total*1000:.0f}ms | Rows: {row_count}")
