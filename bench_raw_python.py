"""Benchmark: Raw Python — same operations as Blitz pipeline."""
import json
import sqlite3
import time

start = time.time()

# 1. Read JSON
t0 = time.time()
with open("bench_data.json") as f:
    data = json.load(f)
t_read = time.time() - t0

# 2. Filter + Compute
t0 = time.time()
filtered = []
for row in data:
    if row["price"] > 50 and row["rating"] > 2.0:
        row["total_value"] = row["price"] * row["quantity"]
        row["price_tier"] = row["price"] > 500
        filtered.append(row)
t_filter = time.time() - t0

# 3. Select + Dedupe + Sort + Limit
t0 = time.time()
fields = ["id", "name", "price", "quantity", "category", "city", "total_value", "price_tier"]
selected = [{k: row[k] for k in fields} for row in filtered]

seen = set()
deduped = []
for row in selected:
    if row["id"] not in seen:
        seen.add(row["id"])
        deduped.append(row)

deduped.sort(key=lambda r: r.get("total_value", 0), reverse=True)
final = deduped[:50000]
t_transform = time.time() - t0

# 4. SQLite write
t0 = time.time()
import os
try:
    os.remove("bench_raw_python.db")
except FileNotFoundError:
    pass

conn = sqlite3.connect("bench_raw_python.db")
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
cols = list(final[0].keys())
col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
conn.execute(f'CREATE TABLE products ({col_defs})')
placeholders = ", ".join("?" for _ in cols)
sql = f'INSERT INTO products VALUES ({placeholders})'
batch_size = 5000
for i in range(0, len(final), batch_size):
    batch = final[i:i+batch_size]
    conn.executemany(sql, [tuple(str(row.get(c, "")) for c in cols) for row in batch])
conn.commit()
conn.close()
t_load = time.time() - t0

total = time.time() - start
print(f"Raw Python    | Read: {t_read*1000:.0f}ms | Filter+Compute: {t_filter*1000:.0f}ms | Transform: {t_transform*1000:.0f}ms | SQLite: {t_load*1000:.0f}ms | TOTAL: {total*1000:.0f}ms | Rows: {len(final)}")
