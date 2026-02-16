"""Benchmark v2: Profile each pipeline phase — Python vs C native."""
import time
import json
import sys

# Check orjson
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

from blitz.native.expr_engine import (
    compile_expr as native_compile,
    eval_filter, eval_compute,
    native_select, native_dedupe, native_sort,
)

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
FILTER_EXPR = "price > 50 and rating > 2.0"
COMPUTE_EXPR = "price * quantity"
SELECT_FIELDS = ["id", "name", "price", "quantity", "category", "city"]
DEDUPE_KEYS = ["category", "city"]
SORT_FIELD = "price"

def timeit(label, fn, *args):
    t0 = time.perf_counter()
    result = fn(*args)
    dt = time.perf_counter() - t0
    return result, dt

def main():
    print(f"=== Blitz Benchmark v2 — {ROWS:,} rows ===\n")

    # ---- Phase 0: JSON Load ----
    print("--- Phase 0: JSON Load ---")

    t0 = time.perf_counter()
    with open("bench_data.json", "r") as f:
        data_std = json.load(f)
    dt_json = time.perf_counter() - t0
    print(f"  json.load():    {dt_json*1000:>8.1f}ms")

    if HAS_ORJSON:
        t0 = time.perf_counter()
        with open("bench_data.json", "rb") as f:
            data = orjson.loads(f.read())
        dt_orjson = time.perf_counter() - t0
        print(f"  orjson.loads(): {dt_orjson*1000:>8.1f}ms  ({dt_json/dt_orjson:.1f}x faster)")
    else:
        data = data_std
        print("  orjson: not installed")

    data = data[:ROWS]
    print(f"  Loaded {len(data):,} rows\n")

    # ---- Phase 1: Filter ----
    print("--- Phase 1: Filter ---")

    # Python
    from blitz.utils.expr import _compile_python
    py_filter = _compile_python(FILTER_EXPR)
    _, dt = timeit("py", lambda d: [r for r in d if py_filter(r)], data)
    py_filtered = [r for r in data if py_filter(r)]
    print(f"  Python AST:  {dt*1000:>8.1f}ms  ({len(py_filtered):,} rows pass)")

    # C native
    c_filter = native_compile(FILTER_EXPR)
    c_filtered, dt = timeit("c", eval_filter, c_filter, data)
    print(f"  C native:    {dt*1000:>8.1f}ms  ({len(c_filtered):,} rows pass)  — {(dt and dt) and f'{(len(data)/dt)/1e6:.1f}M rows/sec' or 'inf'}")

    speedup = (len(data) and dt) and (lambda: None) or None  # use filtered data going forward
    working_data = list(c_filtered)
    print()

    # ---- Phase 2: Compute ----
    print("--- Phase 2: Compute ---")

    py_compute = _compile_python(COMPUTE_EXPR)
    working_copy = [dict(r) for r in working_data]
    t0 = time.perf_counter()
    for row in working_copy:
        row["total_value"] = py_compute(row)
    dt_py = time.perf_counter() - t0
    print(f"  Python AST:  {dt_py*1000:>8.1f}ms")

    c_compute = native_compile(COMPUTE_EXPR)
    working_copy2 = [dict(r) for r in working_data]
    _, dt_c = timeit("c", eval_compute, c_compute, working_copy2, "total_value")
    print(f"  C native:    {dt_c*1000:>8.1f}ms  — {dt_py/dt_c:.1f}x faster")

    working_data = working_copy2
    print()

    # ---- Phase 3: Select ----
    print("--- Phase 3: Select ---")

    t0 = time.perf_counter()
    py_selected = [{k: row.get(k) for k in SELECT_FIELDS} for row in working_data]
    dt_py = time.perf_counter() - t0
    print(f"  Python dict: {dt_py*1000:>8.1f}ms")

    c_selected, dt_c = timeit("c", native_select, working_data, SELECT_FIELDS)
    print(f"  C native:    {dt_c*1000:>8.1f}ms  — {dt_py/dt_c:.1f}x faster")

    working_data = c_selected
    print()

    # ---- Phase 4: Dedupe ----
    print("--- Phase 4: Dedupe ---")

    t0 = time.perf_counter()
    seen = set()
    py_deduped = []
    for row in working_data:
        key = tuple(row.get(k) for k in DEDUPE_KEYS)
        if key not in seen:
            seen.add(key)
            py_deduped.append(row)
    dt_py = time.perf_counter() - t0
    print(f"  Python set:  {dt_py*1000:>8.1f}ms  ({len(py_deduped):,} unique)")

    c_deduped, dt_c = timeit("c", native_dedupe, working_data, DEDUPE_KEYS)
    print(f"  C native:    {dt_c*1000:>8.1f}ms  ({len(c_deduped):,} unique)  — {dt_py/dt_c:.1f}x faster")

    # Use non-deduped for sort benchmark (more data = better test)
    print()

    # ---- Phase 5: Sort ----
    print("--- Phase 5: Sort ---")

    sort_data = list(working_data)  # copy for fair comparison
    t0 = time.perf_counter()
    sort_data.sort(key=lambda r: (r.get(SORT_FIELD) is None, r.get(SORT_FIELD, 0)), reverse=True)
    dt_py = time.perf_counter() - t0
    print(f"  Python sort: {dt_py*1000:>8.1f}ms  ({len(sort_data):,} rows)")

    sort_data2 = list(working_data)
    c_sorted, dt_c = timeit("c", native_sort, sort_data2, SORT_FIELD, True)
    print(f"  C native:    {dt_c*1000:>8.1f}ms  ({len(c_sorted):,} rows)  — {dt_py/dt_c:.1f}x faster")
    print()

    # ---- Full Pipeline ----
    print("=" * 50)
    print("FULL PIPELINE COMPARISON (filter→compute→select→dedupe→sort)")
    print("=" * 50)

    # All Python
    t0 = time.perf_counter()
    d = list(data)
    d = [r for r in d if py_filter(r)]
    for row in d:
        row["total_value"] = py_compute(row)
    d = [{k: row.get(k) for k in SELECT_FIELDS} for row in d]
    seen = set()
    dd = []
    for row in d:
        key = tuple(row.get(k) for k in DEDUPE_KEYS)
        if key not in seen:
            seen.add(key)
            dd.append(row)
    dd.sort(key=lambda r: (r.get(SORT_FIELD) is None, r.get(SORT_FIELD, 0)), reverse=True)
    dt_full_py = time.perf_counter() - t0

    # All C native
    t0 = time.perf_counter()
    d = list(data)
    d = eval_filter(c_filter, d)
    eval_compute(c_compute, d, "total_value")
    d = native_select(d, SELECT_FIELDS)
    d = native_dedupe(d, DEDUPE_KEYS)
    d = native_sort(d, SORT_FIELD, True)
    dt_full_c = time.perf_counter() - t0

    print(f"\n  Python total:  {dt_full_py*1000:>8.1f}ms")
    print(f"  C native:      {dt_full_c*1000:>8.1f}ms")
    print(f"  Speedup:       {dt_full_py/dt_full_c:.1f}x faster")
    print(f"  Throughput:    {(ROWS/dt_full_c)/1e6:.2f}M rows/sec")

if __name__ == "__main__":
    main()
