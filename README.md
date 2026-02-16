# BLITZTIGERCLAW

### 100x Efficiency Data Automation Framework

**Write 8 lines of YAML. Get optimized, parallel, production-grade data pipelines.**

BlitzTigerClaw replaces hundreds of lines of Python with simple declarative YAML — then executes it with async parallelism, batched writes, connection pooling, and retry logic. Built with Toyota Production System principles for quality, efficiency, and zero waste.

---

## The Problem

```python
# The old way: 150+ lines of Python for a simple ETL
import requests
import sqlite3
import json
import time

results = []
for page in range(1, 101):                    # Sequential. Slow.
    resp = requests.get(f"https://api.com/items?page={page}")
    data = resp.json()
    for item in data["items"]:
        if item["price"] > 10:
            results.append(item)

conn = sqlite3.connect("output.db")
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS items ...")
for row in results:                            # One insert at a time. Slow.
    cursor.execute("INSERT INTO items ...", row)
conn.commit()
# ~100 seconds, ~150 lines, no retry, no error handling
```

## The BlitzTigerClaw Way

```yaml
name: scrape_and_store
steps:
  - fetch:
      urls: "https://api.com/items?page={1..100}"
      parallel: 20
      retry: 3
  - transform:
      flatten: "$.data.items"
      filter: "price > 10"
  - load:
      target: sqlite:///output.db
      table: items
      mode: upsert
      key: id
```

```
$ blitztigerclaw run pipeline.yaml

--- Pipeline Complete ---
Rows: 8547
Duration: 5432ms
  fetch: 10000 rows in 5100ms
  transform: 8547 rows in 52ms
  load: 8547 rows in 280ms
  [KAIZEN: metrics recorded]
```

**8 lines. 5 seconds. 20x faster. Zero boilerplate.**

---

## Quick Start

```bash
# Clone and install
git clone https://github.com/handsonnewfurniture-arch/blitz.git
cd blitz
pip install -e .

# Create a starter pipeline
blitztigerclaw init

# Run it
blitztigerclaw run pipeline.yaml
```

---

## How It Works

```
YAML Pipeline  -->  Parser  -->  Optimizer  -->  Async Engine  -->  Results
                                    |
                              Decides strategy:
                              - fetch? async parallel
                              - transform 50k+ rows? multiprocess
                              - load? batched writes
```

Every step is auto-optimized. You declare **what** you want. BlitzTigerClaw figures out **how** to do it fast.

---

## 10 Built-in Step Types

### Data Steps

#### `fetch` — Async Parallel HTTP
```yaml
- fetch:
    urls: "https://api.example.com/data?page={1..100}"
    parallel: 20          # 20 concurrent requests
    retry: 3              # Retry with exponential backoff
    timeout: 30           # Per-request timeout
    method: GET           # GET, POST, PUT, DELETE
    headers:
      Authorization: "Bearer {api_key}"
    extract: "$.data"     # JSONPath extraction
```

#### `transform` — Data Shaping
```yaml
- transform:
    flatten: "$.data.items"          # Flatten nested structures
    select: [id, name, price]        # Pick fields
    rename: { old_name: new_name }   # Rename fields
    filter: "price > 10 and category == 'electronics'"
    compute:
      total: "price * quantity"      # Add computed fields
      label: "name.upper()"
    sort: "price desc"               # Sort
    dedupe: [id]                     # Deduplicate
    limit: 1000                      # Cap results
```

#### `load` — Multi-target Output
```yaml
# SQLite with batched upsert
- load:
    target: sqlite:///output.db
    table: items
    mode: upsert        # insert | upsert | replace
    key: id
    batch_size: 1000

# Or CSV / JSON / stdout
- load: { target: csv://output.csv }
- load: { target: json://output.json }
- load: { target: stdout }
```

#### `scrape` — HTML Extraction
```yaml
- scrape:
    urls: "https://example.com/page/{1..10}"
    parallel: 5
    select:
      title: "h1::text"
      price: ".price::text"
      link: "a.product::attr(href)"
```

#### `shell` — Run Commands
```yaml
- shell:
    command: "python3 process.py --input data.json"
    capture: json        # json | lines | raw
    timeout: 60
```

#### `file` — File I/O
```yaml
- file:
    action: read         # read | write | glob
    path: "./data/*.json"
    format: json         # json | csv | text
```

### Platform Steps

#### `github` — GitHub Integration
```yaml
- github:
    action: notifications    # notifications | issues | pr_list | repo_info | workflow_runs
    repo: owner/repo
    limit: 20
```

#### `railway` — Railway Integration
```yaml
- railway:
    action: status           # status | logs | deploy | variables
```

#### `netlify` — Netlify Integration
```yaml
- netlify:
    action: sites            # sites | status | deploy | logs
    site: my-site
```

### Quality Step

#### `guard` — Quality Gate (Andon Cord)
```yaml
- guard:
    schema:
      id: int
      name: str
      email: str
    required: [id, name]
    expect_rows: "10..1000"
    expect_no_nulls: [id]
    andon: true              # Alert if metrics deviate from history
```

---

## Toyota Production System

BlitzTigerClaw is built on TPS — the manufacturing principles that made Toyota the most efficient automaker in the world. Every pipeline run benefits from these principles automatically.

### KANBAN — Visual Workflow

Queue pipelines and process them with pull-based execution.

```bash
# Queue work
blitztigerclaw queue etl_pipeline.yaml
blitztigerclaw queue sync_pipeline.yaml
blitztigerclaw queue report_pipeline.yaml

# View the board
blitztigerclaw board

  KANBAN Board
  ===========================================================================
  BACKLOG            IN PROGRESS        DONE               FAILED
  ---------------------------------------------------------------------------
  report_pipeline    sync_pipeline      etl_pipeline

# Pull and execute next item
blitztigerclaw work
```

### JIT — Just-In-Time Processing

Skip steps when data hasn't changed. Add `jit: true` to any pipeline:

```yaml
name: incremental_sync
jit: true

steps:
  - fetch:
      url: "https://api.example.com/data"
  - transform:
      select: [id, name, value]
  - load:
      target: sqlite:///data.db
      table: records
      mode: upsert
      key: id
```

```
$ blitztigerclaw run pipeline.yaml --verbose     # First run: full execution
  fetch... 100 rows in 450ms
  transform... 100 rows in 1ms
  load... 100 rows in 15ms

$ blitztigerclaw run pipeline.yaml --verbose     # Second run: JIT detects no change
  fetch... (JIT: unchanged, skipping) 100 rows in 200ms
  transform... (JIT: unchanged, skipping) 100 rows in 0ms
  load... 100 rows in 12ms
  JIT: 2 steps skipped (unchanged)
```

### KAIZEN — Continuous Improvement

Every run is tracked. Bottlenecks are auto-detected with optimization suggestions.

```bash
$ blitztigerclaw metrics

  KAIZEN Dashboard
  ======================================================================
  Pipeline                   Runs   OK Fail   Avg ms Avg rows
  ----------------------------------------------------------------------
  scrape_and_store              12   11    1    326.1       30
  api_etl                        8    8    0    285.0       10
  platform_status                5    5    0    811.3        4

$ blitztigerclaw metrics -p scrape_and_store

  Bottleneck Analysis:
  --------------------------------------------------
  fetch           316.8ms (97.1%) ###################
    -> Increase 'parallel' or add caching
  load              2.6ms ( 0.8%)
  transform         0.5ms ( 0.2%)
```

### JIDOKA — Stop on Anomaly

The `guard` step is your Andon cord. It stops the production line when quality fails:

```yaml
steps:
  - fetch:
      url: "https://api.example.com/users"

  - guard:
      schema: { id: int, email: str }
      required: [id, email]
      expect_rows: "5..500"
      expect_no_nulls: [id]
      andon: true     # Compare against historical averages

  - load:
      target: sqlite:///users.db
      table: users
```

If the guard detects bad data, schema violations, or row counts that deviate 50%+ from historical averages — the pipeline stops before bad data reaches your database.

### MUDA — Waste Elimination

- **Dead step warnings**: Steps that produce zero data get flagged
- **JIT skip**: Don't reprocess unchanged data
- **Pipeline dedup**: Hash-based detection of identical runs

### POKA-YOKE — Error Proofing

Static analysis catches mistakes before execution:

```bash
$ blitztigerclaw lint pipeline.yaml

  [ERR] (POKA-YOKE) step 1: Unknown step type 'fecth'. Available: fetch, ...
  [TIP] (JIDOKA) step 2: Consider adding a 'guard' step between 'fetch' and 'load'
  [WARN] (MUDA) step 3: Steps 3 and 4 are identical 'transform' steps. Possible waste.

  1 errors, 1 warnings, 1 suggestions
```

---

## Variables & Patterns

### Pipeline Variables
```yaml
name: my_pipeline
vars:
  base_url: "https://api.example.com"
  api_key: "$API_KEY"       # Environment variable expansion

steps:
  - fetch:
      url: "{base_url}/data"
      headers:
        Authorization: "Bearer {api_key}"
```

### URL Patterns
```yaml
# Range expansion
urls: "https://api.com/page/{1..100}"     # 100 URLs

# List expansion
urls: "https://api.com/{users,posts,comments}"  # 3 URLs
```

### CLI Overrides
```bash
blitztigerclaw run pipeline.yaml -v api_key=sk-123 -v limit=50
```

---

## Python API

```python
import blitztigerclaw

# Synchronous
result = blitztigerclaw.run_sync("pipeline.yaml", api_key="sk-123")
print(result.data)           # List of row dicts
print(result.summary())      # Timing and row counts

# Async
result = await blitztigerclaw.run("pipeline.yaml")
```

---

## CLI Reference

```
blitztigerclaw run <file>             Execute a pipeline
  --verbose                  Show step-by-step progress
  --dry-run                  Parse and validate only
  -v key=value               Override pipeline variables

blitztigerclaw validate <file>        Validate YAML without executing
blitztigerclaw init                   Create starter pipeline.yaml
blitztigerclaw lint <file>            Static analysis (POKA-YOKE)

blitztigerclaw metrics                Performance dashboard (KAIZEN)
  -p <name>                  Detailed bottleneck analysis

blitztigerclaw board                  View Kanban board
blitztigerclaw queue <file>           Add pipeline to backlog
blitztigerclaw work                   Pull and execute from queue
  -n <count>                 Process N items then stop
```

---

## Performance

| Operation | Naive Python | BlitzTigerClaw | Speedup |
|---|---|---|---|
| Fetch 100 API pages | Sequential, ~100s | 20 concurrent async, ~5s | **20x** |
| Insert 10k rows to SQLite | One at a time, ~10s | Batched executemany, ~0.1s | **100x** |
| Transform + filter | Manual loops, 30+ lines | Declarative YAML, 4 lines | **10x dev time** |
| Retry failed requests | No retry, full failure | Per-request exponential backoff | **Reliability** |
| Reprocess unchanged data | Full rerun | JIT skip | **Eliminated** |

---

## Architecture

```
blitztigerclaw/
  __init__.py           Public API: run(), run_sync()
  cli.py                CLI entry point (8 commands)
  cli_tps.py            TPS commands (metrics, board, lint, queue, work)
  parser.py             YAML -> Pydantic validation
  pipeline.py           Step orchestrator + TPS hooks
  optimizer.py          Auto-selects execution strategy
  context.py            Runtime data flow between steps

  steps/
    fetch.py            Async parallel HTTP (aiohttp)
    transform.py        Data shaping engine
    load.py             Multi-target output (SQLite, CSV, JSON)
    scrape.py           HTML extraction
    shell.py            Subprocess execution
    file.py             File I/O
    guard.py            Quality gates (JIDOKA)
    railway.py          Railway platform integration
    netlify.py          Netlify platform integration
    github.py           GitHub platform integration

  tps/
    metrics.py          KAIZEN: SQLite performance tracking
    kanban.py           KANBAN: JSON workflow board
    change_detector.py  JIT: Hash-based change detection
    linter.py           POKA-YOKE: Static analysis

  utils/
    url_expander.py     {1..100} pattern expansion
    expr.py             Safe AST expression evaluator
    jsonpath.py         Lightweight JSONPath
    cache.py            File-based result caching
```

---

## Requirements

- Python 3.10+
- Dependencies: `click`, `pyyaml`, `aiohttp`, `aiosqlite`, `pydantic`
- Optional: `beautifulsoup4` for HTML scraping

---

## License

MIT

---

**Built with Toyota Production System principles. Zero waste. Maximum efficiency.**
