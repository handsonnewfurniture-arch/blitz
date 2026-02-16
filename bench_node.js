/** Benchmark: Node.js — same operations as Blitz pipeline. */
const fs = require('fs');
const Database = require('better-sqlite3');

const start = performance.now();

// 1. Read JSON
let t0 = performance.now();
const raw = fs.readFileSync('bench_data.json', 'utf8');
const data = JSON.parse(raw);
const tRead = performance.now() - t0;

// 2. Filter + Compute
t0 = performance.now();
const filtered = [];
for (const row of data) {
    if (row.price > 50 && row.rating > 2.0) {
        row.total_value = row.price * row.quantity;
        row.price_tier = row.price > 500;
        filtered.push(row);
    }
}
const tFilter = performance.now() - t0;

// 3. Select + Dedupe + Sort + Limit
t0 = performance.now();
const fields = ['id', 'name', 'price', 'quantity', 'category', 'city', 'total_value', 'price_tier'];
const selected = filtered.map(row => {
    const obj = {};
    for (const k of fields) obj[k] = row[k];
    return obj;
});

const seen = new Set();
const deduped = [];
for (const row of selected) {
    if (!seen.has(row.id)) {
        seen.add(row.id);
        deduped.push(row);
    }
}

deduped.sort((a, b) => (b.total_value || 0) - (a.total_value || 0));
const final = deduped.slice(0, 50000);
const tTransform = performance.now() - t0;

// 4. SQLite write
t0 = performance.now();
try { fs.unlinkSync('bench_node.db'); } catch {}
const db = new Database('bench_node.db');
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

const cols = Object.keys(final[0]);
const colDefs = cols.map(c => `"${c}" TEXT`).join(', ');
db.exec(`CREATE TABLE products (${colDefs})`);

const placeholders = cols.map(() => '?').join(', ');
const insert = db.prepare(`INSERT INTO products VALUES (${placeholders})`);
const insertMany = db.transaction((rows) => {
    for (const row of rows) {
        insert.run(...cols.map(c => String(row[c] ?? '')));
    }
});

for (let i = 0; i < final.length; i += 5000) {
    insertMany(final.slice(i, i + 5000));
}
db.close();
const tLoad = performance.now() - t0;

const total = performance.now() - start;
console.log(`Node.js       | Read: ${tRead.toFixed(0)}ms | Filter+Compute: ${tFilter.toFixed(0)}ms | Transform: ${tTransform.toFixed(0)}ms | SQLite: ${tLoad.toFixed(0)}ms | TOTAL: ${total.toFixed(0)}ms | Rows: ${final.length}`);
