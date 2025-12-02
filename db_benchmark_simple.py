# db_benchmark_simple.py
# Writes 1000 telemetry-like entries each to SQLite, DuckDB, and LMDB.
# Outputs results to db_benchmark_results.csv in the same folder.

import os, time, sqlite3, json, lmdb, duckdb, csv
from datetime import datetime, timezone
import random, string

NUM_ENTRIES = 1000
OUT_CSV = "db_benchmark_results.csv"

def gen_entry(i):
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": f"dev-{i%10:02d}",
        "battery": round(random.uniform(20,100),2),
        "lat": round(17.446 + random.uniform(-0.01,0.01),6),
        "lon": round(78.356 + random.uniform(-0.01,0.01),6),
        "temperature": round(20 + random.uniform(-5,10),2),
        "payload": "".join(random.choices(string.ascii_letters + string.digits, k=64))
    }

results = []

# SQLite benchmark
sqlite_db = "benchmark_sqlite.db"
if os.path.exists(sqlite_db):
    os.remove(sqlite_db)
conn = sqlite3.connect(sqlite_db, timeout=30)
cur = conn.cursor()
cur.execute("PRAGMA journal_mode=WAL;")
cur.execute("""
CREATE TABLE telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    device_id TEXT,
    battery REAL,
    lat REAL,
    lon REAL,
    temperature REAL,
    payload TEXT
)
""")
conn.commit()
start = time.perf_counter()
cur.execute("BEGIN;")
for i in range(NUM_ENTRIES):
    e = gen_entry(i)
    cur.execute("INSERT INTO telemetry (timestamp, device_id, battery, lat, lon, temperature, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (e["timestamp"], e["device_id"], e["battery"], e["lat"], e["lon"], e["temperature"], e["payload"]))
cur.execute("COMMIT;")
conn.commit()
end = time.perf_counter()
total = end - start
avg = total / NUM_ENTRIES
results.append({
    "db": "sqlite",
    "total_seconds": round(total, 6),
    "avg_seconds_per_write": round(avg, 9)
})
conn.close()

# DuckDB benchmark
duckdb_file = "benchmark_duckdb.db"
if os.path.exists(duckdb_file):
    os.remove(duckdb_file)
con = duckdb.connect(duckdb_file)
con.execute("""
CREATE TABLE telemetry (
    id INTEGER,
    timestamp VARCHAR,
    device_id VARCHAR,
    battery DOUBLE,
    lat DOUBLE,
    lon DOUBLE,
    temperature DOUBLE,
    payload VARCHAR
)
""")
start = time.perf_counter()
for i in range(NUM_ENTRIES):
    e = gen_entry(i)
    con.execute("INSERT INTO telemetry VALUES (?,?,?,?,?,?,?,?)",
                (i+1, e["timestamp"], e["device_id"], e["battery"], e["lat"], e["lon"], e["temperature"], e["payload"]))
end = time.perf_counter()
total = end - start
avg = total / NUM_ENTRIES
results.append({
    "db": "duckdb",
    "total_seconds": round(total, 6),
    "avg_seconds_per_write": round(avg, 9)
})
con.close()

# LMDB benchmark
lmdb_dir = "benchmark_lmdb"
if os.path.exists(lmdb_dir):
    import shutil
    shutil.rmtree(lmdb_dir)
env = lmdb.open(lmdb_dir, map_size=1024*1024*1024)  # 1GB map
start = time.perf_counter()
with env.begin(write=True) as txn:
    for i in range(NUM_ENTRIES):
        e = gen_entry(i)
        key = f"key_{i}".encode("utf-8")
        val = json.dumps(e).encode("utf-8")
        txn.put(key, val)
end = time.perf_counter()
total = end - start
avg = total / NUM_ENTRIES
results.append({
    "db": "lmdb",
    "total_seconds": round(total, 6),
    "avg_seconds_per_write": round(avg, 9)
})
env.close()

# Write results CSV
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["db","total_seconds","avg_seconds_per_write"])
    writer.writeheader()
    for r in results:
        writer.writerow(r)

print("Benchmark complete. Results written to", OUT_CSV)
for r in results:
    print(r["db"], "total:", r["total_seconds"], "s", "avg:", r["avg_seconds_per_write"], "s")
