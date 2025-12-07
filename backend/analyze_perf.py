# analyze_perf.py
import sqlite3, csv, statistics, os, argparse
from datetime import datetime
parser = argparse.ArgumentParser()
parser.add_argument("--db", default="telemetry.db")
parser.add_argument("--sys_csv", default="perf_results/system_stats.csv")
args = parser.parse_args()

def iso_to_dt(s):
    if not s:
        return None
    try:
        from dateutil import parser as dtparser
        return dtparser.isoparse(s)
    except Exception:
        return None

def summarize_latencies(rows):
    lat = [r[0] for r in rows if r[0] is not None]
    if not lat:
        return {}
    s = {}
    s["count"] = len(lat)
    s["min_ms"] = min(lat)
    s["max_ms"] = max(lat)
    s["mean_ms"] = statistics.mean(lat)
    s["median_ms"] = statistics.median(lat)
    s["p95_ms"] = sorted(lat)[int(0.95*len(lat))-1]
    return s

db = args.db
if not os.path.exists(db):
    print("DB not found:", db); raise SystemExit

conn = sqlite3.connect(db)
cur = conn.cursor()

# telemetry latency summary
cur.execute("SELECT latency_ms FROM latency_telemetry")
rows = cur.fetchall()
rows_flat = [(r[0],) for r in rows]
telemetry_stats = summarize_latencies(rows)
print("Telemetry latency stats (ms):", telemetry_stats)

# frames FPS and latency
cur.execute("SELECT publish_ts, receive_ts, latency_ms FROM frames_received ORDER BY id ASC")
frame_rows = cur.fetchall()
# FPS: count frames / (last_receive_ts - first_receive_ts)
receive_times = []
lat_list = []
for pr, rr, lm in frame_rows:
    dt = iso_to_dt(rr)
    if dt:
        receive_times.append(dt.timestamp())
    if lm is not None:
        lat_list.append(lm)

if len(receive_times) >= 2:
    duration = receive_times[-1] - receive_times[0]
    fps = len(receive_times) / duration if duration>0 else None
else:
    fps = None

print("Frames received:", len(receive_times))
print("Measured FPS:", fps)
if lat_list:
    print("Frames latency stats (ms):", {
        "count": len(lat_list),
        "min_ms": min(lat_list),
        "max_ms": max(lat_list),
        "mean_ms": statistics.mean(lat_list),
        "median_ms": statistics.median(lat_list)
    })

conn.close()

# system stats
if os.path.exists(args.sys_csv):
    cpu = []
    mem = []
    with open(args.sys_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cpu.append(float(r["cpu_percent"]))
            mem.append(float(r["mem_used_mb"]))
    print("System CPU: avg %.2f, max %.2f" % (statistics.mean(cpu), max(cpu)))
    print("System RAM used (MB): avg %.2f, max %.2f" % (statistics.mean(mem), max(mem)))
else:
    print("System csv not found:", args.sys_csv)
