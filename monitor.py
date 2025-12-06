# monitor.py
# Samples system CPU% and RAM usage every interval for duration seconds.
import psutil, time, csv, argparse, os

parser = argparse.ArgumentParser()
parser.add_argument("--duration", type=int, default=60)
parser.add_argument("--interval", type=float, default=0.5)
parser.add_argument("--out", default="perf_results/system_stats.csv")
args = parser.parse_args()

os.makedirs(os.path.dirname(args.out), exist_ok=True)
rows = []
start = time.time()
while time.time() - start < args.duration:
    t = time.time()
    cpu = psutil.cpu_percent(interval=None)  # non-blocking
    mem = psutil.virtual_memory()  # get memory stats
    rows.append({"ts": t, "cpu_percent": cpu, "mem_used_mb": mem.used/1024/1024, "mem_percent": mem.percent})
    time.sleep(args.interval)

# write CSV
with open(args.out, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["ts","cpu_percent","mem_used_mb","mem_percent"])
    writer.writeheader()
    writer.writerows(rows)
print("Wrote", args.out)
