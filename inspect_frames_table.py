# inspect_frames_table.py
import sqlite3, os, json
DB = "telemetry.db"
if not os.path.exists(DB):
    print("DB not found:", DB)
    raise SystemExit
conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("PRAGMA table_info(frames_received);")
rows = cur.fetchall()
print("frames_received schema (PRAGMA table_info):")
for r in rows:
    # (cid, name, type, notnull, dflt_value, pk)
    print(r)
# show a sample first few rows to see columns and values
cur.execute("SELECT * FROM frames_received LIMIT 5")
print("sample rows (col count %d):" % len(cur.description) )
if cur.description:
    print([d[0] for d in cur.description])
    for r in cur.fetchall():
        print(r)
conn.close()
