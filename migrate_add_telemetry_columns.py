# migrate_add_telemetry_columns.py
import sqlite3, os, sys

DB = "telemetry.db"   # update if your DB filename is different
EXPECTED_COLUMNS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "timestamp": "TEXT",
    "device_id": "TEXT",
    "battery": "REAL",
    "lat": "REAL",
    "lon": "REAL",
    "temperature": "REAL"
}

if not os.path.exists(DB):
    print("DB not found:", DB)
    sys.exit(1)

conn = sqlite3.connect(DB)
cur = conn.cursor()

# list existing columns
cur.execute("PRAGMA table_info(telemetry);")
cols = cur.fetchall()  # (cid, name, type, notnull, dflt_value, pk)
existing = [c[1] for c in cols]
print("Existing telemetry columns:", existing)

# Add any missing columns
added = []
for col, coltype in EXPECTED_COLUMNS.items():
    if col not in existing:
        # skip id because altering primary key if not present is tricky
        if col == "id" and "id" in existing:
            continue
        sql = f'ALTER TABLE telemetry ADD COLUMN {col} {coltype};'
        print("Adding column:", col, coltype)
        try:
            cur.execute(sql)
            added.append(col)
        except Exception as e:
            print("Failed to add column", col, ":", e)

conn.commit()
conn.close()
print("Done. Added columns:", added)
