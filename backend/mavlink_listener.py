# backend/mavlink_listener.py
# MAVLink listener with throttling, dedupe, queue writer, and sample-row previews.

import argparse
import math
import queue
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pymavlink import mavutil

# ----------------------------------------------------------------------
# Sample Row Print Settings (add here near top)
# ----------------------------------------------------------------------
LAST_PRINT_TIME = 0
PRINT_INTERVAL = 3.0   # seconds

# ----------------------------------------------------------------------
# DB Settings
# ----------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parent
DB_PATH = BACKEND_DIR / "telemetry.db"
SQLITE_TIMEOUT = 10

# Throttle: Hz → seconds interval
POSITION_HZ = 5.0
POSITION_MIN_INTERVAL = 1.0 / POSITION_HZ

# Deduping
DEDUPE_LAT_LON_EPS = 1e-6
DEDUPE_ALT_EPS = 0.2

# Queue / Batch
WRITE_BATCH = 64
WRITE_QUEUE_MAX = 5000
DB_WORKER_FLUSH_SECONDS = 0.5
METRICS_INTERVAL = 10.0

write_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=WRITE_QUEUE_MAX)


# ----------------------------------------------------------------------
# DB Setup
# ----------------------------------------------------------------------
def ensure_db(path: Path):
    conn = sqlite3.connect(path, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            battery REAL,
            lat REAL,
            lon REAL,
            temperature REAL,
            altitude REAL DEFAULT 0.0,
            speed REAL DEFAULT 0.0
        )
        """
    )
    conn.commit()

    # Ensure altitude + speed columns exist
    cur.execute("PRAGMA table_info(telemetry)")
    cols = [r[1] for r in cur.fetchall()]

    if "altitude" not in cols:
        cur.execute("ALTER TABLE telemetry ADD COLUMN altitude REAL DEFAULT 0.0")
    if "speed" not in cols:
        cur.execute("ALTER TABLE telemetry ADD COLUMN speed REAL DEFAULT 0.0")
    conn.commit()

    conn.close()


# ----------------------------------------------------------------------
# DB Writer Thread
# ----------------------------------------------------------------------
def db_worker():
    while True:
        buf = []

        try:
            item = write_queue.get(timeout=DB_WORKER_FLUSH_SECONDS)
            buf.append(item)
            try:
                while len(buf) < WRITE_BATCH:
                    item = write_queue.get_nowait()
                    buf.append(item)
            except queue.Empty:
                pass
        except queue.Empty:
            continue

        try:
            conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO telemetry (timestamp, battery, lat, lon, temperature, altitude, speed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                buf,
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print("[DB_WORKER] Insert error:", e)


# ----------------------------------------------------------------------
# Queue insert helper
# ----------------------------------------------------------------------
def insert_queue_row(ts, battery, lat, lon, temp, alt, speed):
    try:
        write_queue.put_nowait(
            (
                ts,
                battery if battery is not None else None,
                float(lat),
                float(lon),
                float(temp) if temp is not None else 0.0,
                float(alt),
                float(speed),
            )
        )
        return True
    except queue.Full:
        return False


# ----------------------------------------------------------------------
# MAVLink Listener
# ----------------------------------------------------------------------
def run_mavlink_listener(conn_str: str):
    print(f"[mavlink_listener] Connecting to MAVLink at {conn_str} ...")
    master = mavutil.mavlink_connection(conn_str)

    print("[mavlink_listener] Waiting for heartbeat...")
    try:
        master.wait_heartbeat(timeout=10)
        print(f"[mavlink_listener] Heartbeat from system {master.target_system}")
    except Exception:
        print("[mavlink_listener] No heartbeat yet, continuing anyway...")

    # Request ALL data streams at POSITION_HZ
    try:
        master.mav.request_data_stream_send(
            master.target_system,
            master.target_component,
            mavutil.mavlink.MAV_DATA_STREAM_ALL,
            int(POSITION_HZ),
            1
        )
    except Exception:
        pass

    last_battery = None
    last_processed_ts = {}
    last_values = {}

    processed = 0
    last_metrics = time.time()

    global LAST_PRINT_TIME

    while True:
        msg = master.recv_match(blocking=True, timeout=5)
        if msg is None:
            continue

        msg_type = msg.get_type()

        # ---------------- BATTERY ----------------
        if msg_type == "SYS_STATUS":
            if msg.battery_remaining not in (None, 255):
                last_battery = float(msg.battery_remaining)

        elif msg_type == "BATTERY_STATUS":
            if msg.battery_remaining not in (None, 255):
                last_battery = float(msg.battery_remaining) / 10.0

        # ---------------- POSITION ----------------
        elif msg_type == "GLOBAL_POSITION_INT":
            lat = msg.lat / 1e7
            lon = msg.lon / 1e7
            alt = msg.relative_alt / 1000.0
            vx = msg.vx / 100.0
            vy = msg.vy / 100.0
            speed = math.hypot(vx, vy)

            now = datetime.now(timezone.utc)
            ts_iso = now.isoformat()
            device_id = str(master.target_system)

            # Throttle
            prev_ts = last_processed_ts.get(device_id)
            if prev_ts:
                if (now - prev_ts).total_seconds() < POSITION_MIN_INTERVAL:
                    continue

            # Dedupe
            prev = last_values.get(device_id)
            if prev:
                if (
                    abs(prev[0] - lat) < DEDUPE_LAT_LON_EPS
                    and abs(prev[1] - lon) < DEDUPE_LAT_LON_EPS
                    and abs(prev[2] - alt) < DEDUPE_ALT_EPS
                ):
                    last_processed_ts[device_id] = now
                    continue

            last_processed_ts[device_id] = now
            last_values[device_id] = (lat, lon, alt)

            insert_queue_row(ts_iso, last_battery, lat, lon, 0.0, alt, speed)
            processed += 1

        # ---------------- METRICS + SAMPLE PRINT ----------------
        if time.time() - last_metrics >= METRICS_INTERVAL:
            qsize = write_queue.qsize()
            rate = processed / max(1.0, (time.time() - last_metrics))
            print(f"[metrics] rate={rate:.2f}/s queue={qsize}")
            processed = 0
            last_metrics = time.time()

            # -------- SAMPLE ROW (every 3 sec) --------
            nowt = time.time()
            if nowt - LAST_PRINT_TIME > PRINT_INTERVAL:
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT timestamp, battery, lat, lon, altitude, speed
                        FROM telemetry
                        ORDER BY id DESC LIMIT 1
                    """)
                    r = cur.fetchone()
                    conn.close()
                    if r:
                        print("[SAMPLE ROW]", r)
                except Exception as e:
                    print("[SAMPLE ROW ERROR]", e)

                LAST_PRINT_TIME = nowt


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conn", default="tcp:127.0.0.1:5760")
    args = parser.parse_args()

    ensure_db(DB_PATH)

    t = threading.Thread(target=db_worker, daemon=True)
    t.start()
    print("[mavlink_listener] DB worker started")

    run_mavlink_listener(args.conn)


if __name__ == "__main__":
    main()
