# fastapi_receiver.py
import json
import sqlite3
import threading
import os
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import paho.mqtt.client as mqtt
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparser 
import csv
from statistics import mean, median
from backend.anomaly_detection import AnomalyDetector 
from backend.drone_registry import router as drone_router

# -------- CONFIG --------
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "device/telemetry"   # must match simulator topic

DB_PATH = os.path.join(os.path.dirname(__file__), "telemetry.db")   # reuse the DB you already have
ROOT_DIR = Path(__file__).resolve().parents[1]
SAMPLES_DIR = ROOT_DIR / "samples"

SQLITE_TIMEOUT = 10
# ------------------------

class TelemetryIn(BaseModel):
    timestamp: str | None = None   # ISO string, optional
    battery: float
    lat: float
    lon: float
    temperature: float


app = FastAPI(title="Telemetry Receiver")
app.include_router(drone_router)

# In-memory latest reading with lock for thread-safety
_latest_lock = threading.Lock()
_latest_reading: Optional[dict] = None

anomaly_detector = AnomalyDetector()

# Ensure DB exists and WAL is enabled, and flattened table exists
def init_db(path: str):
    need_create = not os.path.exists(path)
    conn = sqlite3.connect(path, timeout=SQLITE_TIMEOUT, check_same_thread=False)
    cur = conn.cursor()
    # Enable WAL mode
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    # Create flattened table if doesn't exist (matches your simulator columns)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS telemetry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        battery REAL,
        lat REAL,
        lon REAL,
        temperature REAL
    )
    """)
    conn.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS latency_telemetry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT,
        publish_ts TEXT,
        receive_ts TEXT,
        latency_ms REAL
    )
    """)
    conn.commit()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS frames_received (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT,
        frame_id INTEGER,
        publish_ts TEXT,
        receive_ts TEXT,
        latency_ms REAL
    )
    """)
    conn.commit()

    conn.close()

@app.on_event("startup")
def startup_event():
    # existing db/mqtt startup if you have it
    anomaly_detector.start_gps_monitor()
    print("[Anomaly] GPS monitor started")


# Insert a single flattened row (short-lived connection to be thread safe)
def insert_row(path: str, timestamp, battery, lat, lon, temperature):
    conn = sqlite3.connect(path, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO telemetry (timestamp, battery, lat, lon, temperature)
    VALUES (?, ?, ?, ?, ?)
    """, (timestamp, battery, lat, lon, temperature))
    conn.commit()
    conn.close()
    
def insert_latency_telemetry(path, device_id, publish_ts, receive_ts, latency_ms):
    conn = sqlite3.connect(path, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO latency_telemetry (device_id, publish_ts, receive_ts, latency_ms) VALUES (?, ?, ?, ?)",
        (device_id, publish_ts, receive_ts, latency_ms)
    )
    conn.commit()
    conn.close()

def insert_frame_received(path, device_id, frame_id, publish_ts, receive_ts, latency_ms):
    """
    Insert one frames_received row.
    Note: the frames_received table columns are:
      id (AUTOINCREMENT), device_id, frame_id, publish_ts, receive_ts, latency_ms
    So the SQL must insert exactly 5 values (excluding id).
    """
    conn = sqlite3.connect(path, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()

    # DEBUG: print the tuple we are about to insert (helps confirm shape)
    values = (device_id, frame_id, publish_ts, receive_ts, latency_ms)
    try:
        # Print debug only to server console so you can see what's being inserted
        print("[DEBUG] insert_frame_received values tuple length:", len(values), "values:", values)
    except Exception:
        pass

    cur.execute(
        "INSERT INTO frames_received (device_id, frame_id, publish_ts, receive_ts, latency_ms) VALUES (?, ?, ?, ?, ?)",
        values
    )
    conn.commit()
    conn.close()



# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected to broker.")
        client.subscribe(MQTT_TOPIC)
        client.subscribe("device/frame",qos=1)
        print(f"[MQTT] Subscribed to {MQTT_TOPIC}")
        print(f"[MQTT] Subscribed to device/frame")
    else:
        print("[MQTT] Failed to connect, rc=", rc)

def on_message(client, userdata, msg):
    """
    MQTT message handler for:
      - topic "device/telemetry"  : telemetry JSON (device_id, timestamp, battery, lat, lon, temperature)
      - topic "device/frame"      : frame notifications (device_id, frame_id, timestamp)

    Records:
      - telemetry rows via insert_row(DB_PATH, parsed)
      - latency in latency_telemetry (publish_ts -> receive_ts)
      - frame receives in frames_received (and their latency)
      - updates in-memory latest reading (_latest_reading) for telemetry
    """
    global _latest_reading

    topic = msg.topic
    payload_text = msg.payload.decode("utf-8")

    # parse JSON payload safely
    try:
        parsed = json.loads(payload_text)
    except Exception as e:
        print("[MQTT] Failed to parse JSON payload on topic", topic, "error:", e)
        return

    # receive timestamp recorded by backend (ISO UTC)
    receive_ts = datetime.now(timezone.utc).isoformat()

    # --- Telemetry messages ---
    if topic == "device/telemetry":
        # 1) store telemetry row (existing function)
        try:
            insert_row(
                DB_PATH,
                parsed.get("timestamp"),
                parsed.get("battery"),
                parsed.get("lat"),
                parsed.get("lon"),
                parsed.get("temperature")
            )

        except Exception as e:
            print("[MQTT] Failed insert_row:", e)

        # 2) compute latency if publisher provided timestamp
        pub_ts = parsed.get("timestamp")
        lat_ms = None
        if pub_ts:
            try:
                p = dtparser.isoparse(pub_ts)
                r = dtparser.isoparse(receive_ts)
                lat_ms = (r - p).total_seconds() * 1000.0
            except Exception:
                lat_ms = None

        # 3) insert latency record
        try:
            insert_latency_telemetry(DB_PATH, parsed.get("device_id"), pub_ts, receive_ts, lat_ms)
        except Exception as e:
            print("[MQTT] Failed insert_latency_telemetry:", e)

        # 4) update in-memory latest reading safely
        try:
            with _latest_lock:
                _latest_reading = {
                    "timestamp": parsed.get("timestamp"),
                    "device_id": parsed.get("device_id"),
                    "battery": parsed.get("battery"),
                    "lat": parsed.get("lat"),
                    "lon": parsed.get("lon"),
                    "temperature": parsed.get("temperature")
                }
        except Exception as e:
            print("[MQTT] Failed update _latest_reading:", e)

                # 5) send to anomaly detector
        try:
            anomaly_detector.process_telemetry(parsed)
        except Exception as e:
            print("[Anomaly] process_telemetry error:", e)

        return

    # --- Frame messages ---
    if topic == "device/frame":
        pub_ts = parsed.get("timestamp")
        frame_id = parsed.get("frame_id")
        lat_ms = None
        if pub_ts:
            try:
                p = dtparser.isoparse(pub_ts)
                r = dtparser.isoparse(receive_ts)
                lat_ms = (r - p).total_seconds() * 1000.0
            except Exception:
                lat_ms = None

        try:
            insert_frame_received(DB_PATH, parsed.get("device_id"), frame_id, pub_ts, receive_ts, lat_ms)
        except Exception as e:
            print("[MQTT] Failed insert_frame_received:", e)
        return

    # Unhandled topics: silently ignore or log
    # print("[MQTT] Unhandled topic:", topic)



# Start MQTT client in background (non-blocking)
def start_mqtt_background():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client

@app.post("/telemetry")
def post_telemetry(t: TelemetryIn):
    """
    Accepts telemetry via HTTP POST and stores it in backend/telemetry.db.
    If timestamp is not provided, backend fills in current UTC time.
    """
    ts = t.timestamp or datetime.now(timezone.utc).isoformat()

    try:
        # reuse existing insert_row helper
        insert_row(
            DB_PATH,
            ts,
            t.battery,
            t.lat,
            t.lon,
            t.temperature,
        )
    except Exception as e:
        print("[POST /telemetry] DB write error:", e)
        raise HTTPException(status_code=500, detail="DB write failed")
    
        # feed anomaly detector
    try:
        anomaly_detector.process_telemetry({
            "timestamp": ts,
            "battery": t.battery,
            "lat": t.lat,
            "lon": t.lon,
            "temperature": t.temperature,
            # altitude optional: t.altitude if you add it later
        })
    except Exception as e:
        print("[Anomaly] process_telemetry error (POST):", e)


    return {"status": "ok", "timestamp": ts}


# FastAPI lifecycle: startup/shutdown
@app.on_event("startup")
def startup_event():
    init_db(DB_PATH)
    try:
        app.state.mqtt_client = start_mqtt_background()
    except Exception as e:
        print("[startup] MQTT start failed:", e)

@app.on_event("shutdown")
def shutdown_event():
    client = getattr(app.state, "mqtt_client", None)
    if client:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

# Response model
class TelemetryOut(BaseModel):
    timestamp: Optional[str]
    battery: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    temperature: Optional[float]

@app.get("/telemetry/latest", response_model=TelemetryOut)
def get_latest():
    # 1) try in-memory latest
    with _latest_lock:
        if _latest_reading is not None:
            # return a shallow copy
            return dict(_latest_reading)

    # 2) fallback: query DB last row
    try:
        conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
        cur = conn.cursor()
        cur.execute("SELECT timestamp, battery, lat, lon, temperature FROM telemetry ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        if row:
            ts, bat, lat, lon, temp = row
            return {
                "timestamp": ts,
                "battery": bat,
                "lat": lat,
                "lon": lon,
                "temperature": temp
            }
    except Exception as e:
        print("[GET] DB fetch failed:", e)

    raise HTTPException(status_code=404, detail="No telemetry available yet.")

from typing import List, Optional
from pydantic import BaseModel

# Response model for a telemetry row
class TelemetryRow(BaseModel):
    id: int
    timestamp: Optional[str]
    device_id: Optional[str]
    battery: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    temperature: Optional[float]

# New endpoint: /telemetry/history?last=60
@app.get("/telemetry/history")
def telemetry_history(last: int = 60):
    """
    Returns telemetry rows from the last `last` seconds (time-based),
    using the timestamp column in the SQLite DB.
    """
    if last <= 0:
        raise HTTPException(status_code=400, detail="`last` must be > 0")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=last)
    cutoff_iso = cutoff.isoformat()

    try:
        conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
        cur = conn.cursor()
        # filter by timestamp >= cutoff, sorted oldest -> newest
        cur.execute("""
            SELECT id, timestamp, battery, lat, lon, temperature
            FROM telemetry
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """, (cutoff_iso,))
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print("[GET /telemetry/history] DB read error:", e)
        raise HTTPException(status_code=500, detail="DB read failed")

    result = []
    for r in rows:
        result.append({
            "id": r[0],
            "timestamp": r[1],
            "battery": r[2],
            "lat": r[3],
            "lon": r[4],
            "temperature": r[5],
        })
    return result

@app.get("/alerts")
def get_alerts(limit: int = 50):
    """
    Returns recent anomaly alerts generated by the AnomalyDetector.
    """
    try:
        alerts = anomaly_detector.get_alerts(limit=limit)
        return alerts
    except Exception as e:
        print("[GET /alerts] error:", e)
        raise HTTPException(status_code=500, detail="Could not fetch alerts")


@app.get("/replay/sample")
def replay_sample():
    """
    Returns sample telemetry sequence for frontend replay/demo.
    Data loaded from samples/replay_telemetry.json.
    """
    path = SAMPLES_DIR / "replay_telemetry.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="replay_telemetry.json not found")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print("[/replay/sample] Error reading sample file:", e)
        raise HTTPException(status_code=500, detail="Failed to read replay sample")

    # ensure it's a list of objects
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="replay_telemetry.json must contain a JSON array")

    return data
