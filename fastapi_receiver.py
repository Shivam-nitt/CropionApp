# fastapi_receiver.py
import json
import sqlite3
import threading
import os
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

# -------- CONFIG --------
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "device/telemetry"   # must match simulator topic

DB_PATH = "telemetry.db"          # reuse the DB you already have
SQLITE_TIMEOUT = 10
# ------------------------

app = FastAPI(title="Telemetry Receiver")

# In-memory latest reading with lock for thread-safety
_latest_lock = threading.Lock()
_latest_reading: Optional[dict] = None

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
    conn.close()

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

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected to broker.")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Subscribed to {MQTT_TOPIC}")
    else:
        print("[MQTT] Failed to connect, rc=", rc)

def on_message(client, userdata, msg):
    global _latest_reading
    payload_text = None
    try:
        payload_text = msg.payload.decode("utf-8")
        parsed = json.loads(payload_text)
    except Exception as e:
        print("[MQTT] Failed to decode/parse payload:", e, "raw:", payload_text)
        return

    # Extract fields
    # Accept either "temperature" or "temp"
    battery = parsed.get("battery")
    temperature = parsed.get("temperature", parsed.get("temp"))
    # GPS: either lat/lon or gps: [lat, lon]
    lat = parsed.get("lat")
    lon = parsed.get("lon")
    if (lat is None or lon is None) and isinstance(parsed.get("gps"), (list, tuple)):
        gps = parsed.get("gps")
        if len(gps) >= 2:
            lat, lon = gps[0], gps[1]
    # timestamp: prefer publisher timestamp, else receiver time
    timestamp = parsed.get("timestamp")
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()

    # Try to coerce numeric values, fall back to None
    def to_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    battery = to_float(battery)
    temperature = to_float(temperature)
    lat = to_float(lat)
    lon = to_float(lon)

    # Insert into DB (short-lived connection)
    try:
        insert_row(DB_PATH, timestamp, battery, lat, lon, temperature)
    except Exception as e:
        print("[DB] insert failed:", e)

    # Update in-memory latest reading (atomic)
    with _latest_lock:
        _latest_reading = {
            "timestamp": timestamp,
            "battery": battery,
            "lat": lat,
            "lon": lon,
            "temperature": temperature
        }

    print("[MQTT] Received and stored:", _latest_reading)

# Start MQTT client in background (non-blocking)
def start_mqtt_background():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client

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
