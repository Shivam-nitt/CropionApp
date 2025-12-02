# simulator.py
import time
import json
import random
import csv
import os
import sqlite3
import paho.mqtt.client as mqtt

# ---------- CONFIG ----------
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "device/telemetry"

DB_PATH = "telemetry.db"               # stored in current folder
CSV_PATH = "telemetry_log.csv"         # stored in current folder
SEND_INTERVAL_SEC = 1                  # 1 Hz
TOTAL_MESSAGES = 80                    # change to 60-100 as you want
KEEP_LAST_N = 100                      # keep last 100 rows in sqlite
DEVICE_ID = "sim-001"
# -------------------------------

def get_iso_timestamp():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def generate_telemetry(prev_battery=None):
    if prev_battery is None:
        battery = round(random.uniform(90, 100), 2)
    else:
        battery = max(0.0, round(prev_battery - random.uniform(0.0, 0.2), 2))
    base_lat, base_lon = 17.447, 78.356
    lat = round(base_lat + random.uniform(-0.0005, 0.0005), 6)
    lon = round(base_lon + random.uniform(-0.0005, 0.0005), 6)
    temperature = round(30 + random.uniform(-2, 2), 2)

    payload = {
        "device_id": DEVICE_ID,
        "timestamp": get_iso_timestamp(),
        "battery": battery,
        "temperature": temperature,
        "lat": lat,
        "lon": lon
    }
    return payload, battery

def init_sqlite(path):
    conn = sqlite3.connect(path, timeout=10)
    cur = conn.cursor()
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
    return conn

def init_csv(path):
    need_header = not os.path.exists(path)
    f = open(path, mode="a", newline="", encoding="utf-8")
    writer = csv.writer(f)
    if need_header:
        writer.writerow(["timestamp", "device_id", "battery", "temperature", "lat", "lon"])
        f.flush()
    return f, writer

def main():
    # Init DB + CSV
    conn = init_sqlite(DB_PATH)
    cur = conn.cursor()
    csv_file, csv_writer = init_csv(CSV_PATH)

    # MQTT client
    client = mqtt.Client()
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print("MQTT connect failed:", e)
        print("Make sure mosquitto is running and reachable at", MQTT_BROKER, MQTT_PORT)
        return
    client.loop_start()

    battery = None
    try:
        for i in range(TOTAL_MESSAGES):
            payload, battery = generate_telemetry(battery)
            payload_json = json.dumps(payload)

            # Publish
            result = client.publish(MQTT_TOPIC, payload_json)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print("Publish failed rc=", result.rc)
            else:
                print(f"Published #{i+1}: {payload_json}")

            # SQLite insert
            cur.execute("""
                INSERT INTO telemetry (timestamp, battery, lat, lon, temperature)
                VALUES (?, ?, ?, ?, ?)
            """, (payload["timestamp"], payload["battery"], payload["lat"], payload["lon"], payload["temperature"]))
            conn.commit()

            # CSV append
            csv_writer.writerow([payload["timestamp"], payload["device_id"], payload["battery"], payload["temperature"], payload["lat"], payload["lon"]])
            csv_file.flush()

            # Trim DB to KEEP_LAST_N rows
            cur.execute("DELETE FROM telemetry WHERE id NOT IN (SELECT id FROM telemetry ORDER BY id DESC LIMIT ?)", (KEEP_LAST_N,))
            conn.commit()

            time.sleep(SEND_INTERVAL_SEC)

        print(f"Finished sending {TOTAL_MESSAGES} messages.")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        client.loop_stop()
        client.disconnect()
        csv_file.close()
        conn.close()
        print("Closed MQTT, CSV and DB connections.")

if __name__ == "__main__":
    main()
