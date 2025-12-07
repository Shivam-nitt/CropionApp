# perf_simulator.py
# Publishes telemetry (1 Hz) + frame messages (FPS) for performance testing.

import time, json, argparse, random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

def iso_now():
    return datetime.now(timezone.utc).isoformat()

parser = argparse.ArgumentParser()
parser.add_argument("--broker", default="localhost")
parser.add_argument("--port", type=int, default=1883)
parser.add_argument("--duration", type=int, default=60)
parser.add_argument("--frame_fps", type=float, default=10.0)
parser.add_argument("--telemetry_hz", type=float, default=1.0)
parser.add_argument("--device_id", default="sim-001")
args = parser.parse_args()

client = mqtt.Client()
client.connect(args.broker, args.port, 60)
client.loop_start()

end_time = time.time() + args.duration

tele_interval = 1.0 / args.telemetry_hz
frame_interval = 1.0 / args.frame_fps

next_tele = time.time()
next_frame = time.time()

frame_id = 0

print(f"Running perf simulator for {args.duration}s: {args.frame_fps} FPS, {args.telemetry_hz} Hz telemetry")

while time.time() < end_time:
    now = time.time()

    # --- Telemetry (1Hz or configured) ---
    if now >= next_tele:
        telemetry = {
            "device_id": args.device_id,
            "timestamp": iso_now(),
            "battery": round(100 - random.random()*0.5, 2),
            "temperature": round(25 + random.uniform(-1,1), 2),
            "lat": 17.446 + random.uniform(-0.001, 0.001),
            "lon": 78.356 + random.uniform(-0.001, 0.001)
        }
        client.publish("device/telemetry", json.dumps(telemetry), qos=1)
        next_tele += tele_interval

    # --- Frames (FPS) ---
    if now >= next_frame:
        frame_id += 1
        frame_msg = {
            "device_id": args.device_id,
            "frame_id": frame_id,
            "timestamp": iso_now()
        }
        client.publish("device/frame", json.dumps(frame_msg), qos=1)
        next_frame += frame_interval

    time.sleep(0.0005)

client.loop_stop()
client.disconnect()
print(f"Done. Total frames published: {frame_id}")
