import requests, time
from datetime import datetime, timezone

base = "http://localhost:8000"

for i in range(5):
    payload = {
        "battery": 95.0 - i,
        "lat": 17.446 + i*0.0001,
        "lon": 78.356 + i*0.0001,
        "temperature": 25.0 + i*0.2
        # timestamp omitted on purpose -> backend will fill
    }
    r = requests.post(base + "/telemetry", json=payload)
    print("POST status:", r.status_code, r.json())
    time.sleep(1)