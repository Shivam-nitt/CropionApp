# backend/anomaly_detection.py

from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from threading import Lock, Thread
from typing import Dict, List, Optional
import time


@dataclass
class Alert:
    timestamp: str         # ISO time when alert raised
    device_id: str
    type: str              # e.g. "battery_drop", "battery_low", "gps_loss", "altitude_spike"
    message: str
    details: dict


class AnomalyDetector:
    """
    In-memory anomaly detector.

    - Called whenever new telemetry arrives (from MQTT or POST /telemetry).
    - Maintains short recent history per device.
    - Evaluates rules and stores alerts.
    - Background thread checks GPS loss (>3s without new telemetry).
    """

    def __init__(self):
        # history[device_id] = list of telemetry dicts (recent ~20s)
        self.history: Dict[str, List[dict]] = {}
        # last telemetry time per device
        self.last_ts: Dict[str, datetime] = {}
        # last gps_loss alert time per device (to avoid spamming)
        self.gps_lost: Dict[str, bool] = {}
        # collected alerts
        self._alerts: List[Alert] = []
        self._lock = Lock()
        self._gps_thread_started = False

    # --------------- public API -----------------

    def process_telemetry(self, t: dict):
        """
        t should have:
          - timestamp (ISO str)
          - battery (float)
          - lat, lon (float, optional)
          - altitude (float, optional)
          - device_id (optional, defaults to 'default')
        """
        device_id = t.get("device_id") or "default"

        # parse timestamp
        ts_str = t.get("timestamp")
        if ts_str is None:
            now = datetime.now(timezone.utc)
            ts = now
            ts_str = now.isoformat()
            t["timestamp"] = ts_str
        else:
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                ts = datetime.now(timezone.utc)
                t["timestamp"] = ts.isoformat()

        with self._lock:
            # update last telemetry time
            self.last_ts[device_id] = ts
            #mark device as not lost since we just got telemetry
            self.gps_lost[device_id] = False
            # update history
            hist = self.history.setdefault(device_id, [])
            hist.append(t)
            # keep only last 20 seconds of history
            cutoff = ts - timedelta(seconds=20)
            self.history[device_id] = [e for e in hist if self._parse_ts(e["timestamp"]) >= cutoff]

            # evaluate rules based on history
            self._check_battery_rules(device_id)
            self._check_altitude_rule(device_id)

    def get_alerts(self, limit: int = 50) -> List[dict]:
        with self._lock:
            recent = self._alerts[-limit:]
            return [asdict(a) for a in recent]

    def start_gps_monitor(self):
        """
        Start a background thread that checks for GPS loss:
        - if no telemetry received for >3s => gps_loss alert
        Only starts once.
        """
        if self._gps_thread_started:
            return
        self._gps_thread_started = True
        thread = Thread(target=self._gps_monitor_loop, daemon=True)
        thread.start()

    # --------------- internal helpers -----------------

    def _gps_monitor_loop(self):
        while True:
            now = datetime.now(timezone.utc)
            with self._lock:
                for device_id, last_ts in list(self.last_ts.items()):
                    delta = (now - last_ts).total_seconds()

                    # if silence > 3s and we weren't already "lost" → raise alert ONCE
                    if delta > 3.0:
                        if not self.gps_lost.get(device_id, False):
                            self._add_alert(
                                device_id=device_id,
                                type_="gps_loss",
                                message=f"GPS/telemetry silence for {delta:.1f} seconds",
                                details={"silence_seconds": delta},
                            )
                            # mark as lost so we don't spam
                            self.gps_lost[device_id] = True
                    else:
                        # telemetry is flowing again → clear lost flag
                        self.gps_lost[device_id] = False

            time.sleep(1.0)


    def _parse_ts(self, ts_str: str) -> datetime:
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    def _add_alert(self, device_id: str, type_: str, message: str, details: dict):
        now = datetime.now(timezone.utc).isoformat()
        alert = Alert(
            timestamp=now,
            device_id=device_id,
            type=type_,
            message=message,
            details=details or {},
        )
        self._alerts.append(alert)
        # simple log
        print(f"[ALERT][{type_}] {message} | {details}")

    # ----- rules -----

    def _check_battery_rules(self, device_id: str):
        hist = self.history.get(device_id, [])
        if not hist:
            return

        latest = hist[-1]
        batt = latest.get("battery")
        if batt is None:
            return

        # Rule 1: battery < 20%
        if batt < 20.0:
            self._add_alert(
                device_id=device_id,
                type_="battery_low",
                message=f"Battery low: {batt:.1f}%",
                details={"battery": batt},
            )

        # Rule 2: battery drop > 15% in 10s
        latest_ts = self._parse_ts(latest["timestamp"])
        window_start = latest_ts - timedelta(seconds=10)
        # find oldest sample within last 10s window
        window_samples = [e for e in hist if self._parse_ts(e["timestamp"]) >= window_start]
        if not window_samples:
            return
        first = window_samples[0]
        first_batt = first.get("battery")
        if first_batt is None:
            return

        drop = first_batt - batt
        if drop > 15.0:
            self._add_alert(
                device_id=device_id,
                type_="battery_drop",
                message=f"Battery dropped {drop:.1f}% in 10s",
                details={"from": first_batt, "to": batt, "drop": drop},
            )

    def _check_altitude_rule(self, device_id: str):
        hist = self.history.get(device_id, [])
        if len(hist) < 2:
            return

        latest = hist[-1]
        prev = hist[-2]

        alt = latest.get("altitude")
        prev_alt = prev.get("altitude")
        if alt is None or prev_alt is None:
            return

        t1 = self._parse_ts(prev["timestamp"])
        t2 = self._parse_ts(latest["timestamp"])
        dt = (t2 - t1).total_seconds()
        if dt <= 0:
            return

        rate = (alt - prev_alt) / dt  # m/s
        if abs(rate) > 10.0:
            self._add_alert(
                device_id=device_id,
                type_="altitude_spike",
                message=f"Altitude rate {rate:.1f} m/s exceeds 10 m/s",
                details={"rate_m_per_s": rate, "from": prev_alt, "to": alt, "dt": dt},
            )
