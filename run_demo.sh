#!/usr/bin/env bash
set -euo pipefail

# ------------------------------
# Single-run demo launcher
# Starts:
#  - FastAPI backend
#  - Upload server
#  - NDVI viewer (frontend)
#  - Telemetry + frame simulator
# Logs go into ./logs
# ------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "[run_demo] Root: $ROOT_DIR"
echo "[run_demo] Logs: $LOG_DIR"

# -----------------------------------------------------
# 1) Start FastAPI backend (Uvicorn)
# -----------------------------------------------------
echo "[run_demo] Starting FastAPI backend..."
python -m uvicorn backend.fastapi_receiver:app \
  --host 0.0.0.0 --port 8000 --reload \
  > "$LOG_DIR/backend.log" 2>&1 &

BACKEND_PID=$!
echo "[run_demo] FastAPI backend PID = $BACKEND_PID (log: logs/backend.log)"

# -----------------------------------------------------
# 2) Start upload server (mock S3 / Firebase)
#    NOTE: change 'upload_server.py' if your script name is different
# -----------------------------------------------------
echo "[run_demo] Starting upload server..."
python -m uvicorn backend.upload_server:app --host 0.0.0.0 --port 9000 
  > "$LOG_DIR/upload_server.log" 2>&1 &

UPLOAD_PID=$!
echo "[run_demo] Upload server PID = $UPLOAD_PID (log: logs/upload_server.log)"

# -----------------------------------------------------
# 3) Start NDVI viewer (frontend)
#    NOTE: replace this command with your actual viewer launcher.
#    Examples:
#      - python frontend/ndvi_viewer.py
#      - ./frontend/ndvi_viewer.exe  (on Windows, started via wine or similar)
#      - qmlscene frontend/main.qml
# -----------------------------------------------------
echo "[run_demo] Starting NDVI viewer (frontend)..."

# Placeholder command - CHANGE THIS to your real viewer
# For now we just log a placeholder to show it's wired in.
echo "[run_demo] (TODO) replace this with actual NDVI viewer command" \
  > "$LOG_DIR/ndvi_viewer.log" 2>&1 &

NDVI_PID=$!
echo "[run_demo] NDVI viewer PID = $NDVI_PID (log: logs/ndvi_viewer.log)"

# -----------------------------------------------------
# 4) Run onboard simulator (telemetry + frames)
#    This one we usually run in the foreground so the demo
#    ends when simulation completes.
# -----------------------------------------------------
echo "[run_demo] Starting onboard simulator (perf_simulator)..."
python onboard/perf_simulator.py \
  --duration 60 \
  --frame_fps 5 \
  --telemetry_hz 1 \
  > "$LOG_DIR/simulator.log" 2>&1

echo "[run_demo] Simulator finished. Check logs/simulator.log."

# -----------------------------------------------------
# 5) Shutdown background services
# -----------------------------------------------------
echo "[run_demo] Stopping background services..."
kill "$BACKEND_PID" || true
kill "$UPLOAD_PID" || true
kill "$NDVI_PID" || true

echo "[run_demo] Demo completed. Logs available in ./logs"
