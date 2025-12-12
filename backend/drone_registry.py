# backend/drone_registry.py
"""
Lightweight Drone Registry + JWT placeholder for demo.

Endpoints provided:
- POST /auth/token           -> Accepts {"username":"...", "password":"..."} and returns a JWT (demo only)
- POST /drones/register     -> Register a drone (requires Bearer token)
- DELETE /drones/{drone_id} -> Unregister a drone (requires Bearer token)
- GET  /drones              -> List drones (requires Bearer token)

This module uses the same SQLite DB as other backend components:
    backend/telemetry.db

It ensures table `drones` exists on import.
"""

from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3
from typing import Optional, Dict, Any, List

import jwt  # PyJWT
from fastapi import APIRouter, Depends, HTTPException, status, Header, Request
from pydantic import BaseModel, Field

# JWT settings (demo). In production, keep secret in env/secret manager
JWT_SECRET = os.environ.get("DEMO_JWT_SECRET", "dev-secret-change-me")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours for demo

# DB path (same DB as telemetry)
from pathlib import Path
BACKEND_DIR = Path(__file__).resolve().parent
DB_PATH = BACKEND_DIR / "telemetry.db"
SQLITE_TIMEOUT = 10

router = APIRouter()


# --------------------------
# DB helper / migration
# --------------------------
def ensure_drones_table():
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS drones (
            id TEXT PRIMARY KEY,
            owner TEXT,
            metadata TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


# run migration on import
ensure_drones_table()


# --------------------------
# Pydantic models
# --------------------------
class TokenRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: str


class DroneRegisterRequest(BaseModel):
    id: str = Field(..., description="Unique drone id (UUID recommended)")
    owner: str = Field(..., description="Owner identifier (user id / email)")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class DroneRecord(BaseModel):
    id: str
    owner: str
    metadata: Dict[str, Any]
    created_at: str


# --------------------------
# JWT helpers (placeholder)
# --------------------------
def create_access_token(subject: str, extra: Optional[dict] = None, expire_minutes: int = ACCESS_TOKEN_EXPIRE_MINUTES):
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=expire_minutes)
    payload = {"sub": subject, "iat": int(now.timestamp()), "exp": int(exp.timestamp())}
    if extra:
        payload.update(extra)
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    # PyJWT on some versions returns bytes; ensure str
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token, exp.isoformat()


def verify_token(token: str) -> dict:
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return data
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# --------------------------
# Dependencies
# --------------------------
def get_bearer_token(authorization: Optional[str] = Header(None)):
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization header missing")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid auth header")
    return authorization.split(" ", 1)[1].strip()


def get_current_user(token: str = Depends(get_bearer_token)):
    payload = verify_token(token)
    # For demo, subject is username
    subject = payload.get("sub")
    if not subject:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
    return {"sub": subject, **payload}


# --------------------------
# Routes
# --------------------------
@router.post("/auth/token", response_model=TokenResponse)
def auth_token(req: TokenRequest):
    """
    Demo token endpoint. Accepts any username/password (placeholder) and returns a JWT.
    Replace with proper credential validation in production.
    """
    # Placeholder: accept any credentials for demo. To change, validate here.
    username = req.username
    extra = {"role": "demo-user"}
    token, exp_iso = create_access_token(subject=username, extra=extra)
    return TokenResponse(access_token=token, expires_at=exp_iso)


@router.post("/drones/register", response_model=DroneRecord)
def drone_register(req: DroneRegisterRequest, user=Depends(get_current_user)):
    """
    Register a drone. Requires Bearer token.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    metadata_json = json.dumps(req.metadata, ensure_ascii=False)
    try:
        cur.execute(
            "INSERT INTO drones (id, owner, metadata, created_at) VALUES (?, ?, ?, ?)",
            (req.id, req.owner, metadata_json, now_iso),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="Drone with this id already registered")
    # return record
    record = {"id": req.id, "owner": req.owner, "metadata": req.metadata, "created_at": now_iso}
    conn.close()
    return record


@router.delete("/drones/{drone_id}", status_code=204)
def drone_unregister(drone_id: str, user=Depends(get_current_user)):
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute("DELETE FROM drones WHERE id = ?", (drone_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Drone not found")
    return {}


@router.get("/drones", response_model=List[DroneRecord])
def drone_list(user=Depends(get_current_user)):
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
    cur = conn.cursor()
    cur.execute("SELECT id, owner, metadata, created_at FROM drones ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows:
        meta = {}
        try:
            meta = json.loads(r[2]) if r[2] else {}
        except Exception:
            meta = {}
        out.append(DroneRecord(id=r[0], owner=r[1], metadata=meta, created_at=r[3]))
    return out
