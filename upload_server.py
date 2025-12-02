# upload_server.py
# Simple FastAPI mock server to accept chunked uploads and reassemble them.
# Usage: python -m uvicorn upload_server:app --host 0.0.0.0 --port 9000

import os
import shutil
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import uuid

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_ROOT = os.path.join(BASE_DIR, "uploads")  # where chunks and final files live
os.makedirs(UPLOAD_ROOT, exist_ok=True)

app = FastAPI(title="Mock S3 Chunk Server")

class InitiateRequest(BaseModel):
    filename: str

class InitiateResponse(BaseModel):
    upload_id: str
    chunk_size: int

CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB

def upload_dir(upload_id: str) -> str:
    return os.path.join(UPLOAD_ROOT, upload_id)

@app.post("/upload/initiate", response_model=InitiateResponse)
def initiate(req: InitiateRequest):
    """
    Start a new upload session. Server creates a folder for chunks.
    Returns an upload_id and chunk_size (client should use this).
    """
    uid = str(uuid.uuid4())
    d = upload_dir(uid)
    os.makedirs(d, exist_ok=True)
    # store original filename for later assembly
    with open(os.path.join(d, "meta_filename.txt"), "w", encoding="utf-8") as f:
        f.write(req.filename)
    return InitiateResponse(upload_id=uid, chunk_size=CHUNK_SIZE)

@app.get("/upload/{upload_id}/status")
def status(upload_id: str):
    """
    Return list of chunk indices already uploaded for this upload_id.
    """
    d = upload_dir(upload_id)
    if not os.path.exists(d):
        raise HTTPException(status_code=404, detail="upload_id not found")
    # chunk files are saved as chunk_{index}.part
    uploaded = []
    for name in os.listdir(d):
        if name.startswith("chunk_") and name.endswith(".part"):
            try:
                idx = int(name.split("_")[1].split(".")[0])
                uploaded.append(idx)
            except Exception:
                continue
    uploaded.sort()
    return {"uploaded_chunks": uploaded}

@app.put("/upload/{upload_id}/chunk/{index}")
async def upload_chunk(upload_id: str, index: int, file: UploadFile):
    """
    Upload a single chunk with index (0-based).
    The request body should be the binary chunk.
    """
    d = upload_dir(upload_id)
    if not os.path.exists(d):
        raise HTTPException(status_code=404, detail="upload_id not found")
    # Save chunk to disk as chunk_{index}.part
    chunk_path = os.path.join(d, f"chunk_{index}.part")
    # If chunk already exists, overwrite (idempotent)
    try:
        with open(chunk_path, "wb") as out_f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                out_f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"write error: {e}")
    return JSONResponse({"status": "ok", "index": index})

@app.post("/upload/{upload_id}/complete")
def complete(upload_id: str):
    """
    Assemble chunks (in order) into final file and remove chunk parts.
    Requires meta_filename.txt to exist.
    """
    d = upload_dir(upload_id)
    if not os.path.exists(d):
        raise HTTPException(status_code=404, detail="upload_id not found")

    # find chunk indices
    chunk_files = []
    for name in os.listdir(d):
        if name.startswith("chunk_") and name.endswith(".part"):
            try:
                idx = int(name.split("_")[1].split(".")[0])
                chunk_files.append((idx, name))
            except Exception:
                continue
    if not chunk_files:
        raise HTTPException(status_code=400, detail="no chunks uploaded")

    chunk_files.sort(key=lambda x: x[0])
    # read filename from meta
    with open(os.path.join(d, "meta_filename.txt"), "r", encoding="utf-8") as f:
        original_name = f.read().strip() or "assembled.bin"

    final_path = os.path.join(UPLOAD_ROOT, f"{upload_id}__{original_name}")
    try:
        with open(final_path, "wb") as out_f:
            for idx, fname in chunk_files:
                part_path = os.path.join(d, fname)
                with open(part_path, "rb") as pf:
                    shutil.copyfileobj(pf, out_f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"assemble error: {e}")

    # Optionally, cleanup chunk files and folder
    try:
        shutil.rmtree(d)
    except Exception:
        pass

    return {"status": "assembled", "final_path": final_path}
