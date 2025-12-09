# uploader.py
# Chunked uploader with resume logic for the mock server above.
# Usage: python uploader.py <path-to-file> --server http://localhost:9000

import os
import sys
import time
import json
import hashlib
import argparse
import requests
from tqdm import tqdm

DEFAULT_SERVER = "http://localhost:9000"
CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB
PROGRESS_META_EXT = ".uploadmeta.json"
RETRY_BACKOFF = [1, 2, 5, 10]  # seconds


def md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_meta(meta_path: str):
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_meta(meta_path: str, meta: dict):
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f)


def initiate_upload(server: str, filename: str):
    url = server.rstrip("/") + "/upload/initiate"
    resp = requests.post(url, json={"filename": filename})
    resp.raise_for_status()
    return resp.json()  # has upload_id, chunk_size


def get_uploaded_chunks(server: str, upload_id: str):
    url = server.rstrip("/") + f"/upload/{upload_id}/status"
    resp = requests.get(url)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json().get("uploaded_chunks", [])


def upload_chunk(server: str, upload_id: str, index: int, data: bytes):
    url = server.rstrip("/") + f"/upload/{upload_id}/chunk/{index}"
    files = {"file": ("chunk", data)}
    resp = requests.put(url, files=files, timeout=60)
    resp.raise_for_status()
    return resp.json()


def complete_upload(server: str, upload_id: str):
    url = server.rstrip("/") + f"/upload/{upload_id}/complete"
    resp = requests.post(url)
    resp.raise_for_status()
    return resp.json()


def run_upload(file_path: str, server: str, max_chunks: int | None = None) -> bool:
    """
    Uploads file in chunks with resume support.

    If max_chunks is not None, stops after sending that many new chunks
    (for demo: simulating network drop).
    """
    file_size = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    meta_path = file_path + PROGRESS_META_EXT

    # Compute file checksum to detect file changes
    file_md5 = md5_of_file(file_path)

    # Load meta or initiate new
    meta = load_meta(meta_path)
    if meta and meta.get("file_md5") != file_md5:
        print("File changed since previous upload meta. Removing old meta and starting new.")
        meta = None
        try:
            os.remove(meta_path)
        except Exception:
            pass

    if not meta:
        info = initiate_upload(server, filename)
        upload_id = info["upload_id"]
        chunk_size = info.get("chunk_size", CHUNK_SIZE)
        meta = {
            "upload_id": upload_id,
            "chunk_size": chunk_size,
            "file_size": file_size,
            "filename": filename,
            "file_md5": file_md5,
        }
        save_meta(meta_path, meta)
        print("Initiated upload:", upload_id)
    else:
        upload_id = meta["upload_id"]
        chunk_size = meta["chunk_size"]
        print("Resuming upload:", upload_id)

    total_chunks = (file_size + chunk_size - 1) // chunk_size

    # Ask server for already uploaded chunks
    try:
        uploaded = set(get_uploaded_chunks(server, upload_id))
    except Exception as e:
        print("Could not query server status:", e)
        uploaded = set()

    # Prepare progress bar starting with count of already uploaded chunks
    done_chunks = len(uploaded)
    pbar = tqdm(total=total_chunks, desc="Chunks", initial=done_chunks, unit="chunk")

    chunks_sent = 0  # number of new chunks sent in THIS run

    with open(file_path, "rb") as f:
        for idx in range(total_chunks):
            if idx in uploaded:
                # already uploaded earlier; just advance progress bar
                pbar.update(1)
                continue

            # simulate network drop after some new chunks, if requested
            if max_chunks is not None and chunks_sent >= max_chunks:
                print(f"[uploader] Reached max_chunks={max_chunks}, simulating network drop.")
                pbar.close()
                return False

            # read this chunk from file
            f.seek(idx * chunk_size)
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break  # EOF

            # attempt upload with retries
            for attempt, backoff in enumerate(RETRY_BACKOFF, start=1):
                try:
                    upload_chunk(server, upload_id, idx, chunk_data)
                    uploaded.add(idx)
                    save_meta(meta_path, meta)  # persist meta (upload id etc)
                    pbar.update(1)
                    chunks_sent += 1
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Chunk {idx} upload attempt {attempt} failed: {e}. Retrying in {backoff}s")
                    time.sleep(backoff)
            else:
                # all retries failed for this chunk
                print(f"Chunk {idx} failed after retries, exiting for resume later.")
                pbar.close()
                return False

    pbar.close()

    # All chunks uploaded. Call complete
    try:
        res = complete_upload(server, upload_id)
        print("Upload complete:", res)
        # cleanup meta file
        try:
            os.remove(meta_path)
        except Exception:
            pass
        return True
    except Exception as e:
        print("Complete failed:", e)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chunked uploader with resume")
    parser.add_argument("file", help="Path to file to upload")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Upload server base URL")
    parser.add_argument("--max-chunks", type=int, default=None,
                        help="If set, stop after sending this many chunks (simulate network drop)")
    args = parser.parse_args()

    ok = run_upload(args.file, args.server, max_chunks=args.max_chunks)
    if ok:
        print("Upload succeeded.")
    else:
        print("Upload incomplete; run again to resume.")
