# backend/demo_upload_resume.py

import subprocess
import sys
from pathlib import Path
import hashlib
import time

ROOT_DIR = Path(__file__).resolve().parents[1]
ONBOARD_DIR = ROOT_DIR / "onboard"
BACKEND_DIR = ROOT_DIR / "backend"
SAMPLES_DIR = ROOT_DIR / "samples"
UPLOADS_DIR = BACKEND_DIR / "uploads"

SERVER_URL = "http://localhost:9000"
TEST_FILENAME = "big_test.bin"  # sample file to upload


def md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def pick_server_file() -> Path | None:
    """
    Picks the most recently modified file in UPLOADS_DIR.
    This assumes the latest upload we just completed is the newest file.
    """
    if not UPLOADS_DIR.exists():
        return None
    files = [p for p in UPLOADS_DIR.iterdir() if p.is_file()]
    if not files:
        return None
    # pick newest by modification time
    return max(files, key=lambda p: p.stat().st_mtime)


def main():
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

    src_file = SAMPLES_DIR / TEST_FILENAME
    if not src_file.exists():
        print(f"[demo] Sample file not found: {src_file}")
        sys.exit(1)

    print(f"[demo] Using source file: {src_file}")
    print(f"[demo] Uploads dir: {UPLOADS_DIR}")

    # 1) Clean uploads dir (optional, but makes it clear)
    for f in UPLOADS_DIR.glob("*"):
        try:
            print(f"[demo] Removing old file in uploads: {f}")
            f.unlink()
        except Exception as e:
            print(f"[demo] Could not remove {f}: {e}")

    # 2) Partial upload (simulate network drop)
    print("[demo] Starting partial upload (max_chunks=3)...")
    cmd1 = [
        sys.executable,
        str(ONBOARD_DIR / "uploader.py"),
        str(src_file),
        "--server", SERVER_URL,
        "--max-chunks", "3",
    ]
    print("[demo] Running:", " ".join(cmd1))
    subprocess.run(cmd1, check=False)

    time.sleep(1.0)

    # 3) Resume and complete
    print("[demo] Resuming upload (full run)...")
    cmd2 = [
        sys.executable,
        str(ONBOARD_DIR / "uploader.py"),
        str(src_file),
        "--server", SERVER_URL,
    ]
    print("[demo] Running:", " ".join(cmd2))
    subprocess.run(cmd2, check=True)

    # 4) Find server file
    server_file = pick_server_file()
    if not server_file:
        print("[demo] ERROR: No file found in uploads dir after resume.")
        sys.exit(1)

    print(f"[demo] Detected server file: {server_file}")

    # 5) Compare MD5
    src_md5 = md5_of_file(src_file)
    dest_md5 = md5_of_file(server_file)

    print(f"[demo] Source MD5: {src_md5}")
    print(f"[demo] Server MD5: {dest_md5}")

    if src_md5 == dest_md5:
        print("[demo] ✅ Resume success: server file matches original.")
        sys.exit(0)
    else:
        print("[demo] ❌ Mismatch: server file does NOT match original.")
        sys.exit(2)


if __name__ == "__main__":
    main()
