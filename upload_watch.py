#!/usr/bin/env python3
"""
Watch DROP_DIR for new videos named like:  email@example.com__anything.mp4
Then:
  - upload to R2 as videos/<safe_email>__<rest>
  - write pointer JSON to pointers/<safe_email>.json (via system temp)
"""

import os, time, json, subprocess, sys, shutil, tempfile
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Config from env ---
DROP_DIR    = Path(os.getenv("DROP_DIR", str(Path.home() / "Drop Videos Here")))
R2_BUCKET   = os.getenv("R2_BUCKET", "samples")
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://matlycreative.pages.dev")

# rclone binary (absolute) so launchd finds it
RCLONE_BIN = os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/opt/homebrew/bin/rclone"

# Only process these video extensions (lowercase, include the dot)
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi"}

# Keep track of files already being/been processed in this run
PROCESSING = set()

def safe_id(email: str) -> str:
    return (email or "").lower().replace("@", "_").replace(".", "_")

def run(cmd):
    print(">", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)

def done_writing(p: Path, wait_sec: float = 1.2) -> bool:
    """Heuristic: size stable for ~1.2s."""
    if not p.exists() or not p.is_file():
        return False
    s1 = p.stat().st_size
    time.sleep(wait_sec)
    s2 = p.stat().st_size
    return s1 == s2 and s1 > 0

def derive_company(email: str) -> str:
    if "@" not in (email or ""):
        return ""
    domain = email.split("@", 1)[1]
    base = domain.split(".", 1)[0].replace("-", " ").replace("_", " ")
    return base.capitalize()

def process_file(f: Path):
    # Debounce: avoid double-processing same path
    key = str(f.resolve())
    if key in PROCESSING:
        return
    PROCESSING.add(key)

    try:
        if not done_writing(f):
            print(f"[wait] {f.name} still writing…")
            time.sleep(1.0)
            if not done_writing(f):
                print(f"[skip] {f.name} not stable yet.")
                return

        base = f.name  # e.g. jane@acme.com__tour.mp4
        if "__" not in base:
            print(f"[skip] {base}: expected 'email__something.ext'")
            return

        email = base.split("__", 1)[0]
        rest  = base.split("__", 1)[1]
        if not any(base.lower().endswith(ext) for ext in VIDEO_EXTS):
            print(f"[skip] {base}: not a supported video extension")
            return

        s       = safe_id(email)                   # jane_acme_com
        vid_key = f"videos/{s}__{rest}"            # videos/jane_acme_com__tour.mp4

        # Upload the video (flat key). copyto = file upload (no phantom folder).
        run([RCLONE_BIN, "copyto", str(f), f"r2:{R2_BUCKET}/{vid_key}", "-vv"])

        # Build pointer JSON IN A TEMP FILE (outside watched folder)
        company = derive_company(email)
        pointer = {"key": vid_key, "company": company}

        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as t:
            tmp_path = Path(t.name)
            json.dump(pointer, t)
        try:
            run([RCLONE_BIN, "copyto", str(tmp_path), f"r2:{R2_BUCKET}/pointers/{s}.json", "-vv"])
        finally:
            try: tmp_path.unlink()
            except Exception: pass

        print(f"[ok] Uploaded → r2:{R2_BUCKET}/{vid_key}")
        print(f"[ok] Pointer  → r2:{R2_BUCKET}/pointers/{s}.json")
        print(f"[info] Landing → {PUBLIC_BASE}/p/?id={s}")

    finally:
        # allow future runs for same path if needed
        PROCESSING.discard(key)

class Handler(FileSystemEventHandler):
    def on_created(self, e):
        p = Path(e.src_path)
        self._maybe(p)

    # We do NOT react to on_modified to prevent loops/flapping
    # def on_modified(self, e): pass

    def _maybe(self, p: Path):
        name = p.name
        if not p.is_file():
            return
        if name.startswith("."):
            return
        low = name.lower()
        if low.endswith(".json") or low.endswith(".pointer.json") or low.endswith(".tmp") or low.endswith(".part"):
            return
        if "__" not in name:
            return
        process_file(p)

def main():
    print(f"[watcher] rclone: {RCLONE_BIN}")
    if not Path(RCLONE_BIN).exists():
        print("[error] rclone not found — set RCLONE_BIN env var to its full path.")
        sys.exit(1)

    DROP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[watching] {DROP_DIR}")

    obs = Observer()
    obs.schedule(Handler(), str(DROP_DIR), recursive=False)
    obs.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()

if __name__ == "__main__":
    try:
        import watchdog  # ensure installed
    except Exception:
        print("Install watchdog:  pip install watchdog")
        sys.exit(1)
    main()
