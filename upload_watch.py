#!/usr/bin/env python3
"""
Drag files into DROP_DIR and they are:
  - uploaded to R2 as videos/<safe>__<rest>
  - pointer JSON written to pointers/<safe>.json (created in /tmp then uploaded)

Requires: pip install watchdog
          rclone configured with a remote named 'r2'
Env:
  R2_BUCKET       (e.g. 'samples')
  PUBLIC_BASE     (e.g. 'https://matlycreative.pages.dev') only for notices
"""
import os, time, json, subprocess, sys, shutil, tempfile
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

DROP_DIR    = Path(os.getenv("DROP_DIR", str(Path.home() / "Drop Videos Here")))
R2_BUCKET   = os.getenv("R2_BUCKET", "samples")
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://matlycreative.pages.dev")
RCLONE_BIN  = os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/opt/homebrew/bin/rclone"

VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".mkv", ".webm"}

def safe_id(email:str)->str:
    return email.lower().replace("@","_").replace(".","_")

def run(cmd):
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)

def done_writing(p:Path)->bool:
    if not p.exists() or not p.is_file(): return False
    s1 = p.stat().st_size; time.sleep(1.2); s2 = p.stat().st_size
    return s1 == s2

def derive_company(email:str)->str:
    if "@" not in email: return ""
    domain = email.split("@",1)[1]
    base = domain.split(".",1)[0].replace("-"," ").replace("_"," ")
    return base.capitalize()

def process_file(f:Path):
    if not done_writing(f):
        print("Waiting for file to finish writing…"); time.sleep(1.2)
    base = f.name                               # jane@acme.com__tour.mp4
    if "__" not in base:
        print(f"Skip {base}: expected 'email__something.ext'"); return

    email = base.split("__",1)[0]
    rest  = base.split("__",1)[1]
    s     = safe_id(email)                       # jane_acme_com
    vid_key = f"videos/{s}__{rest}"              # videos/jane_acme_com__tour.mp4

    # Upload video (flat key; copyto = file, not dir)
    run([RCLONE_BIN, "copyto", str(f), f"r2:{R2_BUCKET}/{vid_key}", "-vv"])

    # Pointer JSON -> write to system temp dir (NOT in watched folder)
    company = derive_company(email)
    pointer = {"key": vid_key, "company": company}
    tmp_path = Path(tempfile.gettempdir()) / f"{s}.pointer.json"
    tmp_path.write_text(json.dumps(pointer), encoding="utf-8")
    try:
        run([RCLONE_BIN, "copyto", str(tmp_path), f"r2:{R2_BUCKET}/pointers/{s}.json", "-vv"])
    finally:
        try: tmp_path.unlink()
        except Exception: pass

    print(f"Uploaded → r2:{R2_BUCKET}/{vid_key}")
    print(f"Pointer  → r2:{R2_BUCKET}/pointers/{s}.json")
    print(f"Landing  → {PUBLIC_BASE}/p/?id={s}")

class Handler(FileSystemEventHandler):
    # Only react to NEW files; ignore modifications (prevents loops)
    def on_created(self, e): self._maybe(Path(e.src_path))

    def _maybe(self, p:Path):
        # Ignore hidden/temp/json files
        if p.name.startswith("."): return
        if p.suffix.lower() in {".json", ".tmp", ".part"}: return
        if p.suffix.lower() not in VIDEO_EXTS: return
        if "__" not in p.name: return
        process_file(p)

def main():
    print(f"[watching] {DROP_DIR}")
    print(f"[rclone]   {RCLONE_BIN}")
    DROP_DIR.mkdir(parents=True, exist_ok=True)
    obs = Observer()
    obs.schedule(Handler(), str(DROP_DIR), recursive=False)
    obs.start()
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        obs.stop(); obs.join()

if __name__ == "__main__":
    try:
        import watchdog  # noqa
    except Exception:
        print("Install watchdog:  pip install watchdog")
        sys.exit(1)
    main()
