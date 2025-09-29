cat > /Users/matthieu/matly-tools/upload_watch.py << 'PY'
#!/usr/bin/env python3
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
    print(">", " ".join(cmd), flush=True)
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
        print("Waiting for file to finish writing…", flush=True); time.sleep(1.2)
    base = f.name
    if "__" not in base:
        print(f"Skip {base}: expected 'email__something.ext'", flush=True); return

    email = base.split("__",1)[0]
    rest  = base.split("__",1)[1]
    s     = safe_id(email)
    vid_key = f"videos/{s}__{rest}"

    run([RCLONE_BIN, "copyto", str(f), f"r2:{R2_BUCKET}/{vid_key}", "-vv"])

    company = derive_company(email)
    pointer = {"key": vid_key, "company": company}
    tmp_path = Path(tempfile.gettempdir()) / f"{s}.pointer.json"
    tmp_path.write_text(json.dumps(pointer), encoding="utf-8")
    try:
        run([RCLONE_BIN, "copyto", str(tmp_path), f"r2:{R2_BUCKET}/pointers/{s}.json", "-vv"])
    finally:
        try: tmp_path.unlink()
        except Exception: pass

    print(f"Uploaded → r2:{R2_BUCKET}/{vid_key}", flush=True)
    print(f"Pointer  → r2:{R2_BUCKET}/pointers/{s}.json", flush=True)
    print(f"Landing  → {PUBLIC_BASE}/p/?id={s}", flush=True)

class Handler(FileSystemEventHandler):
    # IMPORTANT: only handle brand-new files
    def on_created(self, e): self._maybe(Path(e.src_path))
    def _maybe(self, p:Path):
        name = p.name
        if name.startswith("."): return
        ext = p.suffix.lower()
        if ext in {".json", ".tmp", ".part"}: return
        if ext not in VIDEO_EXTS: return
        if "__" not in name: return
        process_file(p)

def main():
    print(f"[watching] {DROP_DIR}", flush=True)
    print(f"[rclone]   {RCLONE_BIN}", flush=True)
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
        print("Install watchdog:  pip install watchdog", flush=True)
        sys.exit(1)
    main()
PY

chmod +x /Users/matthieu/matly-tools/upload_watch.py
