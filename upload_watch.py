#!/usr/bin/env python3
"""
Drag files into DROP_DIR and they are:
  - uploaded to R2 as videos/<safe>__<rest>
  - pointer JSON written to pointers/<safe>.json

Requires: pip install watchdog
          rclone configured with a remote named 'r2'
Env:
  R2_BUCKET       (e.g. 'samples')
  PUBLIC_BASE     (e.g. 'https://matlycreative.pages.dev') only for notices
"""
#!/usr/bin/env python3
import os, time, json, subprocess, sys, shutil
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- rclone path resolver (for LaunchAgent envs with minimal PATH) ---
RCLONE_BIN = os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/opt/homebrew/bin/rclone"

DROP_DIR   = Path(os.getenv("DROP_DIR", str(Path.home() / "Drop Videos Here")))
R2_BUCKET  = os.getenv("R2_BUCKET", "samples")
PUBLIC_BASE= os.getenv("PUBLIC_BASE", "https://matlycreative.pages.dev")

def safe_id(email:str)->str: return email.lower().replace("@","_").replace(".","_")

def run(cmd):
    # if the command is 'rclone', replace with absolute path
    if cmd and cmd[0] == "rclone":
        cmd = [RCLONE_BIN] + cmd[1:]
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)

def done_writing(p:Path)->bool:
    if not p.exists() or not p.is_file(): return False
    s1 = p.stat().st_size; time.sleep(1.2); s2 = p.stat().st_size
    return s1 == s2

def derive_company(email:str)->str:
    # best-effort: domain to Title
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
    run(["rclone","copyto",str(f),f"r2:{R2_BUCKET}/{vid_key}","-vv"])

    # Pointer JSON
    company = derive_company(email)
    pointer = {"key": vid_key, "company": company}
    tmp = f.with_suffix(".pointer.json")
    tmp.write_text(json.dumps(pointer), encoding="utf-8")
    run(["rclone","copyto",str(tmp),f"r2:{R2_BUCKET}/pointers/{s}.json","-vv"])
    tmp.unlink(missing_ok=True)

    print(f"Uploaded → r2:{R2_BUCKET}/{vid_key}")
    print(f"Pointer  → r2:{R2_BUCKET}/pointers/{s}.json")
    print(f"Landing  → {PUBLIC_BASE}/p/?id={s}")

class Handler(FileSystemEventHandler):
    def on_created(self, e): self._maybe(Path(e.src_path))
    def on_modified(self, e): self._maybe(Path(e.src_path))
    def _maybe(self, p:Path):
        if p.is_file() and not p.name.startswith(".") and "__" in p.name and not p.suffix.lower() in {".tmp",".part"}:
            process_file(p)

def main():
    from watchdog.observers import Observer
    DROP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[watching] {DROP_DIR}")
    obs = Observer()
    obs.schedule(Handler(), str(DROP_DIR), recursive=False)
    obs.start()
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        obs.stop(); obs.join()

if __name__ == "__main__":
    try:
        import watchdog  # just to hint
    except Exception:
        print("Install watchdog:  pip install watchdog")
        sys.exit(1)
    main()
