#!/usr/bin/env python3
import os, time, json, subprocess, sys, shutil, errno
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ---- config / env ----
DROP_DIR    = Path(os.getenv("DROP_DIR", str(Path.home() / "Drop Videos Here")))
R2_BUCKET   = os.getenv("R2_BUCKET", "samples")
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://matlycreative.pages.dev")

# absolute rclone for LaunchAgents
RCLONE_BIN = os.getenv("RCLONE_BIN") or shutil.which("rclone") or "/opt/homebrew/bin/rclone"
print(f"[watcher] using rclone at: {RCLONE_BIN}")

# where to put files after successful upload (optional but recommended)
UPLOADED_DIR = DROP_DIR / "Uploaded"

def safe_id(email:str)->str:
    return email.lower().replace("@","_").replace(".","_")

def run(cmd):
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)

def done_writing(p:Path, wait=1.2)->bool:
    if not p.exists() or not p.is_file(): 
        return False
    s1 = p.stat().st_size
    time.sleep(wait)
    if not p.exists() or not p.is_file():
        return False
    s2 = p.stat().st_size
    return s1 == s2 and s1 > 0

def derive_company(email:str)->str:
    if "@" not in email: return ""
    domain = email.split("@",1)[1]
    base = domain.split(".",1)[0].replace("-"," ").replace("_"," ")
    return base.capitalize()

def r2_exists(key:str) -> bool:
    try:
        res = subprocess.run(
            [RCLONE_BIN, "lsf", f"r2:{R2_BUCKET}/{key}"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        return res.returncode == 0 and res.stdout.strip() != b""
    except Exception:
        return False

def take_lock(lock_name:str):
    lock_path = f"/tmp/matly-watch.lock.{lock_name}"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.write(fd, str(os.getpid()).encode())
        return fd, lock_path
    except OSError as e:
        if e.errno == errno.EEXIST:
            return None, lock_path
        raise

def release_lock(fd, path):
    try: os.close(fd)
    except Exception: pass
    try: os.unlink(path)
    except Exception: pass

def process_file(f:Path):
    base = f.name
    if "__" not in base:
        print(f"Skip {base}: expected 'email__something.ext'")
        return

    email = base.split("__",1)[0]
    rest  = base.split("__",1)[1]
    s     = safe_id(email)

    lock_fd, lock_path = take_lock(s)
    if not lock_fd:
        print(f"Already processing {s} (lock {lock_path}), skipping.")
        return

    try:
        if not done_writing(f):
            print("Waiting for file to finish writing…")
            if not done_writing(f, wait=2.0):
                print("Still writing; will try again on next event.")
                return

        vid_key = f"videos/{s}__{rest}"   # flat path
        ptr_key = f"pointers/{s}.json"

        # Skip if pointer already exists in R2 (prevents repeats)
        if r2_exists(ptr_key):
            print(f"Pointer already exists in R2 ({ptr_key}); skipping re-upload.")
            try:
                UPLOADED_DIR.mkdir(parents=True, exist_ok=True)
                f.rename(UPLOADED_DIR / f.name)
            except Exception as e:
                print(f"Move to Uploaded/ failed (non-fatal): {e}")
            return

        # Upload video
        run([RCLONE_BIN, "copyto", str(f), f"r2:{R2_BUCKET}/{vid_key}", "-vv"])

        # Write pointer JSON to /tmp then upload
        company = derive_company(email)
        pointer = {"key": vid_key, "company": company}
        tmp = Path("/tmp") / f"{s}.pointer.json"
        tmp.write_text(json.dumps(pointer), encoding="utf-8")
        run([RCLONE_BIN, "copyto", str(tmp), f"r2:{R2_BUCKET}/{ptr_key}", "-vv"])
        try: tmp.unlink()
        except FileNotFoundError: pass

        print(f"Uploaded → r2:{R2_BUCKET}/{vid_key}")
        print(f"Pointer  → r2:{R2_BUCKET}/{ptr_key}")
        print(f"Landing  → {PUBLIC_BASE}/p/?id={s}")

        # Move original file to Uploaded/
        try:
            UPLOADED_DIR.mkdir(parents=True, exist_ok=True)
            f.rename(UPLOADED_DIR / f.name)
        except Exception as e:
            print(f"Move to Uploaded/ failed (non-fatal): {e}")

    finally:
        release_lock(lock_fd, lock_path)

# Only react on created/moved (NOT modified)
class Handler(FileSystemEventHandler):
    def on_created(self, e): self._maybe(Path(e.src_path))
    def on_moved(self, e):   self._maybe(Path(e.dest_path))
    def _maybe(self, p:Path):
        if p.is_file() and not p.name.startswith(".") and "__" in p.name:
            if p.suffix.lower() in {".tmp", ".part"}:  # ignore temp
                return
            process_file(p)

def main():
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
        import watchdog  # noqa
    except Exception:
        print("Install watchdog:  pip install watchdog")
        sys.exit(1)
    main()
