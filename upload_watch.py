#!/usr/bin/env python3
import os, sys, time, mimetypes, shutil, json, datetime
import pathlib, re, logging
from dotenv import load_dotenv

# ---- Load .env from this folder ----
HERE = pathlib.Path(__file__).resolve().parent
load_dotenv(HERE / ".env")

# ---- Config from .env ----
ROOT_WATCH             = os.getenv("ROOT_WATCH", "").strip()  # path to your "Drop video here" folder
R2_ACCOUNT_ID          = os.getenv("R2_ACCOUNT_ID", "").strip()
R2_ENDPOINT            = os.getenv("R2_ENDPOINT", "").strip()
R2_ACCESS_KEY_ID       = os.getenv("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY   = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET              = os.getenv("R2_BUCKET", "").strip()

VIDEO_EXTS = [e.strip().lower() for e in (os.getenv("VIDEO_EXTS") or ".mp4,.mov,.m4v,.avi,.mkv,.webm").split(",")]

# Filename must start with "<safe_email>__"
NAME_PREFIX_RE = re.compile(r"^([a-z0-9_]+)__(.+)$", re.I)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def make_s3():
    import boto3
    endpoint = R2_ENDPOINT or (f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else "")
    if not endpoint:
        raise SystemExit("No R2_ENDPOINT or R2_ACCOUNT_ID provided.")
    if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY or not R2_BUCKET:
        raise SystemExit("Missing R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET.")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )

def is_video_file(p: pathlib.Path) -> bool:
    return p.is_file() and p.suffix.lower() in VIDEO_EXTS

def wait_for_stable(p: pathlib.Path, checks=3, delay=1.0) -> bool:
    """Wait until file size stops changing."""
    last = -1
    try:
        for _ in range(checks):
            sz = p.stat().st_size
            if sz == last and sz > 0:
                return True
            last = sz
            time.sleep(delay)
        return (p.stat().st_size == last) and last > 0
    except FileNotFoundError:
        return False

def upload_to_r2(s3, src: pathlib.Path, key: str):
    ctype, _ = mimetypes.guess_type(str(src))
    if not ctype:
        ctype = "application/octet-stream"
    s3.upload_file(str(src), R2_BUCKET, key, ExtraArgs={"ContentType": ctype})

def write_pointer(s3, safe_id: str, video_key: str):
    """Write pointers/<safe_id>.json -> tells your API which video to stream."""
    payload = {
        "key": video_key,
        "fileName": pathlib.Path(video_key).name,
        "safeId": safe_id,
        "updated": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=f"pointers/{safe_id}.json",
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    logging.info("pointer updated: pointers/%s.json -> %s", safe_id, video_key)

def handle_file(p: pathlib.Path, s3=None):
    if not is_video_file(p):
        return

    m = NAME_PREFIX_RE.match(p.name)
    if not m:
        logging.warning("skip (filename missing '<safe_email>__' prefix): %s", p.name)
        return

    safe_id = m.group(1).lower()  # this is your safe_email
    if not wait_for_stable(p):
        logging.warning("file not stable (skipping for now): %s", p.name)
        return

    s3 = s3 or make_s3()
    video_key = f"videos/{p.name}"

    logging.info("uploading -> r2://%s/%s", R2_BUCKET, video_key)
    upload_to_r2(s3, p, video_key)
    logging.info("upload ok: %s", p.name)

    # Write/update pointer so /api/sample?id=<safe_id> becomes "ready"
    write_pointer(s3, safe_id, video_key)

    # Move to 'uploaded' folder
    uploaded_dir = p.parent / "uploaded"
    uploaded_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    dest = uploaded_dir / f"{ts}_{p.name}"
    try:
        shutil.move(str(p), str(dest))
        logging.info("moved -> %s", dest)
    except Exception as e:
        logging.warning("could not move to uploaded/: %s", e)

def scan_once():
    if not ROOT_WATCH:
        logging.error("Set ROOT_WATCH in .env")
        return
    watch = pathlib.Path(ROOT_WATCH)
    if not watch.exists():
        logging.error("ROOT_WATCH does not exist: %s", watch)
        return

    for entry in list(watch.iterdir()):
        if entry.is_file():
            try:
                handle_file(entry)
            except Exception as e:
                logging.exception("error handling %s: %s", entry.name, e)

def main():
    logging.info("watch=%s", ROOT_WATCH)
    scan_once()

if __name__ == "__main__":
    main()
