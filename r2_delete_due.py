#!/usr/bin/env python3
"""
Delete due Cloudflare R2 samples based on marker objects.

Looks for objects under:  {R2_MARKER_PREFIX}/<anything>.json  (default prefix: delete_markers)
Each marker JSON must contain:
  {
    "id":  "<safe_id>",         # e.g. "john_doe_gmail_com"
    "due": "2025-10-31T00:00:00Z"   # or "2025-10-31" or "20251031"
  }

If due <= now (UTC), deletes:
  - {KEY_PREFIX_POINTERS}/{id}.json      (default: pointers/<id>.json)
  - all {KEY_PREFIX_VIDEOS}/{id}__*      (default: videos/<id>__*)

Env (any missing -> exit):
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, (R2_BUCKET or R2_BUCKET_NAME)

Optional env:
  R2_MARKER_PREFIX      (default: delete_markers)
  KEY_PREFIX_POINTERS   (default: pointers)
  KEY_PREFIX_VIDEOS     (default: videos)
  DRY_RUN               (1/true/on to log only)
"""

import os, json, re, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

def _get(name, default=""): return (os.getenv(name) or default).strip()

ACCOUNT = _get("R2_ACCOUNT_ID") or _get("CF_R2_ACCOUNT_ID")
ACCESS  = _get("R2_ACCESS_KEY_ID") or _get("CF_R2_ACCESS_KEY_ID")
SECRET  = _get("R2_SECRET_ACCESS_KEY") or _get("CF_R2_SECRET_ACCESS_KEY")
BUCKET  = _get("R2_BUCKET") or _get("R2_BUCKET_NAME")
MARKERS = _get("R2_MARKER_PREFIX", "delete_markers")
PFX_PTR = _get("KEY_PREFIX_POINTERS", "pointers")
PFX_VID = _get("KEY_PREFIX_VIDEOS",   "videos")
DRY     = _get("DRY_RUN", "0").lower() in ("1","true","yes","on")

missing = [k for k,v in {
    "R2_ACCOUNT_ID": ACCOUNT, "R2_ACCESS_KEY_ID": ACCESS,
    "R2_SECRET_ACCESS_KEY": SECRET, "R2_BUCKET/R2_BUCKET_NAME": BUCKET,
}.items() if not v]
if missing:
    sys.exit("Missing env: " + ", ".join(missing))

endpoint = f"https://{ACCOUNT}.r2.cloudflarestorage.com"
s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS,
    aws_secret_access_key=SECRET,
    region_name="auto",
    endpoint_url=endpoint,
    config=Config(signature_version="s3v4"),
)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def list_objects(prefix: str):
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for it in resp.get("Contents", []) or []:
            yield it["Key"]
        token = resp.get("NextContinuationToken")
        if not token:
            break

def read_json(key: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[skip] cannot read {key}: {e}")
        return None

_ISO_Z = re.compile(r"Z$", re.I)

def parse_due(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("empty due")
    # YYYYMMDD
    if re.fullmatch(r"\d{8}", s):
        y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
        return datetime(y, m, d, tzinfo=timezone.utc)
    # ISO with Z or offset
    try:
        if _ISO_Z.search(s):
            s = _ISO_Z.sub("+00:00", s)
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    # YYYY-MM-DD
    try:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    raise ValueError(f"unrecognized due format: {s!r}")

def delete_key(key: str):
    if DRY:
        print(f"[dry] delete {key}")
        return
    try:
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"deleted {key}")
    except Exception as e:
        print(f"[warn] delete failed {key}: {e}")

def main():
    due_count = 0
    marker_prefix = f"{MARKERS}/"
    print(f"[scan] markers under s3://{BUCKET}/{marker_prefix}  (ptr={PFX_PTR}, vid={PFX_VID}, dry={DRY})")

    for key in list_objects(marker_prefix):
        data = read_json(key)
        if not data:
            continue
        safe_id = (data.get("id") or "").strip()
        due_raw = (data.get("due") or "").strip()
        if not (safe_id and due_raw):
            print(f"[skip] bad marker {key} (need 'id' and 'due')")
            continue

        try:
            due_dt = parse_due(due_raw)
        except Exception as e:
            print(f"[skip] bad due in {key}: {e}")
            continue

        if due_dt > now_utc():
            # not due yet
            continue

        print(f"[due ] {safe_id}  marker={key}  due={due_dt.isoformat()}")

        # delete pointer
        ptr_key = f"{PFX_PTR}/{safe_id}.json"
        delete_key(ptr_key)

        # delete all videos for that id
        for vkey in list_objects(f"{PFX_VID}/{safe_id}__"):
            delete_key(vkey)

        # delete the marker last
        delete_key(key)
        due_count += 1
        time.sleep(0.15)

    print(f"Done. Deleted {due_count} id(s).")

if __name__ == "__main__":
    main()
