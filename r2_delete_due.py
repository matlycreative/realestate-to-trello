#!/usr/bin/env python3
"""
Delete due Cloudflare R2 samples based on marker objects.

Looks for markers under:  {R2_MARKER_PREFIX}/<safe_id>__YYYYMMDD.json
Each marker JSON: {"id": "<safe_id>", "due": "2025-10-01T12:00:00Z"}

If due <= now, deletes:
  - pointers/<safe_id>.json
  - all videos/<safe_id>__*

Env:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, (R2_BUCKET_NAME or R2_BUCKET)
  R2_MARKER_PREFIX (default: delete_markers)
  DRY_RUN (optional: "1" to log without deleting)
"""

import os, json, re, sys, time
from datetime import datetime, timezone

def _get(name, default=""): return (os.getenv(name) or default).strip()

ACCOUNT = _get("R2_ACCOUNT_ID") or _get("CF_R2_ACCOUNT_ID")
ACCESS  = _get("R2_ACCESS_KEY_ID") or _get("CF_R2_ACCESS_KEY_ID")
SECRET  = _get("R2_SECRET_ACCESS_KEY") or _get("CF_R2_SECRET_ACCESS_KEY")
BUCKET  = _get("R2_BUCKET_NAME") or _get("R2_BUCKET")
PREFIX  = _get("R2_MARKER_PREFIX", "delete_markers")
DRY     = _get("DRY_RUN", "0").lower() in ("1","true","yes","on")

if not (ACCOUNT and ACCESS and SECRET and BUCKET):
    sys.exit("Missing R2 env (ACCOUNT/ACCESS/SECRET/BUCKET)")

import boto3
endpoint = f"https://{ACCOUNT}.r2.cloudflarestorage.com"
s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS,
    aws_secret_access_key=SECRET,
    region_name="auto",
    endpoint_url=endpoint,
)

def now_utc():
    return datetime.now(timezone.utc)

def list_objects(prefix):
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if token: kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for it in resp.get("Contents", []) or []:
            yield it["Key"]
        token = resp.get("NextContinuationToken")
        if not token:
            break

def delete_key(key):
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
    for key in list_objects(f"{PREFIX}/"):
        # read marker
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            safe_id = (data.get("id") or "").strip()
            due_iso = (data.get("due") or "").strip()
            if not (safe_id and due_iso):
                print(f"[skip] bad marker {key}")
                continue
            # parse ISO; accept trailing 'Z'
            if due_iso.endswith("Z"):
                due_iso = due_iso[:-1]
            due_dt = datetime.fromisoformat(due_iso).replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"[skip] unreadable marker {key}: {e}")
            continue

        if due_dt > now_utc():
            continue

        # delete pointer + all videos
        ptr = f"pointers/{safe_id}.json"
        delete_key(ptr)
        for vkey in list_objects(f"videos/{safe_id}__"):
            delete_key(vkey)

        # delete marker last
        delete_key(key)
        due_count += 1
        time.sleep(0.1)

    print(f"Done. Deleted {due_count} id(s).")

if __name__ == "__main__":
    main()
