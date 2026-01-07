#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Trello Lead Scrubber — bulk filter obviously bad emails (NO MX / invalid syntax / missing)

What it does:
- Reads cards from SOURCE list
- Extracts Email from card desc (and fallback scan)
- Validates basic email syntax
- Checks domain MX (cached)
- If bad -> comments on card + moves to BAD list
- If good -> leaves it untouched

Env required:
  TRELLO_KEY
  TRELLO_TOKEN
  TRELLO_LIST_ID_SOURCE
  TRELLO_LIST_ID_BAD
"""

import os, re, json, time, html, subprocess
from datetime import datetime
import requests

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

def log(*a): print(*a, flush=True)

def _get_env(name, default=""):
    v = os.getenv(name, default)
    return (v or "").strip()

TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
SOURCE_LIST  = _get_env("TRELLO_LIST_ID_SOURCE")
BAD_LIST     = _get_env("TRELLO_LIST_ID_BAD")

MX_CACHE_FILE = _get_env("MX_CACHE_FILE", ".data/mx_cache.json")
MAX_CHECKS_PER_RUN = int(_get_env("MAX_CHECKS_PER_RUN", "0"))

SESS = requests.Session()
SESS.headers.update({"User-Agent": "TrelloEmailScrubber/1.0"})

def trello_req(method: str, path: str, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    url = f"https://api.trello.com/1/{path.lstrip('/')}"
    r = SESS.request(method, url, params=params, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else None

def trello_get(path, **params): return trello_req("GET", path, **params)
def trello_post(path, **params): return trello_req("POST", path, **params)
def trello_put(path, **params): return trello_req("PUT", path, **params)

def load_cache():
    try:
        with open(MX_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(cache: dict):
    d = os.path.dirname(MX_CACHE_FILE)
    if d:
        os.makedirs(d, exist_ok=True)
    try:
        with open(MX_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, sort_keys=True)
    except Exception:
        pass

def extract_email(text: str) -> str:
    txt = html.unescape(text or "")
    m = EMAIL_RE.search(txt)
    return m.group(0).strip().lower() if m else ""

def domain_of(email: str) -> str:
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()

def has_mx_via_nslookup(domain: str) -> bool:
    """
    Portable MX check using nslookup (works on macOS/Linux).
    We consider MX present if output contains 'mail exchanger' or 'MX preference' or similar.
    """
    domain = (domain or "").strip().lower()
    if not domain:
        return False
    try:
        p = subprocess.run(
            ["nslookup", "-type=mx", domain],
            capture_output=True,
            text=True,
            timeout=8
        )
        out = (p.stdout or "") + "\n" + (p.stderr or "")
        out_l = out.lower()
        # Heuristics that match typical nslookup output
        if "mail exchanger" in out_l or "mx preference" in out_l:
            # Also ensure we didn't just get "non-existent domain"
            if "non-existent domain" in out_l or "nxdomain" in out_l:
                return False
            return True
        # Some systems show "can't find" for no MX
        if "can't find" in out_l or "no answer" in out_l:
            return False
        return False
    except Exception:
        return False

def mx_ok(domain: str, cache: dict) -> bool:
    """
    Cached MX check.
    cache[domain] = {"ok": bool, "ts": iso}
    """
    domain = (domain or "").strip().lower()
    if not domain:
        return False

    if domain in cache:
        return bool(cache[domain].get("ok"))

    ok = has_mx_via_nslookup(domain)
    cache[domain] = {"ok": bool(ok), "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z"}
    return ok

def comment(card_id: str, text: str):
    try:
        trello_post(f"cards/{card_id}/actions/comments", text=text)
    except Exception as e:
        log(f"[WARN] comment failed {card_id}: {e}")

def move_card(card_id: str, list_id: str):
    # Move by updating idList
    trello_put(f"cards/{card_id}", idList=list_id)

def main():
    missing = []
    for k, v in [("TRELLO_KEY", TRELLO_KEY), ("TRELLO_TOKEN", TRELLO_TOKEN), ("TRELLO_LIST_ID_SOURCE", SOURCE_LIST), ("TRELLO_LIST_ID_BAD", BAD_LIST)]:
        if not v:
            missing.append(k)
    if missing:
        raise SystemExit("Missing env: " + ", ".join(missing))

    cache = load_cache()

    cards = trello_get(f"lists/{SOURCE_LIST}/cards", fields="id,name,desc", limit=1000)
    if not isinstance(cards, list):
        log("No cards returned.")
        return

    checked = 0
    moved = 0

    for c in cards:
        if MAX_CHECKS_PER_RUN and checked >= MAX_CHECKS_PER_RUN:
            break

        card_id = c.get("id")
        title = c.get("name", "(no title)")
        desc = c.get("desc") or ""

        checked += 1

        email = extract_email(desc)
        if not email:
            comment(card_id, f"Scrubber: No valid email found — moved to BAD list.")
            move_card(card_id, BAD_LIST)
            moved += 1
            log(f"[BAD] {title}: no email")
            continue

        dom = domain_of(email)
        if not dom or "." not in dom:
            comment(card_id, f"Scrubber: Invalid email domain '{dom}' ({email}) — moved to BAD list.")
            move_card(card_id, BAD_LIST)
            moved += 1
            log(f"[BAD] {title}: invalid domain {dom}")
            continue

        ok = mx_ok(dom, cache)
        if not ok:
            comment(card_id, f"Scrubber: Domain has no MX (won't receive email): {dom} — moved to BAD list.")
            move_card(card_id, BAD_LIST)
            moved += 1
            log(f"[BAD] {title}: no MX {dom}")
            continue

        # good
        log(f"[OK] {title}: {email}")

        # be gentle with Trello API
        if checked % 25 == 0:
            save_cache(cache)
            time.sleep(0.6)

    save_cache(cache)
    log(f"Done. Checked={checked} | Moved_to_BAD={moved} | MX_cache={MX_CACHE_FILE}")

if __name__ == "__main__":
    main()
