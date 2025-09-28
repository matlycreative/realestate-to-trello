#!/usr/bin/env python3
"""
Poll a Trello list (Day 0). For each card found:
- Read Company / First / Email from the card description (header block).
- Choose template A (no "First") or B (has "First").
- Fill {company}, {first}, {from_name} placeholders.
- Send the email via SMTP (plain text + HTML alternative).
- Append signature (optional logo).
- Mark the card as "Sent" (comment) to avoid re-sending.
"""

import os, re, time, json, html, mimetypes
from datetime import datetime
import requests

# ----------------- Small helpers -----------------
def _get_env(*names, default=""):
    """Return the first non-empty env var among names (case-sensitive), else default."""
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

# ----------------- Config / Env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")  # prefer DAY0, fallback LIST_ID

# Email templates (from GitHub Secrets; keep them as plain text)
SUBJECT_A = _get_env("SUBJECT_A", default="Quick idea for {company}")
BODY_A    = _get_env("BODY_A",    default="Hi there,\n\nWe help {company}...\n\n– {from_name}")
SUBJECT_B = _get_env("SUBJECT_B", default="Quick idea for {company}")
BODY_B    = _get_env("BODY_B",    default="Hi {first},\n\nWe help {company}...\n\n– {from_name}")

# From identity
FROM_NAME  = _get_env("FROM_NAME", default="Outreach")
FROM_EMAIL = _get_env("FROM_EMAIL")

# SMTP (tolerant names + sensible defaults)
SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")

# optional defaults
LINK_URL=""                 # fallback URL if card has no Website:
LINK_TEXT="My Portfolio"    # what the link shows as
LINK_COLOR="#1a73e8"        # link color in the HTML email (blue-ish)

# Password accepted under several names
SMTP_PASS = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")

# Username: prefer explicit, fall back to FROM_EMAIL
SMTP_USER = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")

# HTML styling + signature logo
EMAIL_FONT_PX       = int(os.getenv("EMAIL_FONT_PX", "16"))
SIGNATURE_LOGO_URL  = os.getenv("SIGNATURE_LOGO_URL", "").strip()
SIGNATURE_INLINE    = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX  = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))

# Signature line controls
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

# Poll behavior / gating
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: Day0")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_day0.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))  # 0 = unlimited per run

# HTTP session
UA = f"TrelloEmailer/1.2 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# Header labels we expect in the card description
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# --------------- Helpers ----------------
def require_env():
    missing = []
    if not TRELLO_KEY:  missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN: missing.append("TRELLO_TOKEN")
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_DAY0")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS (or SMTP_PASSWORD / smtp_pass)")

    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    if not SMTP_USER:
        print("Warning: SMTP_USER/SMTP_USERNAME not set; will use FROM_EMAIL as SMTP login.")
    print("ENV check: SMTP_PASS:", "set" if bool(SMTP_PASS) else "missing",
          "| SMTP_USER:", SMTP_USER or "(empty)")

def _trello_call(method, url_path, **params):
    """GET/POST with simple retry on Trello throttling."""
    for attempt in range(3):
        try:
            params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
            url = f"https://api.trello.com/1/{url_path.lstrip('/')}"
            r = (SESS.get if method == "GET" else SESS.post)(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("Unreachable")

def trello_get(url_path, **params):
    return _trello_call("GET", url_path, **params)

def trello_post(url_path, **params):
    return _trello_call("POST", url_path, **params)

def load_sent_cache():
    try:
        with open(SENT_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_sent_cache(ids):
    d = os.path.dirname(SENT_CACHE_FILE)
    if d:
        os.makedirs(d, exist_ok=True)
    try:
        with open(SENT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f)
    except Exception:
        pass

def parse_header(desc: str) -> dict:
    out = {k: "" for k in TARGET_LABELS}
    d = (desc or "").replace("\r\n","\n").replace("\r","\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                val = (m.group(1) or "").strip()
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip(); i += 1
                out[lab] = val
                break
        i += 1
    return out

def clean_email(raw: str) -> str:
    if not raw:
        return ""
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

def fill_template(tpl: str, *, company: str, first: str, from_name: str, link: str = "") -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        if key == "link":      return link or ""      
