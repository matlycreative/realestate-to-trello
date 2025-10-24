#!/usr/bin/env python3
"""
FU1 — Poll a Trello list and send one email per card (company-based personal link).

- Reads cards from the list ID in TRELLO_LIST_ID_FU1.
- Parses Company / First / Email from the card description.
- Builds the personalized ID from Company slug (fallback to email-safe id).
- ALWAYS links to: <PUBLIC_BASE>/p/?id=<id>
  (We only ping /api/sample?id=<id> to detect readiness and tweak copy.)
- Chooses template A (no First) or B (has First).
- Sends via SMTP (plain + HTML, signature, optional inline logo).
- Marks the card with "Sent: FU1" and caches it locally so it won’t resend.

Baked-in defaults (overridable via .env):
  FROM_NAME=Matthieu from Matly
  FROM_EMAIL=matthieu@matlycreative.com
  CONTACT_EMAIL=matthieu@matlycreative.com
  INCLUDE_CONTACT_LINK=1
  CONTACT_LINK_TEXT=Email me
  LINK_TEXT=See examples
  LINK_COLOR=#1a73e8
"""

import os, re, time, json, html, unicodedata, mimetypes
from datetime import datetime
from typing import Tuple
import requests

def log(*a): print(*a, flush=True)

# ----------------- Small helpers -----------------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

def _env_bool(name: str, default: str = "0") -> bool:
    val = os.getenv(name, default)
    return (val or "").strip().lower() in ("1", "true", "yes", "on")

def _safe_id_from_email(email: str) -> str:
    return (email or "").strip().lower().replace("@", "_").replace(".", "_")

def _slugify_company(name: str) -> str:
    s = (name or "").strip()
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]+", "", s)
    s = re.sub(r"[\s-]+", "_", s).strip("_")
    return s or ""

def choose_id(company: str, email: str) -> str:
    sid = _slugify_company(company)
    return sid if sid else _safe_id_from_email(email)

# ----------------- Config / Env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_FU1", "TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME",  default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")

SMTP_DEBUG = _env_bool("SMTP_DEBUG", "0")
BCC_TO     = _get_env("BCC_TO", default="").strip()

# ----------------- Templates -----------------
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
log(f"[tpl] Using {'ENV' if USE_ENV_TEMPLATES else 'HARDCODED'} templates")

if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env("SUBJECT_A", default="Quick follow-up on listing videos for {company}")
    SUBJECT_B = _get_env("SUBJECT_B", default="Quick follow-up for {first} — listing videos at {company}")

    BODY_A = _get_env("BODY_A", default=
"""Hi there,

Just following up in case you didn’t get a chance to look yet {extra}: {link}

{extra}

Best,
Matthieu from Matly""")

    BODY_B = _get_env("BODY_B", default=
"""hi {first}

Just following up on the portfolio I shared {extra}: {link}

{extra}

Best,
Matthieu from Matly""")
else:
    SUBJECT_A = "Quick follow-up on listing videos for {company}"
    SUBJECT_B = "Quick follow-up for {first} — listing videos at {company}"
    BODY_A = """Hi there,

Just following up in case you didn’t get a chance to look yet {extra}: {link}

{extra}

Best,
Matthieu from Matly"""
    BODY_B = """hi {first}

Just following up on the portfolio I shared {extra}: {link}

{extra}

Best,
Matthieu from Matly"""

# Appearance / signature
EMAIL_FONT_PX         = int(os.getenv("EMAIL_FONT_PX", "16"))
SIGNATURE_LOGO_URL    = os.getenv("SIGNATURE_LOGO_URL", "").strip()
SIGNATURE_INLINE      = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX    = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

# Link styles + contact defaults (same as Day-0)
INCLUDE_PLAIN_URL    = _env_bool("INCLUDE_PLAIN_URL", "0")
LINK_TEXT            = _get_env("LINK_TEXT",  default="See examples")
LINK_COLOR           = _get_env("LINK_COLOR", default="#1a73e8")

CONTACT_EMAIL        = _get_env("CONTACT_EMAIL", "FROM_EMAIL", default="matthieu@matlycreative.com")
INCLUDE_CONTACT_LINK = _env_bool("INCLUDE_CONTACT_LINK", "1")
CONTACT_LINK_TEXT    = _get_env("CONTACT_LINK_TEXT", default="Email me")
CONTACT_LINK_COLOR   = _get_env("CONTACT_LINK_COLOR", default=LINK_COLOR)

SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: FU1")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_fu1.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

PUBLIC_BASE   = _get_env("PUBLIC_BASE")       # e.g., https://matlycreative.com
PORTFOLIO_URL = _get_env("PORTFOLIO_URL", default="")  # fallback not used for final link

def _norm_base(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")

PUBLIC_BASE   = _norm_base(PUBLIC_BASE)
PORTFOLIO_URL = _norm_base(PORTFOLIO_URL) or PUBLIC_BASE
log(f"[env] PUBLIC_BASE={PUBLIC_BASE}")

# HTTP session
UA = f"TrelloEmailer-FU1/3.0 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# Header parsing
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*[:\-]\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# --------------- Sanity checks ----------------
def require_env():
    missing = []
    if not TRELLO_KEY:   missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN: missing.append("TRELLO_TOKEN")
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_FU1")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS")
    if not PUBLIC_BASE:  missing.append("PUBLIC_BASE (https://matlycreative.com)")
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")
    if not SMTP_USER:
        log("Warning: SMTP_USER not set; will use FROM_EMAIL as SMTP login.")
    log("ENV check: SMTP_PASS:", "set" if bool(SMTP_PASS) else "missing",
        "| SMTP_USER:", SMTP_USER or "(empty)")

# --------------- Trello helpers ----------------
def _trello_call(method, url_path, **params):
    for attempt in range(3):
        try:
            params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
            url = f"https://api.trello.com/1/{url_path.lstrip('/')}"
            r = (SESS.get if method == "GET" else SESS.post)(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("Unreachable")

def trello_get(url_path, **params):  return _trello_call("GET", url_path, **params)
def trello_post(url_path, **params): return _trello_call("POST", url_path, **params)

def already_marked(card_id: str, marker: str) -> bool:
    try:
        acts = trello_get(f"cards/{card_id}/actions", filter="commentCard", limit=50)
    except Exception:
        return False
    marker_l = (marker or "").lower().strip()
    for a in acts:
        txt = (a.get("data", {}).get("text") or a.get("text") or "").strip()
        if txt.lower().startswith(marker_l):
            return True
    return False

def mark_sent(card_id: str, marker: str, extra: str = ""):
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    text = f"{marker} — {ts}"
    if extra:
        text += f"\n{extra}"
    try:
        trello_post(f"cards/{card_id}/actions/comments", text=text)
    except Exception:
        pass

# --------------- Cache ----------------
def load_sent_cache():
    try:
        with open(SENT_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_sent_cache(ids):
    d = os.path.dirname(SENT_CACHE_FILE)
    if d: os.makedirs(d, exist_ok=True)
    try:
        with open(SENT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f)
    except Exception:
        pass

# --------------- Parsing ----------------
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
    if not raw: return ""
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

# --------------- Link & readiness ----------------
def _sample_info(personal_id: str) -> Tuple[bool, str]:
    """
    Ping /
