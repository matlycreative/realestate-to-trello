#!/usr/bin/env python3
"""
FU2 — Poll a Trello list and send one email per card.

- Reads cards from the list ID in TRELLO_LIST_ID_FU2.
- Parses Company / First / Email from the card description.
- Chooses template A (no First) or B (has First).
- If /api/sample?id=<safe_id> returns a stream URL, we use the API 'link'.
  Otherwise we fall back to PORTFOLIO_URL or PUBLIC_BASE.
- You can override API linking with USE_API_LINK=0 to always use your PUBLIC_BASE.
- Marks the card with "Sent: FU2" and caches it locally so it won’t resend.
"""

import os, re, time, json, html, mimetypes
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

# ----------------- Config / Env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_FU2", "TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME", default="Outreach")
FROM_EMAIL = _get_env("FROM_EMAIL")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")

# Diagnostics (optional)
SMTP_DEBUG = _env_bool("SMTP_DEBUG", "0")
BCC_TO     = _get_env("BCC_TO", default="").strip()

# ----------------- Templates -----------------
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
log(f"[tpl] Using {'ENV' if USE_ENV_TEMPLATES else 'HARDCODED'} templates")

if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env("SUBJECT_A", default="Quick follow-up (2) on listing videos for {company}")
    SUBJECT_B = _get_env("SUBJECT_B", default="Quick follow-up (2) for {first} — listing videos at {company}")

    BODY_A = _get_env("BODY_A", default=
"""Hi there,

{extra} {link}

{extra}

Best,
Matthieu from Matly""")

    BODY_B = _get_env("BODY_B", default=
"""hi {first}

{extra} {link}

{extra}

Best,
Matthieu from Matly""")
else:
    SUBJECT_A = "Quick follow-up (2) on listing videos for {company}"
    SUBJECT_B = "Quick follow-up (2) for {first} — listing videos at {company}"

    BODY_A = """Hi there,

{extra} {link}

{extra}

Best,
Matthieu from Matly"""

    BODY_B = """hi {first}

{extra} {link}

{extra}

Best,
Matthieu from Matly"""

# Appearance / signature (kept consistent with FU1/DAY0)
EMAIL_FONT_PX         = int(os.getenv("EMAIL_FONT_PX", "16"))
SIGNATURE_LOGO_URL    = os.getenv("SIGNATURE_LOGO_URL", "").strip()
SIGNATURE_INLINE      = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX    = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

INCLUDE_PLAIN_URL = _env_bool("INCLUDE_PLAIN_URL", "0")

SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: FU2")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_fu2.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

PUBLIC_BASE   = _get_env("PUBLIC_BASE")       # e.g., https://matlycreative.com
LINK_TEXT     = _get_env("LINK_TEXT", default="My portfolio")
LINK_COLOR    = _get_env("LINK_COLOR", default="")
PORTFOLIO_URL = _get_env("PORTFOLIO_URL", default="")
USE_API_LINK  = _env_bool("USE_API_LINK", "1")    # 0 -> force PUBLIC_BASE link

def _norm_base(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")

PUBLIC_BASE   = _norm_base(PUBLIC_BASE)
PORTFOLIO_URL = _norm_base(PORTFOLIO_URL) or PUBLIC_BASE
log(f"[env] PUBLIC_BASE={PUBLIC_BASE}  PORTFOLIO_URL={PORTFOLIO_URL}  USE_API_LINK={USE_API_LINK}")

# HTTP session
UA = f"TrelloEmailer-FU2/2.0 (+{FROM_EMAIL or 'no-email'})"
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
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_FU2 or TRELLO_LIST_ID_DAY0")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS (or SMTP_PASSWORD / smtp_pass)")
    if not PUBLIC_BASE:  missing.append("PUBLIC_BASE (e.g., https://matlycreative.com)")
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")
    if not SMTP_USER:
        log("Warning: SMTP_USER/SMTP_USERNAME not set; will use FROM_EMAIL as SMTP login.")
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
            if attempt == 2: raise
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

# --------------- Parsing / email formatting ----------------
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

def fill_template(tpl: str, *, company: str, first: str, from_name: str, link: str = "", extra: str = "") -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        if key == "link":      return link or ""
        if key == "extra":     return extra or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name|link|extra)\s*}", repl, tpl, flags=re.I)

def fill_template_skip_extra(tpl: str, *, company: str, first: str, from_name: str, link: str) -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        if key == "link":      return link or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name|link)\s*}", repl, tpl, flags=re.I)

# --- Two-{extra} logic ---
EXTRA_TOKEN = re.compile(r"\{\s*extra\s*\}", flags=re.I)

def fill_with_two_extras(
    tpl: str, *, company: str, first: str, from_name: str,
    link: str, is_ready: bool, extra_ready: str, extra_wait: str
) -> str:
    base = fill_template_skip_extra(
        tpl, company=company, first=first, from_name=from_name, link=link
    )
    if is_ready:
        step1 = EXTRA_TOKEN.sub(extra_ready, base, count=1)  # first -> ready text
        step2 = EXTRA_TOKEN.sub("",         step1, count=1)  # second -> removed
    else:
        step1 = EXTRA_TOKEN.sub("",         base, count=1)   # first -> removed
        step2 = EXTRA_TOKEN.sub(extra_wait, step1, count=1)  # second -> wait text
    final = EXTRA_TOKEN.sub("", step2)
    final = re.sub(r"\s*:\s+(?=(https?://|www\.|<))", " ", final)
    final = re.sub(r"\n{3,}", "\n\n", final).strip()
    return final

def sanitize_subject(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]

def text_to_html(text: str) -> str:
    esc = html.escape(text or "").replace("\r\n","\n").replace("\r","\n")
    esc = esc.replace("\n\n", "</p><p>").replace("\n", "<br>")
    p_style = "margin:0 0 12px 0;color:#111111 !important;"
    wrap_style = (
        f"font-family:Arial,Helvetica,sans-serif;font-size:{EMAIL_FONT_PX}px;line-height:1.6;"
        "color:#111111 !important;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;"
    )
    esc = f'<p style="{p_style}">{esc}</p>'
    esc = esc.replace("<p>", f'<p style="{p_style}">')
    return f'<div style="{wrap_style}">{esc}</div>'

_URL_RE = re.compile(r"https?://[^\s<>\")']+")
def _autolink_html(escaped_html: str) -> str:
    def _wrap(m):
        url = m.group(0)
        escu = html.escape(url, quote=True)
        return f'<a href="{escu}">{escu}</a>'
    return _URL_RE.sub(_wrap, escaped_html)

def signature_html(logo_cid: str | None) -> str:
    parts = []
    if SIGNATURE_ADD_NAME:
        line = SIGNATURE_CUSTOM_TEXT if SIGNATURE_CUSTOM_TEXT else f"– {FROM_NAME}"
        parts.append(f'<p style="margin:16px 0 0 0;">{html.escape(line)}</p>')
    if SIGNATURE_LOGO_URL:
        img_src = f"cid:{logo_cid}" if (SIGNATURE_INLINE and logo_cid) else html.escape(SIGNATURE_LOGO_URL)
        parts.append(
            f'<div style="margin-top:8px;"><img src="{img_src}" alt="" '
            f'style="max-width:{SIGNATURE_MAX_W_PX}px;height:auto;border:0;display:block;"></div>'
        )
    return "".join(parts)

# ----------------- Email sender -----------------
def send_email(to_email: str, subject: str, body_text: str, *, link_url: str = "", link_text: str = "", link_color: str = ""):
    from email.message import EmailMessage
    import smtplib

    # Normalize link + label
    if link_url and not re.match(r"^https?://", link_url, flags=re.I):
        link_url = "https://" + link_url
    label = (link_text or "My portfolio").strip() or "My portfolio"

    full = link_url
    bare = re.sub(r"^https?://", "", full, flags=re.I) if full else ""
    esc_full = html.escape(full, quote=True) if full else ""
    esc_bare = html.escape(bare, quote=True) if bare else ""

    # Plain text body: replace URL with label unless INCLUDE_PLAIN_URL=1
    body_pt = body_text
    if full:
        if not INCLUDE_PLAIN_URL:
            for pat in (full, bare):
                if pat:
                    body_pt = body_pt.replace(pat, label)
        else:
            if full not in body_pt and bare not in body_pt:
                body_pt = (body_pt.rstrip() + "\n\n" + full).strip()

    # HTML body: mark link, autolink others, insert styled anchor
    MARK = "__LINK_MARKER__"
    body_marked = body_text
    for pat in (full, bare):
        if pat:
            body_marked = body_marked.replace(pat, MARK)

    html_core = text_to_html(body_marked)
    html_core = _autolink_html(html_core)
    for pat in (esc_full, esc_bare):
        if pat:
            html_core = html_core.replace(pat, MARK)

    if full:
        style_attr = f' style="color:{html.escape(link_color)};text-decoration:underline;"' if link_color else ""
        anchor = f'<a{style_attr} href="{html.escape(full, quote=True)}">{html.escape(label)}</a>'
        if MARK in html_core:
            html_core = html_core.replace(MARK, anchor)
        else:
            html_core += f"<p>{anchor}</p>"

    # finalize HTML + optional signature
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    if BCC_TO:
        msg["Bcc"] = BCC_TO
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)
    msg.add_alternative(html_full, subtype="html
