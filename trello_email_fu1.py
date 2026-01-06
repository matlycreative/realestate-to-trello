#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FU1 — Minimal sender (Day-0 base) with URLs + markers.

PLAIN TEXT ONLY
- No HTML, no visuals
- Reads cards from TRELLO_LIST_ID_FU1
- Sends simple email with: Company, First (if present), URLs
- Always includes URLs (so they stay clickable in plain text):
    - Personal page: <PUBLIC_BASE>/p/?id=<id>
    - Portfolio    : <PORTFOLIO_URL> (defaults to <PUBLIC_BASE>/portfolio)
    - Upload       : <UPLOAD_URL>
- Keeps cache + Trello marker to avoid double sends

Optional debug overrides:
- IGNORE_SENT=1  -> ignore cache + marker and resend (use carefully)
- MAX_SEND_PER_RUN=N -> limit sends per run
"""

import os, re, time, json, html, unicodedata
from datetime import datetime
import requests

def log(*a): print(*a, flush=True)

# ----------------- tiny utils -----------------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

def _env_bool(name: str, default: str = "0") -> bool:
    return (_get_env(name, default=default) or "").strip().lower() in ("1","true","yes","on")

def sanitize_subject(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]

def clean_one_line(s: str) -> str:
    """Critical: prevent hidden newlines breaking EmailMessage headers."""
    if s is None:
        return ""
    s = html.unescape(str(s))
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _safe_id_from_email(email: str) -> str:
    return (email or "").strip().lower().replace("@", "_").replace(".", "_")

def _slugify_company(name: str) -> str:
    s = (name or "").strip()
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]+", "", s)
    s = re.sub(r"[\s-]+", "_", s).strip("_")
    return s or ""

def choose_id(company: str, email: str) -> str:
    sid = _slugify_company(company)
    return sid if sid else _safe_id_from_email(email)

def _norm_base(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")

# ----------------- env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_FU1", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME",  default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = (_get_env("SMTP_USE_TLS", "smtp_use_tls", default="1") or "").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")
SMTP_DEBUG   = _env_bool("SMTP_DEBUG", "0")
BCC_TO       = _get_env("BCC_TO", default="").strip()

PUBLIC_BASE   = _norm_base(_get_env("PUBLIC_BASE"))  # required
PORTFOLIO_URL = _norm_base(_get_env("PORTFOLIO_URL")) or (PUBLIC_BASE + "/portfolio")
UPLOAD_URL    = _get_env("UPLOAD_URL", default=(PUBLIC_BASE + "/upload") if PUBLIC_BASE else "https://matlycreative.com/upload").rstrip("/")

SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: FU1")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_fu1.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))
IGNORE_SENT      = _env_bool("IGNORE_SENT", "0")

# Email copy (single template, no branching)
SUBJECT_TPL = _get_env("SUBJECT", default="Quick follow-up — {Company}")
BODY_TPL    = _get_env("BODY", default=
"""Hey {FirstLine}

Just bumping this in case it got buried.

We edit listing videos for agencies that don’t want the hassle of in-house editing — faster turnarounds, consistent style, zero headaches.

Here are some exemples:
{PortfolioUrl}

If {Company} has a busy pipeline right now, this could take some weight off your plate.
Open to a quick test?

Best,
{FromName}"""
)

log(f"[env] LIST_ID={LIST_ID} | PUBLIC_BASE={PUBLIC_BASE} | PORTFOLIO_URL={PORTFOLIO_URL} | UPLOAD_URL={UPLOAD_URL}")
log(f"[env] SENT_MARKER_TEXT='{SENT_MARKER_TEXT}' | CACHE='{SENT_CACHE_FILE}' | IGNORE_SENT={IGNORE_SENT}")

# ----------------- HTTP -----------------
UA = f"TrelloEmailer-FU1-min/1.0 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ----------------- parsing -----------------
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*[:\-]\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

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
                        val = nxt.strip()
                        i += 1
                out[lab] = val
                break
        i += 1
    return out

def clean_email(raw: str) -> str:
    if not raw: return ""
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

# ----------------- Trello I/O -----------------
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
        except Exception as e:
            if attempt == 2:
                raise
            log(f"[WARN] Trello attempt {attempt+1}/3 failed: {e}")
            time.sleep(1.2 * (attempt + 1))
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
    except Exception as e:
        log(f"[WARN] Could not mark card as sent: {e}")

# ----------------- templating -----------------
def fill(tpl: str, mapping: dict) -> str:
    def repl(m):
        k = m.group(1)
        return str(mapping.get(k, m.group(0)))
    return re.sub(r"\{([A-Za-z0-9_]+)\}", repl, tpl or "")

# ----------------- sender -----------------
def send_email(to_email: str, subject: str, body_text: str):
    from email.message import EmailMessage
    import smtplib

    to_email = clean_one_line(to_email)
    subject  = sanitize_subject(subject)
    body_pt  = (body_text or "").strip() + "\n"

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body_pt)

    if BCC_TO:
        msg["Bcc"] = BCC_TO

    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_DEBUG:
                    s.set_debuglevel(1)
                if SMTP_USE_TLS:
                    s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            log(f"[WARN] SMTP attempt {attempt+1}/3 failed: {e}")
            if attempt == 2:
                raise
            time.sleep(1.0 * (attempt + 1))

# ----------------- cache -----------------
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
    except Exception as e:
        log(f"[WARN] Could not save cache: {e}")

# ----------------- main -----------------
def main():
    missing = []
    for k in ("TRELLO_KEY","TRELLO_TOKEN","FROM_EMAIL","SMTP_PASS","PUBLIC_BASE"):
        if not globals().get(k):
            missing.append(k)
    if not LIST_ID:
        missing.append("TRELLO_LIST_ID_FU1")
    if missing:
        raise SystemExit("Missing env: " + ", ".join(missing))

    sent_cache = load_sent_cache()

    cards = trello_get(f"lists/{LIST_ID}/cards", fields="id,name,desc", limit=200)
    if not isinstance(cards, list):
        log("No cards found or Trello error.")
        return

    processed = 0
    for c in cards:
        if MAX_SEND_PER_RUN and processed >= MAX_SEND_PER_RUN:
            break

        card_id = c.get("id")
        title   = c.get("name", "(no title)")
        if not card_id:
            continue

        if not IGNORE_SENT and card_id in sent_cache:
            log(f"Skip (cache): {title}")
            continue

        if not IGNORE_SENT and already_marked(card_id, SENT_MARKER_TEXT):
            log(f"Skip (marker): {title}")
            sent_cache.add(card_id)
            continue

        desc   = c.get("desc") or ""
        fields = parse_header(desc)

        company = clean_one_line((fields.get("Company") or "").strip()) or clean_one_line(title)
        first   = clean_one_line((fields.get("First") or "").strip())
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)

        if not email_v:
            log(f"Skip: no valid Email on '{title}'.")
            continue

        pid = choose_id(company, email_v)
        personal_url = f"{PUBLIC_BASE}/p/?id={pid}" if PUBLIC_BASE else ""
        portfolio_url = PORTFOLIO_URL
        upload_url = UPLOAD_URL

        first_line = (first + ",") if first else "there,"

        mapping = {
            "Company": company,
            "First": first,
            "FirstLine": first_line,
            "FromName": FROM_NAME,
            "PersonalUrl": personal_url,
            "PortfolioUrl": portfolio_url,
            "UploadUrl": upload_url,
        }

        subject = fill(SUBJECT_TPL, mapping)
        body    = fill(BODY_TPL, mapping).strip()

        log(f"[send] to={email_v} | company='{company}' | first='{first}' | pid={pid}")
        try:
            send_email(email_v, subject, body)
            processed += 1
            log(f"[ok] Sent — '{title}'")
        except Exception as e:
            log(f"[FAIL] Send failed for '{title}' to {email_v}: {e}")
            continue

        if not IGNORE_SENT:
            mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {sanitize_subject(subject)}")
            sent_cache.add(card_id)
            save_sent_cache(sent_cache)

        time.sleep(0.6)

    log(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
