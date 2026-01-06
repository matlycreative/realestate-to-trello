#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FU3 — Poll Trello and send one email per card (final nudge for free sample).

CHANGES (per your request):
- Design removed (NO HTML, NO logo, NO card).
- URLs are clickable by keeping them as RAW URLs in plain text.
- [here] is replaced with the UPLOAD_URL (raw URL).

STRICT RULES (match Day-0/FU1/FU2/FU3 intent):
- Personalized ID = Company slug (fallback email-safe).
- READY -> link to personal page   : <PUBLIC_BASE>/p/?id=<id>
- NOT READY -> link to portfolio   : <PORTFOLIO_URL>  (defaults to <PUBLIC_BASE>/portfolio)
- With MATLY_POINTER_BASE: pointer must exist, be fresh, AND filename must contain 'sample'.
- Add a clickable [here] link that points to UPLOAD_URL (default https://matlycreative.com/upload/).
- No "Email me" contact line; no hidden overrides in send_email().
"""

import os
import re
import time
import json
import html
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict

import requests


def log(*a):  # tiny logger
    print(*a, flush=True)


# ----------------- tiny utils -----------------
def _get_env(*names, default: str = "") -> str:
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
    if not s:
        return ""
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
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")


# ----------------- env -----------------
TRELLO_KEY = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
# FU3 uses its own list ID
LIST_ID = _get_env("TRELLO_LIST_ID_FU3", "TRELLO_LIST_ID")

FROM_NAME = _get_env("FROM_NAME", default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
SMTP_PASS = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER = _get_env(
    "SMTP_USER",
    "SMTP_USERNAME",
    "smtp_user",
    "smtp_username",
    "FROM_EMAIL",
)
SMTP_DEBUG = _env_bool("SMTP_DEBUG", "0")
BCC_TO = _get_env("BCC_TO", default="").strip()

PUBLIC_BASE = _norm_base(_get_env("PUBLIC_BASE"))  # e.g., https://matlycreative.com
PORTFOLIO_URL = _norm_base(_get_env("PORTFOLIO_URL")) or (PUBLIC_BASE + "/portfolio")
UPLOAD_URL = _get_env("UPLOAD_URL", default="https://matlycreative.com/upload/").rstrip(
    "/"
)

# Pointer readiness (optional)
MATLY_POINTER_BASE = _get_env("MATLY_POINTER_BASE", default="").rstrip("/")
READY_MAX_AGE_DAYS = int(_get_env("READY_MAX_AGE_DAYS", default="30"))

# Visuals (kept for compatibility; not used now that design is removed)
LINK_COLOR = _get_env("LINK_COLOR", default="#858585")

# Send control (FU3-specific)
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: FU3")
SENT_CACHE_FILE = _get_env("SENT_CACHE_FILE", default=".data/sent_fu3.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

log(
    f"[env] PUBLIC_BASE={PUBLIC_BASE} | PORTFOLIO_URL={PORTFOLIO_URL} | "
    f"UPLOAD_URL={UPLOAD_URL} | POINTER_BASE={MATLY_POINTER_BASE or '(disabled)'}"
)

# ----------------- HTTP -----------------
UA = f"TrelloEmailer-FU3/1.1 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ----------------- templates -----------------
USE_ENV_TEMPLATES = (
    os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1", "true", "yes", "on")
)
if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env(
        "SUBJECT_A", default="Before I close this — free sample for {Company}?"
    )
    SUBJECT_B = _get_env(
        "SUBJECT_B", default="Before I close this — free sample for {Company}?"
    )
    BODY_A = _get_env(
        "BODY_A",
        default="""Hi there,
I only do a few free samples per week, and I’ve got time to fit one more in for {Company}.

If you want one for an upcoming listing, you can upload 4-5 raw clips [here].
I’ll cut a clean, cinematic preview so you can see exactly how your listings could look with a sharper style.

If not, no worries — just tell me and I’ll close the loop.

Best,
Matthieu from Matly""",
    )
    BODY_B = _get_env(
        "BODY_B",
        default="""Hey {first},
I only do a few free samples per week, and I’ve got time to fit one more in for {Company}.

If you want one for an upcoming listing, you can upload 4-5 raw clips [here].
I’ll cut a clean, cinematic preview so you can see exactly how your listings could look with a sharper style.

If not, no worries — just tell me and I’ll close the loop.

Best,
Matthieu from Matly
""",
    )
else:
    SUBJECT_A = "Before I close this — free sample for {Company}?"
    SUBJECT_B = "Before I close this — free sample for {Company}?"
    BODY_A = """Hi there,
I only do a few free samples per week, and I’ve got time to fit one more in for {Company}.

If you want one for an upcoming listing, you can upload 4-5 raw clips [here].
I’ll cut a clean, cinematic preview so you can see exactly how your listings could look with a sharper style.

If not, no worries — just tell me and I’ll close the loop.

Best,
Matthieu from Matly"""
    BODY_B = """Hey {first},
I only do a few free samples per week, and I’ve got time to fit one more in for {Company}.

If you want one for an upcoming listing, you can upload 4-5 raw clips [here].
I’ll cut a clean, cinematic preview so you can see exactly how your listings could look with a sharper style.

If not, no worries — just tell me and I’ll close the loop.

Best,
Matthieu from Matly"""

# ----------------- parsing -----------------
TARGET_LABELS = ["Company", "First", "Email", "Hook", "Variant", "Website"]
LABEL_RE: Dict[str, re.Pattern] = {
    lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*[:\-]\s*(.*)$")
    for lab in TARGET_LABELS
}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)


def parse_header(desc: str) -> dict:
    out = {k: "" for k in TARGET_LABELS}
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                val = (m.group(1) or "").strip()
                if not val and (i + 1) < len(lines):
                    nxt = lines[i + 1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip()
                        i += 1
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


# ----------------- Trello I/O -----------------
def _trello_call(method: str, url_path: str, **params):
    for attempt in range(3):
        try:
            params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
            url = f"https://api.trello.com/1/{url_path.lstrip('/')}"
            if method == "GET":
                r = SESS.get(url, params=params, timeout=30)
            else:
                r = SESS.post(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError("Unreachable")


def trello_get(url_path: str, **params):
    return _trello_call("GET", url_path, **params)


def trello_post(url_path: str, **params):
    return _trello_call("POST", url_path, **params)


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


# ----------------- readiness -----------------
def _pointer_ready(pid: str) -> bool:
    """Pointer must exist, be fresh, AND filename must include 'sample'."""
    base = MATLY_POINTER_BASE
    if not base:
        return False
    if not base.endswith("/pointers") and not base.endswith("/pointers/"):
        base = base.rstrip("/") + "/pointers"
    url = f"{base.rstrip('/')}/{pid}.json"
    try:
        r = SESS.get(url, timeout=10, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return False
        data = r.json()
        fname = (data.get("filename") or "").lower()
        if "sample" not in fname:
            return False
        updated = (data.get("updatedAt") or "").strip()
        if not updated:
            return False
        if updated.endswith("Z"):
            updated = updated[:-1]
        dt = datetime.fromisoformat(updated).replace(tzinfo=timezone.utc)
        fresh_after = datetime.now(timezone.utc) - timedelta(days=READY_MAX_AGE_DAYS)
        return dt >= fresh_after
    except Exception:
        return False


def _api_ready(pid: str) -> bool:
    """Fallback: /api/sample must 200 with a playable src."""
    check_url = f"{PUBLIC_BASE}/api/sample?id={pid}"
    try:
        r = SESS.get(check_url, timeout=12, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return False
        data = (
            r.json()
            if r.headers.get("Content-Type", "").lower().startswith("application/json")
            else {}
        )
        if not isinstance(data, dict) or str(data.get("error", "")).strip():
            return False
        src = (
            data.get("src")
            or data.get("streamUrl")
            or data.get("signedUrl")
            or data.get("url")
            or ""
        ).strip()
        if not src:
            return False
        if re.search(r"iframe\.videodelivery\.net/[A-Za-z0-9_-]{8,}", src, re.I):
            return True
        if re.match(r"^[A-Za-z0-9_-]{12,40}$", src):
            return True
        if re.match(r"^https?://.+\.(mp4|m3u8)(\?.*)?$", src, re.I):
            return True
        return False
    except Exception:
        return False


def is_sample_ready(pid: str) -> bool:
    if MATLY_POINTER_BASE:
        ok = _pointer_ready(pid)
        log(f"[ready pointer] id={pid} -> {ok}")
        return ok
    ok = _api_ready(pid)
    log(f"[ready api] id={pid} -> {ok}")
    return ok


# ----------------- templating -----------------
def fill_template(
    tpl: str, *, company: str, first: str, from_name: str, link: str = "", extra: str = ""
) -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":
            return company or ""
        if key == "first":
            return first or ""
        if key == "from_name":
            return from_name or ""
        if key == "link":
            return link or ""
        if key == "extra":
            return extra or ""
        return m.group(0)

    return re.sub(r"{\s*(company|first|from_name|link|extra)\s*}", repl, tpl, flags=re.I)


def sanitize_subject(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]


# ----------------- sender (NO DESIGN + CLICKABLE URLs) -----------------
def send_email(to_email: str, subject: str, body_text: str):
    """
    Plain text only. URLs are clickable by leaving them as raw URLs.
    [here] is replaced with UPLOAD_URL (raw URL).

    FIX:
    - Normalize body to safe SMTP-friendly plain text
    - Remove weird trailing whitespace / mixed newlines that can break sending
    """
    from email.message import EmailMessage
    import smtplib

    body_pt = (body_text or "")

    # Normalize newlines (important when BODY_B comes from env/templates)
    body_pt = body_pt.replace("\r\n", "\n").replace("\r", "\n")

    # Replace token with raw URL
    if "[here]" in body_pt:
        body_pt = body_pt.replace("[here]", UPLOAD_URL)

    # Strip trailing whitespace on each line + trim the whole message
    body_pt = "\n".join(line.rstrip() for line in body_pt.split("\n")).strip() + "\n"

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)

    # Explicit charset avoids edge cases on some SMTP servers
    msg.set_content(body_pt, subtype="plain", charset="utf-8")

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
    except Exception:
        pass


# ----------------- main -----------------
def main():
    missing = []
    for k in ("TRELLO_KEY", "TRELLO_TOKEN", "FROM_EMAIL", "SMTP_PASS", "PUBLIC_BASE"):
        if not globals()[k]:
            missing.append(k)
    if not LIST_ID:
        missing.append("TRELLO_LIST_ID_FU3")
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
        title = c.get("name", "(no title)")
        if not card_id or card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        fields = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first = (fields.get("First") or "").strip()
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)
        if not email_v:
            log(f"Skip: no valid Email on '{title}'.")
            continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            log(f"Skip: already marked '{SENT_MARKER_TEXT}' — {title}")
            sent_cache.add(card_id)
            continue

        pid = choose_id(company, email_v)
        ready = is_sample_ready(pid)
        chosen_link = f"{PUBLIC_BASE}/p/?id={pid}" if ready else PORTFOLIO_URL
        log(f"[decide] id={pid} ready={ready} -> link={chosen_link}")

        use_b = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B if use_b else BODY_A

        subject = fill_template(
            subj_tpl,
            company=company,
            first=first,
            from_name=FROM_NAME,
            link=chosen_link,
        )

        body = fill_template(
            body_tpl,
            company=company,
            first=first,
            from_name=FROM_NAME,
            link=chosen_link,
            extra="",
        )

        try:
            send_email(email_v, subject, body)
            processed += 1
            log(f"Sent FU3 to {email_v} — '{title}' — ready={ready}")
        except Exception as e:
            log(f"Send failed for '{title}' to {email_v}: {e}")
            continue

        mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(0.8)

    log(f"Done. FU3 emails sent: {processed}")


if __name__ == "__main__":
    main()
