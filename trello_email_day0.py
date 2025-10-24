#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Day-0 — Poll a Trello list and send one email per card.

Key rules:
- Personalized ID = Company slug (fallback to email-safe id).
- READY -> link:  <PUBLIC_BASE>/p/?id=<id>
- NOT READY -> link:  <PORTFOLIO_URL>  (default: <PUBLIC_BASE>/portfolio)
- "Ready" is determined strictly:
    1) If MATLY_POINTER_BASE is set: pointer must exist AND be newer than READY_MAX_AGE_DAYS (default 30).
    2) Else: /api/sample?id=<id> must return a playable src (CF Stream or valid .mp4/.m3u8 HEAD 200).

No “Email me” line in the signature.
"""

import os, re, time, json, html, unicodedata, mimetypes
from datetime import datetime, timezone, timedelta
from typing import Tuple
import requests

def log(*a): print(*a, flush=True)

# ---------- helpers ----------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

def _env_bool(name: str, default: str = "0") -> bool:
    val = os.getenv(name, default)
    return (val or "").strip().lower() in ("1","true","yes","on")

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
    if not re.match(r"^https?://", u or "", flags=re.I):
        u = "https://" + (u or "")
    return u.rstrip("/")

# ---------- env ----------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME",  default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")

SMTP_DEBUG = _env_bool("SMTP_DEBUG", "0")
BCC_TO     = _get_env("BCC_TO", default="").strip()

# Site & portfolio
PUBLIC_BASE   = _norm_base(_get_env("PUBLIC_BASE"))  # e.g., https://matlycreative.com
PORTFOLIO_URL = _norm_base(_get_env("PORTFOLIO_URL")) or (PUBLIC_BASE + "/portfolio")

# Pointer freshness (optional but recommended)
MATLY_POINTER_BASE = _get_env("MATLY_POINTER_BASE", default="").rstrip("/")
READY_MAX_AGE_DAYS = int(_get_env("READY_MAX_AGE_DAYS", default="30"))

# Link appearance
INCLUDE_PLAIN_URL = _env_bool("INCLUDE_PLAIN_URL", "0")
LINK_TEXT         = _get_env("LINK_TEXT",  default="See examples")
LINK_COLOR        = _get_env("LINK_COLOR", default="#1a73e8")

# Sending control
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: Day0")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_day0.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

log(f"[env] PUBLIC_BASE={PUBLIC_BASE} | PORTFOLIO_URL={PORTFOLIO_URL} | POINTER_BASE={MATLY_POINTER_BASE or '(disabled)'}")

# ---------- HTTP ----------
UA = f"TrelloEmailer-Day0/4.0 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ---------- templates ----------
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env("SUBJECT_A", default="Polished videos for {company}'s listings")
    SUBJECT_B = _get_env("SUBJECT_B", default="Polished videos for {company}'s listings")

    BODY_A = _get_env("BODY_A", default=
"""Hi there, hope you're doing well,

I noticed {company} shares great properties, but editing can take valuable time away from your business. I specialize in turning raw footage into clean, polished videos that make listings shine.

Here’s my portfolio with examples of how polished video can make properties more appealing to clients {extra} : {link}

{extra}

If it looks useful, just reply — I’d be happy to chat about handling edits so you can focus on selling.

Best,
Matthieu from Matly""")

    BODY_B = _get_env("BODY_B", default=
"""hi {first}

I noticed {company} shares great properties, but editing can take valuable time away from your business. I specialize in turning raw footage into clean, polished videos that make listings shine.

Here’s my portfolio with examples of how polished video can make properties more appealing to clients {extra} : {link}

{extra}

If it looks useful, just reply — I’d be happy to chat about handling edits so you can focus on selling.

Best,
Matthieu from Matly""")
else:
    SUBJECT_A = "Polished videos for {company}'s listings"
    SUBJECT_B = "Polished videos for {company}'s listings"
    BODY_A = """Hi there, hope you're doing well,

I noticed {company} shares great properties, but editing can take valuable time away from your business. I specialize in turning raw footage into clean, polished videos that make listings shine.

Here’s my portfolio with examples of how polished video can make properties more appealing to clients {extra} : {link}

{extra}

If it looks useful, just reply — I’d be happy to chat about handling edits so you can focus on selling.

Best,
Matthieu from Matly"""
    BODY_B = """hi {first}

I noticed {company} shares great properties, but editing can take valuable time away from your business. I specialize in turning raw footage into clean, polished videos that make listings shine.

Here’s my portfolio with examples of how polished video can make properties more appealing to clients {extra} : {link}

{extra}

If it looks useful, just reply — I’d be happy to chat about handling edits so you can focus on selling.

Best,
Matthieu from Matly"""

# ---------- parsing ----------
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

# ---------- Trello ----------
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
    if extra: text += f"\n{extra}"
    try:
        trello_post(f"cards/{card_id}/actions/comments", text=text)
    except Exception:
        pass

# ---------- readiness checks ----------
_CF_IFRAME = re.compile(r'iframe\.videodelivery\.net/[A-Za-z0-9_-]{8,}', re.I)
_CF_PLAYID = re.compile(r'^[A-Za-z0-9_-]{12,40}$')
_HTTP_MPX  = re.compile(r'^https?://.+\.(mp4|m3u8)(\?.*)?$', re.I)

def _pointer_ready_by_freshness(pid: str) -> bool:
    """Use R2 pointer 'updatedAt' to decide readiness (fresh only)."""
    base = MATLY_POINTER_BASE
    if not base:
        return False
    url = f"{base}/{pid}.json" if not base.endswith("/pointers") and not base.endswith("/pointers/") \
          else f"{base.rstrip('/')}/{pid}.json"
    try:
        r = SESS.get(url, timeout=10, headers={"Accept":"application/json"})
        if r.status_code != 200:
            return False
        data = r.json()
        updated = (data.get("updatedAt") or "").strip()
        if not updated:
            return False
        # handle ...Z
        if updated.endswith("Z"): updated = updated[:-1]
        dt = datetime.fromisoformat(updated).replace(tzinfo=timezone.utc)
        fresh_after = datetime.now(timezone.utc) - timedelta(days=READY_MAX_AGE_DAYS)
        return dt >= fresh_after
    except Exception:
        return False

def _api_ready(pid: str) -> bool:
    check_url = f"{PUBLIC_BASE}/api/sample?id={pid}"
    try:
        r = SESS.get(check_url, timeout=12, headers={"Accept":"application/json"})
        if r.status_code != 200:
            return False
        data = r.json() if r.headers.get("Content-Type","").lower().startswith("application/json") else {}
        if not isinstance(data, dict) or str(data.get("error","")).strip():
            return False
        src = (data.get("src") or data.get("streamUrl") or data.get("signedUrl") or data.get("url") or "").strip()
        if _CF_IFRAME.search(src) or _CF_PLAYID.match(src):
            return True
        if _HTTP_MPX.match(src):
            try:
                h = requests.head(src, timeout=6, allow_redirects=True)
                return h.status_code == 200
            except Exception:
                return False
        return False
    except Exception:
        return False

def is_sample_ready(pid: str) -> bool:
    # Prefer pointer freshness if configured; else fall back to API probing.
    if MATLY_POINTER_BASE:
        ok = _pointer_ready_by_freshness(pid)
        log(f"[ready by pointer] id={pid} -> {ok}")
        if ok:
            return True
        # if pointer says not fresh, treat as NOT ready (don’t fall back to API)
        return False
    ok = _api_ready(pid)
    log(f"[ready by api] id={pid} -> {ok}")
    return ok

# ---------- templating ----------
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

EXTRA_TOKEN = re.compile(r"\{\s*extra\s*\}", flags=re.I)

def fill_with_two_extras(
    tpl: str, *, company: str, first: str, from_name: str,
    link: str, is_ready: bool, extra_ready: str, extra_wait: str
) -> str:
    base = fill_template_skip_extra(
        tpl, company=company, first=first, from_name=from_name, link=link
    )
    if is_ready:
        step1 = EXTRA_TOKEN.sub(extra_ready, base, count=1)
        step2 = EXTRA_TOKEN.sub("",         step1, count=1)
    else:
        step1 = EXTRA_TOKEN.sub("",         base, count=1)
        step2 = EXTRA_TOKEN.sub(extra_wait, step1, count=1)
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

# ---------- signature (no “Email me” line) ----------
EMAIL_FONT_PX         = int(os.getenv("EMAIL_FONT_PX", "16"))
SIGNATURE_LOGO_URL    = os.getenv("SIGNATURE_LOGO_URL", "").strip()
SIGNATURE_INLINE      = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX    = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

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

# ---------- send email (FORCE portfolio on portfolio label) ----------
def send_email(to_email: str, subject: str, body_text: str, *, link_url: str = "", link_text: str = "", link_color: str = ""):
    from email.message import EmailMessage
    import smtplib

    label = (link_text or "See examples").strip() or "See examples"

    # If we're using a portfolio label, hard-force the portfolio URL.
    if label.lower() in {"my portfolio", "see examples", os.getenv("LINK_TEXT", "See examples").strip().lower()}:
        link_url = PORTFOLIO_URL

    if link_url and not re.match(r"^https?://", link_url, flags=re.I):
        link_url = "https://" + link_url

    full = link_url
    bare = re.sub(r"^https?://", "", full, flags=re.I) if full else ""
    esc_full = html.escape(full, quote=True) if full else ""
    esc_bare = html.escape(bare, quote=True) if full else ""

    # Plain text
    body_pt = body_text
    if full:
        if not INCLUDE_PLAIN_URL:
            for pat in (full, bare):
                if pat:
                    body_pt = body_pt.replace(pat, label)
        else:
            if full not in body_pt and bare not in body_pt:
                body_pt = (body_pt.rstrip() + "\n\n" + full).strip()

    # HTML
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
        style_attr = f' style="color:{html.escape(link_color or LINK_COLOR)};text-decoration:underline;"'
        anchor = f'<a{style_attr} href="{html.escape(full, quote=True)}">{html.escape(label)}</a>'
        html_core = html_core.replace(MARK, anchor) if MARK in html_core else (html_core + f"<p>{anchor}</p>")

    # Signature
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    if BCC_TO: msg["Bcc"] = BCC_TO
    msg["Reply-To"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)
    msg.add_alternative(html_full, subtype="html")

    if SIGNATURE_INLINE and SIGNATURE_LOGO_URL:
        try:
            r = requests.get(SIGNATURE_LOGO_URL, timeout=20)
            r.raise_for_status()
            data = r.content
            ctype = r.headers.get("Content-Type") or mimetypes.guess_type(SIGNATURE_LOGO_URL)[0] or "image/png"
            if not ctype.startswith("image/"): ctype = "image/png"
            maintype, subtype = ctype.split("/", 1)
            msg.get_payload()[-1].add_related(data, maintype=maintype, subtype=subtype, cid="siglogo@local")
        except Exception as e:
            log(f"Inline logo fetch failed, sending without embed: {e}")

    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_DEBUG: s.set_debuglevel(1)
                if SMTP_USE_TLS: s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            log(f"[WARN] SMTP attempt {attempt+1}/3 failed: {e}")
            if attempt == 2: raise
            time.sleep(1.2 * (attempt + 1))

# ---------- cache ----------
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

# ---------- main ----------
def main():
    missing = []
    if not TRELLO_KEY:   missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN: missing.append("TRELLO_TOKEN")
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_DAY0")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS")
    if not PUBLIC_BASE:  missing.append("PUBLIC_BASE")
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

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
        title   = c.get("name","(no title)")
        if not card_id or card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        fields  = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First")   or "").strip()
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)
        if not email_v:
            log(f"Skip: no valid Email on card '{title}'.")
            continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            log(f"Skip: already marked '{SENT_MARKER_TEXT}' — {title}")
            sent_cache.add(card_id)
            continue

        pid = choose_id(company, email_v)
        ready = is_sample_ready(pid)

        chosen_link = (f"{PUBLIC_BASE}/p/?id={pid}" if ready else PORTFOLIO_URL)

        use_b    = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B    if use_b else BODY_A

        subject = fill_template(
            subj_tpl,
            company=company, first=first, from_name=FROM_NAME, link=chosen_link
        )

        extra_ready = "as well as a free sample made with your content"
        extra_wait  = "If you can share 1–2 raw clips, I’ll cut a quick sample for you this week (free)."

        body = fill_with_two_extras(
            body_tpl,
            company=company,
            first=first,
            from_name=FROM_NAME,
            link=chosen_link,
            is_ready=ready,
            extra_ready=extra_ready,
            extra_wait=extra_wait
        )

        link_label = "Portfolio + Sample (free)" if ready else LINK_TEXT

        try:
            send_email(
                email_v, subject, body,
                link_url=chosen_link,
                link_text=link_label,
                link_color=LINK_COLOR
            )
            processed += 1
            log(f"Sent to {email_v} — '{title}' — id={pid} ready={ready} link={chosen_link}")
        except Exception as e:
            log(f"Send failed for '{title}' to {email_v}: {e}")
            continue

        mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(0.8)

    log(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
