#!/usr/bin/env python3
"""
Day-0 — Poll a Trello list and send one email per card.

- Parse Company / First / Email from the card description.
- Choose template A (no First) or B (has First).
- Build a personalized link from PUBLIC_BASE + the prospect's email.
- If /api/sample?id=... returns a stream URL, prefer the API's 'link' for the email;
  otherwise, use PORTFOLIO_URL (or PUBLIC_BASE).
- You can override API linking with USE_API_LINK=0 to always use your PUBLIC_BASE.
- Send via SMTP (plain text + HTML; optional inline <signature logo).
- Mark the card as "Sent" to avoid re-sending (local cache + Trello comment).
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
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME", default="Outreach")
FROM_EMAIL = _get_env("FROM_EMAIL")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")

# Diagnostics & delivery helpers
SMTP_DEBUG = _env_bool("SMTP_DEBUG", "0")
BCC_TO     = _get_env("BCC_TO", default="").strip()

# ----------------- Templates -----------------
# Default to using templates from env (so workflow secrets are respected).
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
log(f"[tpl] Using {'ENV' if USE_ENV_TEMPLATES else 'HARDCODED'} templates")

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

# Appearance / signature
EMAIL_FONT_PX         = int(os.getenv("EMAIL_FONT_PX", "16"))
SIGNATURE_LOGO_URL    = os.getenv("SIGNATURE_LOGO_URL", "").strip()
SIGNATURE_INLINE      = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX    = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

INCLUDE_PLAIN_URL = _env_bool("INCLUDE_PLAIN_URL", "0")

# Sending control
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: Day0")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_day0.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

# Links
PUBLIC_BASE   = _get_env("PUBLIC_BASE")       # e.g., https://yourdomain.com
LINK_TEXT     = _get_env("LINK_TEXT", default="My portfolio")
LINK_COLOR    = _get_env("LINK_COLOR", default="")
PORTFOLIO_URL = _get_env("PORTFOLIO_URL", default="")  # falls back to PUBLIC_BASE if blank
USE_API_LINK  = _env_bool("USE_API_LINK", "1")         # 0 -> force PUBLIC_BASE link

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
UA = f"TrelloEmailer-Day0/2.0 (+{FROM_EMAIL or 'no-email'})"
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
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_DAY0")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS (or SMTP_PASSWORD / smtp_pass)")
    if not PUBLIC_BASE:  missing.append("PUBLIC_BASE (e.g., https://yourdomain.com)")
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

# --- Two-{extra} logic: fill only the correct slot ---
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
def send_email(
    to_email: str,
    subject: str,
    body_text: str,
    *,
    link_url: str = "",
    link_text: str = "",
    link_color: str = "",
):
    from email.message import EmailMessage
    import smtplib

    # Normalize link and label
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

    # HTML body: mark link locations, autolink other URLs, then insert anchor
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

    # Append signature (optional inline logo)
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    if BCC_TO:
        msg["Bcc"] = BCC_TO
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)
    msg.add_alternative(html_full, subtype="html")

    if SIGNATURE_INLINE and SIGNATURE_LOGO_URL:
        try:
            r = SESS.get(SIGNATURE_LOGO_URL, timeout=20)
            r.raise_for_status()
            data = r.content
            ctype = r.headers.get("Content-Type") or mimetypes.guess_type(SIGNATURE_LOGO_URL)[0] or "image/png"
            if not ctype.startswith("image/"):
                ctype = "image/png"
            maintype, subtype = ctype.split("/", 1)
            msg.get_payload()[-1].add_related(data, maintype=maintype, subtype=subtype, cid=logo_cid)
        except Exception as e:
            log(f"Inline logo fetch failed, sending without embed: {e}")

    # Send with retries
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
            time.sleep(1.5 * (attempt + 1))

def _sample_info(safe_id: str) -> Tuple[bool, str]:
    """
    Query /api/sample?id=<safe_id> (WordPress).
    Returns (is_ready, best_link_to_use).

    WordPress now returns: { id, src, embedType, link }
    Treat presence of 'src' (or legacy streamUrl/signedUrl/url) as ready.
    If USE_API_LINK=0, always return /p/?id=<id> while still marking ready=True.
    """
    check_url = f"{PUBLIC_BASE}/api/sample?id={safe_id}"
    log(f"[ready?] id={safe_id}")
    log(f"[ready?] GET {check_url}")

    try:
        r = SESS.get(check_url, timeout=15)
        preview = (r.text or "")[:300]
        log(f"[ready?] HTTP {r.status_code} :: {preview!r}")
        if not r.ok:
            return (False, PORTFOLIO_URL)

        try:
            data = r.json()
        except Exception:
            log("[ready?] non-JSON response")
            return (False, PORTFOLIO_URL)

        # New WP shape: 'src' + 'embedType' + 'link'
        # (also accept legacy keys to be safe)
        src = (data.get("src") or
               data.get("streamUrl") or
               data.get("signedUrl") or
               data.get("url"))

        if not src:
            err = data.get("error")
            if err:
                log(f"[ready?] API error: {err}")
            return (False, PORTFOLIO_URL)

        # Normalize 'link' (what we put in the email)
        api_link = (data.get("link") or "").strip()
        if api_link:
            if api_link.startswith("/"):
                api_link = f"{PUBLIC_BASE}{api_link}"
            elif not re.match(r"^https?://", api_link, flags=re.I):
                api_link = f"{PUBLIC_BASE.rstrip('/')}/{api_link.lstrip('/')}"

        # Respect override: if USE_API_LINK=0 -> always use your site /p/?id=<id>
        if not USE_API_LINK:
            best = f"{PUBLIC_BASE}/p/?id={safe_id}"
        else:
            best = api_link if api_link else f"{PUBLIC_BASE}/p/?id={safe_id}"

        log(f"[link] chosen is_ready={True} USE_API_LINK={USE_API_LINK} -> {best}")
        return (True, best)

    except Exception as e:
        log(f"[ready?] error: {e}")
        return (False, PORTFOLIO_URL)

# --------------- Main Flow ----------------
def main():
    require_env()
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
        if not card_id:
            continue
        if card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        fields = parse_header(desc)
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

        # -------- Build links + decide which to send now --------
        safe_id = _safe_id_from_email(email_v)
        is_ready, chosen_link = _sample_info(safe_id)  # uses API link when ready (unless override)

        # Choose template A/B *before* composing extra
        use_b    = bool(first)  # B if First is present
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B    if use_b else BODY_A

        n_extra = len(EXTRA_TOKEN.findall(body_tpl or ""))
        log(f"[compose] template={'B' if use_b else 'A'} extras={n_extra} ready={is_ready} link={chosen_link}")

        # --- Subject ---
        subject = fill_template(
            subj_tpl,
            company=company, first=first, from_name=FROM_NAME, link=chosen_link
        )

        # --- Two different extra lines depending on readiness ---
        extra_ready = "as well as a free sample made with your content"
        extra_wait  = "If you can share 1–2 raw clips, I’ll cut a quick sample for you this week (free)."

        # --- Build the body with two-{extra} logic ---
        body = fill_with_two_extras(
            body_tpl,
            company=company,
            first=first,
            from_name=FROM_NAME,
            link=chosen_link,
            is_ready=is_ready,
            extra_ready=extra_ready,
            extra_wait=extra_wait
        )

        # --- Link label for the anchor/button ---
        link_label = "Portfolio + Sample (free)" if is_ready else (LINK_TEXT or "My portfolio")

        # Send
        try:
            send_email(
                email_v,
                subject,
                body,
                link_url=chosen_link,
                link_text=link_label,
                link_color=LINK_COLOR
            )
            processed += 1
            log(f"Sent to {email_v} — card '{title}' (type {'B' if use_b else 'A'}) — link={'video' if is_ready else 'portfolio'} :: {chosen_link}")
        except Exception as e:
            log(f"Send failed for '{title}' to {email_v}: {e}")
            continue

        # Mark sent (Trello + cache)
        mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(1.0)

    log(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
