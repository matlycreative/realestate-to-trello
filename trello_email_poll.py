#!/usr/bin/env python3
"""
Poll a Trello list (Day 0). For each card found:
- Read Company / First / Email from the card description (header block).
- Choose template A (no "First") or B (has "First").
- Fill {company}, {first}, {from_name}, {link} placeholders.
- Build a personalized link from PUBLIC_BASE + the prospect's email.
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

def _safe_id_from_email(email: str) -> str:
    """Lowercase and replace @ and . with _ to match how your site expects IDs."""
    return (email or "").strip().lower().replace("@", "_").replace(".", "_")

# ----------------- Config / Env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")  # prefer DAY0, fallback LIST_ID

# Email templates (from GitHub Secrets; keep them as plain text)
SUBJECT_A = _get_env("SUBJECT_A", default="Quick idea for {company}")
BODY_A    = _get_env("BODY_A",    default="Hi there,\n\nWe help {company}...\n\n– {from_name}\n\n{link}")
SUBJECT_B = _get_env("SUBJECT_B", default="Quick idea for {company}")
BODY_B    = _get_env("BODY_B",    default="Hi {first},\n\nWe help {company}...\n\n– {from_name}\n\n{link}")

# From identity
FROM_NAME  = _get_env("FROM_NAME", default="Outreach")
FROM_EMAIL = _get_env("FROM_EMAIL")

# SMTP (tolerant names + sensible defaults)
SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")

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

# Link pieces
PUBLIC_BASE = _get_env("PUBLIC_BASE")  # e.g., https://matlycreative.pages.dev
LINK_TEXT   = _get_env("LINK_TEXT", default="View your tailored sample")
LINK_COLOR  = _get_env("LINK_COLOR", default="")  # optional CSS color

# HTTP session
UA = f"TrelloEmailer/1.3 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# Header labels we expect in the card description
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# Friendly link toggles
APPEND_FRIENDY_LINK = os.getenv("APPEND_FRIENDLY_LINK", "0").strip().lower() in ("1","true","yes","on")
INCLUDE_PLAIN_URL   = os.gentenv("INCLUDE_PLAIN_URL", "0".strip().lower() in ("1","true","yes","on")

# --------------- Helpers ----------------
def require_env():
    missing = []
    if not TRELLO_KEY:  missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN: missing.append("TRELLO_TOKEN")
    if not LIST_ID:      missing.append("TRELLO_LIST_ID_DAY0")
    if not FROM_EMAIL:   missing.append("FROM_EMAIL")
    if not SMTP_PASS:    missing.append("SMTP_PASS (or SMTP_PASSWORD / smtp_pass)")
    if not PUBLIC_BASE:  missing.append("PUBLIC_BASE (e.g., https://matlycreative.pages.dev)")

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
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name|link)\s*}", repl, tpl, flags=re.I)

def sanitize_subject(s: str) -> str:
    """Avoid header injection and weird linebreaks in Subject."""
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]

def text_to_html(text: str) -> str:
    """Convert plain text to simple HTML with a bigger font."""
    esc = html.escape(text or "")
    esc = esc.replace("\r\n", "\n").replace("\r", "\n")
    esc = esc.replace("\n\n", "</p><p>").replace("\n", "<br>")
    return (
        f'<div style="font-family:Arial,Helvetica,sans-serif;'
        f'font-size:{EMAIL_FONT_PX}px;line-height:1.6;color:#111;">'
        f'<p>{esc}</p></div>'
    )

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

_URL_RE = re.compile(r"https?://[^\s<>\")']+")

def _autolink_html(escaped_html: str) -> str:
    # Turn visible URLs into <a href="...">...</a>
    def _wrap(m):
        url = m.group(0)
        esc = html.escape(url, quote=True)
        return f'<a href="{esc}">{esc}</a>'
    return _URL_RE.sub(_wrap, escaped_html)

def send_email(to_email: str, subject: str, body_text: str, *, link_url: str = "", link_text: str = "", link_color: str = ""):
    import smtplib
    from email.message import EmailMessage

    # Build HTML from plain text first
    html_core = text_to_html(body_text)

    # Normalize/link pieces
    if link_url and not re.match(r"^https?://", link_url, flags=re.I):
        link_url = "https://" + link_url
    esc_u = html.escape(link_url, quote=True) if link_url else ""
    friendly = html.escape(link_text or link_url or "")
    style_attr = f' style="color:{html.escape(link_color)};text-decoration:underline;"' if link_color else ""
    anchor_html = f'<a{style_attr} href="{esc_u}">{friendly}</a>' if link_url else ""

    # 1) Autolink any bare URLs first
    html_core = _autolink_html(html_core)

    # 2) Force-convert any visible raw URL to a friendly anchor
    if link_url:
        # If an existing anchor shows the URL as its text, replace its inner text
        html_core = html_core.replace(f'>{esc_u}<', f'>{friendly}<')
        # Replace any remaining plain occurrences of the URL with the friendly anchor
        html_core = html_core.replace(esc_u, anchor_html)

        # Do we already have an anchor with that href?
        has_anchor = (f'href="{esc_u}"' in html_core)

        # 3) Only append a friendly anchor if explicitly allowed and none exists
        if APPEND_FRIENDLY_LINK and not has_anchor:
            html_core += f'<p>{anchor_html}</p>'

    # 4) Plain-text part: include the naked URL only if enabled
    if INCLUDE_PLAIN_URL and link_url and link_url not in body_text:
        body_text = (body_text.rstrip() + "\n\n" + link_url).strip()

    # Signature + message assembly
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_text)                      # plain text fallback
    msg.add_alternative(html_full, subtype="html")  # HTML

    # Optional inline logo
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
            print(f"Inline logo fetch failed, sending without embed: {e}")

    # SMTP send with retry
    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_USE_TLS:
                    s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))

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
    trello_post(f"cards/{card_id}/actions/comments", text=text)

# --------------- Main Flow ----------------
def main():
    require_env()
    sent_cache = load_sent_cache()

    cards = trello_get(f"lists/{LIST_ID}/cards", fields="id,name,desc", limit=200)
    if not isinstance(cards, list):
        print("No cards found or Trello error.")
        return

    processed = 0
    for c in cards:
        if MAX_SEND_PER_RUN and processed >= MAX_SEND_PER_RUN:
            break

        card_id = c.get("id")
        if not card_id or card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        # Parse header block
        TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
        fields = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First")   or "").strip()
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)  # fallback: scan whole desc

        if not email_v:
            print(f"Skip: no valid Email on card '{c.get('name','(no title)')}'.")
            continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            print(f"Skip: already marked '{SENT_MARKER_TEXT}' — {c.get('name','(no title)')}")
            sent_cache.add(card_id)
            continue

        # Build personalized link from the email
        safe = _safe_id_from_email(email_v)
        link_url = f"{PUBLIC_BASE.rstrip('/')}/p/?id={safe}"

        # Choose template A/B and inject placeholders (including {link})
        use_b = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B if use_b else BODY_A

        subject = fill_template(subj_tpl, company=company, first=first, from_name=FROM_NAME, link=link_url)
        body    = fill_template(body_tpl, company=company, first=first, from_name=FROM_NAME, link=link_url)

        # Send the email
        try:
            send_email(
                email_v,
                subject,
                body,
                link_url=link_url,
                link_text=LINK_TEXT,
                link_color=LINK_COLOR
            )
            processed += 1
            print(f"Sent to {email_v} — card '{c.get('name','(no title)')}' (type {'B' if use_b else 'A'}) | {link_url}")
        except Exception as e:
            print(f"Send failed for '{c.get('name','(no title)')}' to {email_v}: {e}")
            continue

        # Mark sent + cache
        try:
            mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}\nLink: {link_url}")
        except Exception:
            pass
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(1.0)

    print(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
