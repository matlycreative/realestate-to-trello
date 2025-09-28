#!/usr/bin/env python3
"""
Poll a Trello list (Day 0). For each card found:
- Read Company / First / Email from the card description (header block).
- Choose template A (no "First") or B (has "First").
- Fill {company}, {first}, {from_name} placeholders.
- Send the email via SMTP (plain text + HTML alternative).
- Append signature (optional logo).
- Mark the card as "Sent: Day0" (comment) to avoid re-sending.
"""

import os, re, time, json, html, mimetypes
from datetime import datetime
import requests

# ----------------- Config / Env -----------------
TRELLO_KEY   = os.getenv("TRELLO_KEY", "").strip()
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN", "").strip()
LIST_ID      = os.getenv("TRELLO_LIST_ID_DAY0", "").strip()

# Email templates (from GitHub Secrets; keep them as plain text)
SUBJECT_A = os.getenv("SUBJECT_A", "Quick idea for {company}")
BODY_A    = os.getenv("BODY_A", "Hi there,\n\nWe help {company}...\n\n– {from_name}")
SUBJECT_B = os.getenv("SUBJECT_B", "Quick idea for {company}")
BODY_B    = os.getenv("BODY_B", "Hi {first},\n\nWe help {company}...\n\n– {from_name}")

# From identity
FROM_NAME  = os.getenv("FROM_NAME", "Outreach")
FROM_EMAIL = os.getenv("FROM_EMAIL", "").strip()

# SMTP (names with fallbacks)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USE_TLS = (os.getenv("SMTP_USE_TLS", "1").strip().lower() in ("1","true","yes","on"))

SMTP_PASS = (
    os.getenv("SMTP_PASS") or
    os.getenv("SMTP_PASSWORD") or
    ""
)
SMTP_USER = (
    os.getenv("SMTP_USER") or
    os.getenv("SMTP_USERNAME") or
    os.getenv("FROM_EMAIL") or
    os.getenv("SMTP_PASS") or
    ""
)

# HTML styling + signature logo
EMAIL_FONT_PX       = int(os.getenv("EMAIL_FONT_PX", "16"))  # tweak font size here
SIGNATURE_LOGO_URL  = os.getenv("SIGNATURE_LOGO_URL", "").strip()  # public https URL to your logo
SIGNATURE_INLINE    = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX  = int(os.getenv("SIGNATURE_MAX_W_PX", "200"))  # max logo width

# Poll behavior
SENT_MARKER_TEXT = os.getenv("SENT_MARKER_TEXT", "Sent: Day0")
SENT_CACHE_FILE  = os.getenv("SENT_CACHE_FILE", ".data/sent_day0.json")

# HTTP session
UA = f"TrelloEmailer/1.1 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# Header labels we expect in the card description
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# --------------- Helpers ----------------
def require_env():
    missing = []
    for n in ("TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID_DAY0","FROM_EMAIL","SMTP_PASS"):
        if not os.getenv(n):
            missing.append(n)
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")
    if not SMTP_USER:
        print("Warning: SMTP_USER is empty; using FROM_EMAIL or (last resort) SMTP_PASS as username.")

def trello_get(url_path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    r = SESS.get(f"https://api.trello.com/1/{url_path.lstrip('/')}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def trello_post(url_path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    r = SESS.post(f"https://api.trello.com/1/{url_path.lstrip('/')}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def load_sent_cache():
    try:
        with open(SENT_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_sent_cache(ids):
    os.makedirs(os.path.dirname(SENT_CACHE_FILE), exist_ok=True)
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
        matched = False
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                matched = True
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
    if not raw:
        return ""
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

def fill_template(tpl: str, *, company: str, first: str, from_name: str) -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name)\s*}", repl, tpl, flags=re.I)

def text_to_html(text: str) -> str:
    """Convert plain text to simple HTML with a bigger font."""
    esc = html.escape(text or "")
    # paragraph-ish formatting
    esc = esc.replace("\r\n", "\n").replace("\r", "\n")
    esc = esc.replace("\n\n", "</p><p>").replace("\n", "<br>")
    return (
        f'<div style="font-family:Arial,Helvetica,sans-serif;'
        f'font-size:{EMAIL_FONT_PX}px;line-height:1.6;color:#111;">'
        f'<p>{esc}</p></div>'
    )

def signature_html(logo_cid: str | None) -> str:
    parts = [f'<p style="margin:16px 0 0 0;">– {html.escape(FROM_NAME)}</p>']
    if SIGNATURE_LOGO_URL:
        if SIGNATURE_INLINE and logo_cid:
            img_src = f"cid:{logo_cid}"
        else:
            img_src = html.escape(SIGNATURE_LOGO_URL)
        parts.append(
            f'<div style="margin-top:8px;"><img src="{img_src}" alt="" '
            f'style="max-width:{SIGNATURE_MAX_W_PX}px;height:auto;border:0;display:block;"></div>'
        )
    return "".join(parts)

def send_email(to_email: str, subject: str, body_text: str):
    import smtplib
    from email.message import EmailMessage

    # Prepare HTML version
    html_core = text_to_html(body_text)
    logo_cid = "siglogo@local"
    html_full = html_core + signature_html(logo_cid if SIGNATURE_INLINE and SIGNATURE_LOGO_URL else None)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = subject

    # Plain text (fallback) + HTML alternative
    msg.set_content(body_text)
    msg.add_alternative(html_full, subtype="html")

    # Optionally embed the logo inline (CID)
    if SIGNATURE_INLINE and SIGNATURE_LOGO_URL:
        try:
            r = SESS.get(SIGNATURE_LOGO_URL, timeout=20)
            r.raise_for_status()
            data = r.content
            ctype = r.headers.get("Content-Type") or mimetypes.guess_type(SIGNATURE_LOGO_URL)[0] or "image/png"
            if not ctype.startswith("image/"):
                ctype = "image/png"
            maintype, subtype = ctype.split("/", 1)
            # The last payload is the HTML part; attach related image to it
            msg.get_payload()[-1].add_related(data, maintype=maintype, subtype=subtype, cid=logo_cid)
        except Exception as e:
            # Fall back silently if inline embedding fails
            print(f"Inline logo fetch failed, sending without embed: {e}")

    # Send
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_USE_TLS:
            s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

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
        card_id = c.get("id")
        if not card_id or card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        fields = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First")   or "").strip()
        email_v = clean_email(fields.get("Email") or "")

        if not email_v:
            print(f"Skip: no valid Email on card '{c.get('name','(no title)')}'.")
            continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            print(f"Skip: already marked '{SENT_MARKER_TEXT}' — {c.get('name','(no title)')}")
            sent_cache.add(card_id)
            continue

        use_b = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B if use_b else BODY_A

        subject = fill_template(subj_tpl, company=company, first=first, from_name=FROM_NAME)
        body    = fill_template(body_tpl, company=company, first=first, from_name=FROM_NAME)

        try:
            send_email(email_v, subject, body)
            processed += 1
            print(f"Sent to {email_v} — card '{c.get('name','(no title)')}' (type {'B' if use_b else 'A'})")
        except Exception as e:
            print(f"Send failed for '{c.get('name','(no title)')}' to {email_v}: {e}")
            continue

        try:
            mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        except Exception:
            pass
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(1.0)

    print(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
