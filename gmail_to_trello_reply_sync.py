#!/usr/bin/env python3
"""
Gmail → Trello reply sync (append once)

- Polls Gmail IMAP for UNSEEN messages (INBOX).
- For each email, finds Trello card(s) whose description has "Email: <sender>".
- Moves matching card(s) to a target list and APPENDS the email (Subject/Body) to the description.
- Ensures it only happens ONCE per card by looking for a prior sync comment.

Requirements (env):
  IMAP_USER, IMAP_PASS                     # Gmail address + App Password (IMAP enabled)
  IMAP_HOST (default: imap.gmail.com)
  IMAP_PORT (default: 993)

  TRELLO_KEY, TRELLO_TOKEN
  TRELLO_BOARD_ID                          # Board to scan (all lists)
  TRELLO_DEST_LIST_ID                      # List to move the card into (when a match is found)

Optional:
  MAX_EMAILS_PER_RUN (default: 20)
  BODY_MAX_CHARS (default: 4000)
  ONCE_MARKER_PREFIX (default: "Synced reply from")
  ONCE_MARKER_TAG    (default: "[SYNCED_ONCE]")
"""

import os, re, time, json, html, email, imaplib
from email.header import decode_header, make_header
from datetime import datetime
import requests

def log(*a): print(*a, flush=True)

# ---------- Env ----------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

IMAP_USER = _get_env("IMAP_USER")
IMAP_PASS = _get_env("IMAP_PASS")
IMAP_HOST = _get_env("IMAP_HOST", default="imap.gmail.com")
IMAP_PORT = int(_get_env("IMAP_PORT", default="993"))

TRELLO_KEY        = _get_env("TRELLO_KEY")
TRELLO_TOKEN      = _get_env("TRELLO_TOKEN")
TRELLO_BOARD_ID   = _get_env("TRELLO_BOARD_ID")
DEST_LIST_ID      = _get_env("TRELLO_DEST_LIST_ID")

MAX_EMAILS_PER_RUN = int(_get_env("MAX_EMAILS_PER_RUN", default="20"))
BODY_MAX_CHARS     = int(_get_env("BODY_MAX_CHARS", default="4000"))

ONCE_MARKER_PREFIX = _get_env("ONCE_MARKER_PREFIX", default="Synced reply from")
ONCE_MARKER_TAG    = _get_env("ONCE_MARKER_TAG",    default="[SYNCED_ONCE]")

# ---------- Trello helpers ----------
SESS = requests.Session()
def trello_call(method, path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    url = f"https://api.trello.com/1/{path.lstrip('/')}"
    for attempt in range(3):
        try:
            if method == "GET":
                r = SESS.get(url, params=params, timeout=30)
            elif method == "POST":
                r = SESS.post(url, params=params, timeout=30)
            elif method == "PUT":
                r = SESS.put(url, params=params, timeout=30)
            else:
                raise ValueError("method must be GET/POST/PUT")
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: raise
            time.sleep(1.2 * (attempt + 1))

def trello_get(path, **params):  return trello_call("GET", path, **params)
def trello_put(path, **params):  return trello_call("PUT", path, **params)
def trello_post(path, **params): return trello_call("POST", path, **params)

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
    m = EMAIL_RE.search(raw)
    return m.group(0).strip().lower() if m else ""

# ---------- Gmail helpers ----------
def decode_mime_words(s: str | None) -> str:
    if not s: return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s

def extract_plain_text(msg: email.message.Message) -> str:
    # prefer text/plain; fallback to stripped html
    parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type() or ""
            disp  = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            except Exception:
                continue
            if ctype.startswith("text/plain"):
                parts.append(text)
            elif not parts and ctype.startswith("text/html"):
                stripped = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", text)
                stripped = re.sub(r"(?is)<br\s*/?>", "\n", stripped)
                stripped = re.sub(r"(?is)</p\s*>", "\n\n", stripped)
                stripped = re.sub(r"(?is)<.*?>", "", stripped)
                parts.append(html.unescape(stripped))
    else:
        payload = msg.get_payload(decode=True) or b""
        parts.append(payload.decode(msg.get_content_charset() or "utf-8", errors="replace"))
    body = "\n".join(p.strip() for p in parts if p is not None).strip()
    if len(body) > BODY_MAX_CHARS:
        body = body[:BODY_MAX_CHARS].rstrip() + "\n…"
    return body

# ---------- Append helper ----------
def append_block(current_desc: str, block: str) -> str:
    cur = (current_desc or "").rstrip()
    if not cur:
        return block
    # nice visual separator; Trello supports Markdown
    return f"{cur}\n\n---\n\n{block}"

# ---------- Once-per-card helper ----------
def card_already_synced(card_id: str) -> bool:
    """Return True if the card already has our sync marker comment."""
    try:
        acts = trello_get(f"cards/{card_id}/actions", filter="commentCard", limit=50)
    except Exception:
        return False
    pfx = (ONCE_MARKER_PREFIX or "").strip().lower()
    tag = (ONCE_MARKER_TAG or "").strip().lower()
    for a in acts or []:
        txt = (a.get("data", {}).get("text") or a.get("text") or "").strip()
        tl = txt.lower()
        if (tag and tag in tl) or (pfx and tl.startswith(pfx)):
            return True
    return False

# ---------- Core ----------
def main():
    # sanity
    missing = []
    if not IMAP_USER:       missing.append("IMAP_USER")
    if not IMAP_PASS:       missing.append("IMAP_PASS")
    if not TRELLO_KEY:      missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN:    missing.append("TRELLO_TOKEN")
    if not TRELLO_BOARD_ID: missing.append("TRELLO_BOARD_ID")
    if not DEST_LIST_ID:    missing.append("TRELLO_DEST_LIST_ID")
    if missing:
        raise SystemExit("Missing env: " + ", ".join(missing))

    # 1) Load every card on the board (id, name, desc, idList)
    log("[trello] fetching board cards…")
    cards = trello_get(f"boards/{TRELLO_BOARD_ID}/cards", fields="id,name,desc,idList", limit=1000)
    email_to_cards = {}
    for c in cards or []:
        desc = c.get("desc") or ""
        fields = parse_header(desc)
        em = clean_email(fields.get("Email", ""))
        if em:
            email_to_cards.setdefault(em, []).append(c)

    # 2) Connect IMAP and search UNSEEN
    log("[imap] connecting…")
    M = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    M.login(IMAP_USER, IMAP_PASS)
    M.select("INBOX")
    typ, data = M.search(None, 'UNSEEN')
    if typ != "OK":
        log("[imap] search failed")
        M.logout()
        return
    ids = (data[0] or b"").split()
    ids = ids[-MAX_EMAILS_PER_RUN:] if MAX_EMAILS_PER_RUN else ids
    log(f"[imap] unseen emails: {len(ids)}")

    processed = 0
    for eid in ids:
        typ, msgdata = M.fetch(eid, '(RFC822)')
        if typ != "OK" or not msgdata:
            continue
        raw = msgdata[0][1]
        msg = email.message_from_bytes(raw)

        from_hdr = decode_mime_words(msg.get("From", ""))
        subj_hdr = decode_mime_words(msg.get("Subject", ""))
        m = EMAIL_RE.search(from_hdr)
        sender = m.group(0).lower() if m else ""
        body = extract_plain_text(msg)
        when = decode_mime_words(msg.get("Date", "")) or datetime.utcnow().isoformat(timespec="seconds")+"Z"

        log(f"[imap] from={sender} subject={subj_hdr!r}")

        if not sender or sender not in email_to_cards:
            M.store(eid, '+FLAGS', '\\Seen')
            continue

        for c in email_to_cards[sender]:
            cid    = c["id"]
            old    = c.get("desc") or ""
            title  = c.get("name") or "(no title)"

            # Skip if we've already synced once for this card
            if card_already_synced(cid):
                log(f"[trello] skip (already synced once): {title}")
                continue

            block = f"Subject :\n{subj_hdr}\n\nBody :\n{body}\n\n"
            new_desc = append_block(old, block)

            # 3) Move + update desc + leave a marker comment
            try:
                trello_put(f"cards/{cid}", idList=DEST_LIST_ID, desc=new_desc)
                trello_post(
                    f"cards/{cid}/actions/comments",
                    text=f"{ONCE_MARKER_TAG} {ONCE_MARKER_PREFIX} {sender} — {when}"
                )
                log(f"[trello] updated + moved: {title}")
            except Exception as e:
                log(f"[trello] update failed for {title}: {e}")

        M.store(eid, '+FLAGS', '\\Seen')
        processed += 1
        time.sleep(0.6)

    M.close(); M.logout()
    log(f"Done. Emails processed: {processed}")

if __name__ == "__main__":
    main()
