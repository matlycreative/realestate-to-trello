# trello_email_poll.py
import os, re, smtplib, ssl, time, json
from email.message import EmailMessage
from email.utils import parseaddr, formataddr
import requests

TRELLO_KEY   = os.getenv("TRELLO_KEY", "")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN", "")
LIST_ID      = os.getenv("TRELLO_LIST_ID_DAY0", "")
SMTP_USER    = os.getenv("SMTP_USER", "")
SMTP_PASS    = os.getenv("SMTP_PASS", "")
EMAIL_FROM   = os.getenv("EMAIL_FROM", SMTP_USER)
DAILY_LIMIT  = int(os.getenv("DAILY_EMAIL_LIMIT", "10"))
DEBUG        = os.getenv("DEBUG","0") in ("1","true","yes","on")

SESS = requests.Session()
SESS.params = {"key": TRELLO_KEY, "token": TRELLO_TOKEN}

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in TARGET_LABELS}

MARKER_COMMENT = "DAY0_SENT"  # used to avoid double-sends from this list

def log(*a): 
    if DEBUG: print(*a)

def parse_card_header(desc: str) -> dict:
    """Extract Company/First/Email/... from the Trello description block."""
    out = {k:"" for k in TARGET_LABELS}
    d = (desc or "").replace("\r\n","\n").replace("\r","\n")
    lines = d.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        for lab, rx in LABEL_RE.items():
            m = rx.match(line or "")
            if m:
                val = (m.group(1) or "").strip()
                # support value on next line if this line is empty
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1].strip()
                    # next line must not itself be a label
                    if nxt and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt
                        i += 1
                out[lab] = val
                break
        i += 1
    return out

def sanitize_email(value: str) -> str:
    """
    Return a bare addr-spec suitable for SMTP envelope.
    - Strips Markdown/angle-brackets/names.
    - Picks the first RFC5322-ish email in the string.
    """
    value = (value or "").strip()
    # If there's a real email somewhere in the string, pull that
    m = EMAIL_RE.search(value)
    if m:
        return m.group(0)
    # Fallback: use parseaddr (handles "Name <email>")
    name, addr = parseaddr(value)
    addr = (addr or "").strip()
    # Some cards might have only a name with 'mailto:' — try to strip that
    if addr.lower().startswith("mailto:"):
        addr = addr[7:].strip()
    # Final validation
    return addr if EMAIL_RE.fullmatch(addr or "") else ""

def already_sent(card_id: str) -> bool:
    """Check if we've already posted our marker comment on this card."""
    r = SESS.get(f"https://api.trello.com/1/cards/{card_id}/actions", params={"filter":"commentCard"})
    r.raise_for_status()
    for a in r.json():
        data = a.get("data", {})
        txt = (data.get("text") or "").strip()
        if txt == MARKER_COMMENT:
            return True
    return False

def mark_sent(card_id: str):
    SESS.post(f"https://api.trello.com/1/cards/{card_id}/actions/comments",
              params={"text": MARKER_COMMENT}).raise_for_status()

def build_message(to_addr: str, company: str, first: str) -> EmailMessage:
    msg = EmailMessage()
    # Use plain address for From/To headers to avoid utf-8/quoting issues
    msg["From"] = EMAIL_FROM
    msg["To"]   = to_addr

    # Choose template: if First present => Type B; else Type A
    if first:
        subject = f"{company} × quick idea"
        body = f"""Hi {first},

We help {company} get more qualified seller leads without buying portal ads.

If I show you 2–3 prospects tailored to your area, would that be useful?

Best,
— Your Name
"""
    else:
        subject = f"Quick idea for {company}"
        body = f"""Hi there,

We help {company} get more qualified seller leads without buying portal ads.

If I show you 2–3 prospects tailored to your area, would that be useful?

Best,
— Your Name
"""

    msg["Subject"] = subject
    msg.set_content(body)
    return msg

def send_email(msg: EmailMessage, to_addr: str):
    # Gmail / most providers: STARTTLS on 587
    host, port = "smtp.gmail.com", 587
    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=60) as s:
        s.ehlo()
        s.starttls(context=ctx)
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        # IMPORTANT: envelope addresses must be bare addr-specs
        res = s.send_message(msg, from_addr=EMAIL_FROM, to_addrs=[to_addr])
        # send_message returns a dict of failures; empty dict means success
        if res:
            raise RuntimeError(f"Partial failure: {res}")

def main():
    # Preflight
    need = [("TRELLO_KEY", TRELLO_KEY), ("TRELLO_TOKEN", TRELLO_TOKEN),
            ("TRELLO_LIST_ID_DAY0", LIST_ID), ("SMTP_USER", SMTP_USER),
            ("SMTP_PASS", SMTP_PASS), ("EMAIL_FROM", EMAIL_FROM)]
    miss = [k for k,v in need if not v]
    if miss:
        raise SystemExit("Missing env: " + ", ".join(miss))

    # Get cards in the list
    r = SESS.get(f"https://api.trello.com/1/lists/{LIST_ID}/cards",
                 params={"fields":"id,name,desc"})
    r.raise_for_status()
    cards = r.json()

    sent_count = 0
    for c in cards:
        if sent_count >= DAILY_LIMIT:
            log("Daily limit reached.")
            break

        cid = c["id"]
        if already_sent(cid):
            log(f"Skip {c['name']} — already sent from this list.")
            continue

        fields = parse_card_header(c.get("desc") or "")
        raw_email = fields.get("Email","")
        to_addr = sanitize_email(raw_email)

        if not to_addr:
            log(f"Skip {c['name']} — invalid/empty Email field: {raw_email!r}")
            continue

        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First") or "").strip()

        try:
            msg = build_message(to_addr, company, first)
            print(f"Sending to {to_addr} (card: {c['name']}) …")
            send_email(msg, to_addr)
            mark_sent(cid)
            sent_count += 1
            print(f"Sent OK → {to_addr}")
            # be polite, avoid hammering SMTP
            time.sleep(1.0)
        except smtplib.SMTPRecipientsRefused as e:
            # Show exactly what the server refused
            print(f"Send failed for {c['name']} to {to_addr}: {e.recipients}")
        except smtplib.SMTPException as e:
            print(f"SMTP error for {c['name']} to {to_addr}: {e}")
        except Exception as e:
            print(f"Unexpected error for {c['name']} to {to_addr}: {e}")

    print(f"Done. Emails sent: {sent_count}")

if __name__ == "__main__":
    main()
