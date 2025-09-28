import os, time, re, smtplib, ssl
from email.message import EmailMessage
import requests

# ---------- ENV ----------
TRELLO_KEY  = os.getenv("TRELLO_KEY")
TRELLO_TOKEN= os.getenv("TRELLO_TOKEN")
LIST_ID     = os.getenv("TRELLO_LIST_ID_DAY0","").strip()
LABEL_ID    = (os.getenv("TRELLO_EMAILED_LABEL_ID") or "").strip()

SMTP_HOST   = os.getenv("SMTP_HOST","smtp.gmail.com")
SMTP_PORT   = int(os.getenv("SMTP_PORT","587"))            # 587 (TLS) or 465 (SSL)
SMTP_USER   = os.getenv("SMTP_USER","")
SMTP_PASS   = os.getenv("SMTP_PASS","")
FROM_EMAIL  = os.getenv("FROM_EMAIL", SMTP_USER)
FROM_NAME   = os.getenv("FROM_NAME","")
REPLY_TO    = os.getenv("REPLY_TO", FROM_EMAIL)

DAILY_EMAIL_CAP = int(os.getenv("DAILY_EMAIL_CAP","10"))
SEND_INTERVAL_S = float(os.getenv("SEND_INTERVAL_S","1"))

SUBJECT_A = os.getenv("SUBJECT_A", "Quick question about {company}")
BODY_A    = os.getenv("BODY_A",
"""Hi there,

I found {company} and had a quick idea that could help you attract more property leads.
Are you the right person for this? Happy to keep it short.

Best,
{sender}
""".strip())

SUBJECT_B = os.getenv("SUBJECT_B", "Hi {first} — quick question")
BODY_B    = os.getenv("BODY_B",
"""Hi {first},

Loved what {company} is doing. I had a short idea to help you bring in more listings and buyer enquiries.
Open to a quick chat this week?

Best,
{sender}
""".strip())

MARKER_PREFIX = "✅ [AUTOMATION] Day0 sent "  # comment marker to avoid duplicates

SESSION = requests.Session()
SESSION.params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})

# ---------- HELPERS ----------
LABELS = ["Company","First","Email"]
LABEL_RE = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in LABELS}

def parse_fields(desc: str):
    """
    Parse Company / First / Email from the card description.
    Supports the value on the same line or the next line.
    """
    desc = (desc or "").replace("\r\n","\n").replace("\r","\n")
    lines = desc.splitlines()
    vals = {"Company":"", "First":"", "Email":""}

    i = 0
    while i < len(lines):
        line = lines[i]
        matched = False
        for lab, rx in LABEL_RE.items():
            m = rx.match(line)
            if m:
                matched = True
                val = (m.group(1) or "").strip()
                # value may be on next visual line
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(rx2.match(nxt) for rx2 in LABEL_RE.values()):
                        val = nxt.strip()
                        i += 1
                vals[lab] = val
                break
        i += 1
    return vals

def list_cards(list_id):
    url = f"https://api.trello.com/1/lists/{list_id}/cards"
    r = SESSION.get(url, params={"fields":"id,name,desc,dateLastActivity,idLabels"})
    r.raise_for_status()
    return r.json()

def card_comments(card_id):
    url = f"https://api.trello.com/1/cards/{card_id}/actions"
    r = SESSION.get(url, params={"filter":"commentCard","limit":100})
    r.raise_for_status()
    return r.json()

def add_comment(card_id, text):
    url = f"https://api.trello.com/1/cards/{card_id}/actions/comments"
    r = SESSION.post(url, params={"text": text})
    r.raise_for_status()

def add_label(card_id, label_id):
    if not label_id: return
    url = f"https://api.trello.com/1/cards/{card_id}/idLabels"
    r = SESSION.post(url, params={"value": label_id})
    # ignore 409 conflicts if label already present
    if r.status_code not in (200, 201):
        try: r.raise_for_status()
        except: pass

def already_sent(card_id):
    try:
        acts = card_comments(card_id)
    except Exception:
        # if Trello hiccups, assume not sent (safe cap will protect)
        return False
    for a in acts:
        if (a.get("type") == "commentCard") and isinstance(a.get("data"), dict):
            text = (a.get("data",{}).get("text") or "").strip()
            if text.startswith(MARKER_PREFIX):
                return True
    return False

def build_email(to_addr, company, first):
    first_clean = (first or "").strip()
    # Decide template A (Company only) or B (Company + First)
    if first_clean:
        subject = SUBJECT_B.format(first=first_clean, company=company, sender=FROM_NAME or FROM_EMAIL)
        body    = BODY_B.format(first=first_clean, company=company, sender=FROM_NAME or FROM_EMAIL)
    else:
        subject = SUBJECT_A.format(company=company, sender=FROM_NAME or FROM_EMAIL)
        body    = BODY_A.format(company=company, sender=FROM_NAME or FROM_EMAIL)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>" if FROM_NAME else FROM_EMAIL
    msg["To"] = to_addr
    msg["Subject"] = subject
    if REPLY_TO: msg["Reply-To"] = REPLY_TO
    msg.set_content(body)
    return msg

def send_email(msg):
    # Try STARTTLS first (587), then SSL as fallback (465)
    port = SMTP_PORT
    if port == 465:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, port, context=context, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        return
    # STARTTLS path
    with smtplib.SMTP(SMTP_HOST, port, timeout=30) as server:
        server.ehlo()
        server.starttls(context=ssl.create_default_context())
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ---------- MAIN ----------
def main():
    # sanity checks
    need = [("TRELLO_KEY", TRELLO_KEY), ("TRELLO_TOKEN", TRELLO_TOKEN), ("TRELLO_LIST_ID_DAY0", LIST_ID),
            ("SMTP_USER", SMTP_USER), ("SMTP_PASS", SMTP_PASS)]
    missing = [k for k,v in need if not v]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    cards = list_cards(LIST_ID)
    print(f"Cards in trigger list: {len(cards)}")
    sent = 0

    for c in cards:
        if sent >= DAILY_EMAIL_CAP:
            print("Daily cap reached — exiting.")
            break

        cid = c.get("id"); name = c.get("name","")
        if already_sent(cid):
            print(f"Skip (already sent) — {name}")
            continue

        fields = parse_fields(c.get("desc") or "")
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First") or "").strip()
        email   = (fields.get("Email") or "").strip()

        # very basic email sanity
        if not email or "@" not in email:
            print(f"Skip (no valid Email) — card {cid}")
            continue
        if not company:
            print(f"Skip (no Company) — card {cid}")
            continue

        try:
            msg = build_email(email, company, first)
            send_email(msg)
        except Exception as e:
            print(f"ERROR sending to {email}: {e}")
            continue

        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        marker = f"{MARKER_PREFIX}{timestamp} to {email}"
        try:
            add_comment(cid, marker)
            add_label(cid, LABEL_ID)
        except Exception as e:
            print(f"Warning: sent ok but failed to annotate Trello: {e}")

        sent += 1
        print(f"Sent to {email} — card {cid}")
        time.sleep(SEND_INTERVAL_S)

    print(f"Done. Sent {sent} emails.")

if __name__ == "__main__":
    main()
