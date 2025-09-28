import os, re, time, smtplib, requests
from email.message import EmailMessage
from datetime import datetime, timezone, date
from dateutil.parser import isoparse

# -------- Config from env --------
TRELLO_KEY   = os.getenv("TRELLO_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
LIST_ID      = os.getenv("TRELLO_LIST_ID_DAY0")
LAB_ID       = (os.getenv("TRELLO_EMAILED_LABEL_ID") or "").strip() or None

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL") or SMTP_USER
FROM_NAME  = os.getenv("FROM_NAME", "")
REPLY_TO   = (os.getenv("REPLY_TO") or FROM_EMAIL)

DAILY_CAP  = int(os.getenv("DAILY_EMAIL_CAP", "10"))
PAUSE_S    = int(os.getenv("SEND_INTERVAL_S", "60"))

# Fallback templates if not provided via secrets
SUBJECT_A = os.getenv("SUBJECT_A", "Quick idea for {company}")
BODY_A = os.getenv("BODY_A", """Hi there,

I had a quick idea for {company} after checking your site.
We help similar agencies get more qualified seller leads with minimal busywork.

If you’re open to it, I can share a 2–3 sentence teardown specific to {company}.

Best,
""")

SUBJECT_B = os.getenv("SUBJECT_B", "{first} — quick idea for {company}")
BODY_B = os.getenv("BODY_B", """Hi {first},

Had a quick idea for {company} after checking your site.
We help similar agencies get more qualified seller leads with minimal busywork.

If helpful, I can send a 2–3 sentence teardown tailored to {company}.

Best,
""")

# -------- Helpers --------
BASE = "https://api.trello.com/1"
def TGET(path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    r = requests.get(f"{BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def TPOST(path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    r = requests.post(f"{BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else {}

def add_comment(card_id, text):
    return TPOST(f"/cards/{card_id}/actions/comments", text=text)

def add_label(card_id, label_id):
    # POST /1/cards/{id}/idLabels?value={labelId}
    return TPOST(f"/cards/{card_id}/idLabels", value=label_id)

LABELS = ["Company","First","Email","Website"]
PAT = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in LABELS}

def parse_desc(desc: str):
    """Extract Company / First / Email / Website from Trello card description."""
    out = {k:"" for k in LABELS}
    if not desc: return out
    desc = desc.replace("\r\n","\n").replace("\r","\n")
    lines = desc.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        matched = False
        for lab in LABELS:
            m = PAT[lab].match(line)
            if m:
                val = (m.group(1) or "").strip()
                # value might be on next line visually
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(PAT[L].match(nxt) for L in LABELS):
                        val = nxt.strip(); i += 1
                out[lab] = val
                matched = True
                break
        i += 1
    return out

AUTO_MARK = "[AUTOMATION] Day0 sent"

def card_has_been_sent(card_id):
    """True if card has any 'Day0 sent' comment."""
    acts = TGET(f"/cards/{card_id}/actions", filter="commentCard", limit=100)
    for a in acts:
        txt = (a.get("data",{}).get("text") or "").strip()
        if AUTO_MARK in txt:
            return True
    return False

def count_sent_today_in_list(list_id, cap):
    """Count how many Day0 sends happened today (UTC) across cards in the list."""
    today = date.today()
    cards = TGET(f"/lists/{list_id}/cards", fields="id", limit=300)
    count = 0
    for c in cards:
        acts = TGET(f"/cards/{c['id']}/actions", filter="commentCard", limit=50)
        for a in acts:
            txt = (a.get("data",{}).get("text") or "")
            if AUTO_MARK in txt:
                d = isoparse(a.get("date")).astimezone(timezone.utc).date()
                if d == today:
                    count += 1
                    if count >= cap:
                        return count
    return count

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>" if FROM_NAME else FROM_EMAIL
    msg["To"] = to_email
    if REPLY_TO:
        msg["Reply-To"] = REPLY_TO
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.ehlo()
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def main():
    # safety checks
    for var in [TRELLO_KEY, TRELLO_TOKEN, LIST_ID, SMTP_HOST, SMTP_USER, SMTP_PASS, FROM_EMAIL]:
        if not var:
            raise SystemExit("Missing a required env/secret. Check workflow Preflight step.")

    # 1) How many sent today?
    sent_today = count_sent_today_in_list(LIST_ID, DAILY_CAP)
    print(f"Already sent today: {sent_today}/{DAILY_CAP}")
    if sent_today >= DAILY_CAP:
        print("Daily cap reached — exiting.")
        return

    # 2) Get candidate cards (those in target list)
    cards = TGET(f"/lists/{LIST_ID}/cards", fields="id,name,desc,url", limit=500)
    to_process = []
    for c in cards:
        if not card_has_been_sent(c["id"]):
            to_process.append(c)

    print(f"Unsent cards found: {len(to_process)}")

    # 3) Process up to the remaining quota
    remaining = max(0, DAILY_CAP - sent_today)
    processed = 0

    for c in to_process:
        if processed >= remaining:
            break

        fields = parse_desc(c.get("desc",""))
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First") or "").strip()
        email   = (fields.get("Email") or "").strip()

        if not email or "@" not in email:
            print(f"Skip {c['id']} — no valid Email in description.")
            continue
        if not company:
            print(f"Skip {c['id']} — no Company in description.")
            continue

        # Choose template A (no first) vs B (has first)
        if first:
            subject = SUBJECT_B.format(company=company, first=first)
            body    = BODY_B.format(company=company, first=first)
        else:
            subject = SUBJECT_A.format(company=company)
            body    = BODY_A.format(company=company)

        try:
            send_email(email, subject, body)
            timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            add_comment(c["id"], f"✅ {AUTO_MARK} {timestamp} to {email}")
            if LAB_ID:
                try:
                    add_label(c["id"], LAB_ID)
                except Exception:
                    pass  # label is optional
            print(f"Sent to {email} — card {c['id']}")
            processed += 1
            if processed < remaining:
                time.sleep(PAUSE_S)
        except Exception as e:
            print(f"Send failed for {email} (card {c['id']}): {e}")

    print(f"Done. Sent today now: {sent_today + processed}/{DAILY_CAP}")

if __name__ == "__main__":
    main()
