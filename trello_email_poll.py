import os, re, time, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import requests

# ---------- Config from env ----------
TRELLO_KEY   = os.getenv("TRELLO_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
LIST_ID      = os.getenv("TRELLO_LIST_ID") or os.getenv("TRELLO_LIST_ID_DAY0")  # fallback name
SENT_MARKER  = os.getenv("SENT_MARKER", "day0")  # unique per list (e.g., day0, day3)
MAX_PER_RUN  = int(os.getenv("OUTREACH_MAX_PER_RUN", "5"))

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL") or SMTP_USER
FROM_NAME  = os.getenv("FROM_NAME", "")

SUBJECT_A = os.getenv("SUBJECT_A", "Quick idea for {company}")
BODY_A    = os.getenv("BODY_A",    "Hi there,\n\nIdea for {company}…\n\nBest,\n{from_name}")
SUBJECT_B = os.getenv("SUBJECT_B", "Quick idea for {company}, {first}")
BODY_B    = os.getenv("BODY_B",    "Hi {first},\n\nIdea for {company}…\n\nBest,\n{from_name}")

# ---------- Trello helpers ----------
API = "https://api.trello.com/1"
AUTH = {"key": TRELLO_KEY, "token": TRELLO_TOKEN}
SESSION = requests.Session()
SESSION.params.update(AUTH)

TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in TARGET_LABELS}

def require_env():
    miss = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID","SMTP_USER","SMTP_PASS"] if not os.getenv(n)]
    # Accept legacy name TRELLO_LIST_ID_DAY0
    if not os.getenv("TRELLO_LIST_ID") and not os.getenv("TRELLO_LIST_ID_DAY0"):
        miss.append("TRELLO_LIST_ID (or TRELLO_LIST_ID_DAY0)")
    if miss:
        raise SystemExit("Missing env: " + ", ".join(miss))

def get_cards_in_list(list_id):
    r = SESSION.get(f"{API}/lists/{list_id}/cards", params={"fields":"id,name,desc,url"})
    r.raise_for_status()
    return r.json()

def parse_fields(desc: str):
    """Return dict with Company/First/Email/Website (strings)."""
    d = (desc or "").replace("\r\n","\n").replace("\r","\n")
    out = {k:"" for k in TARGET_LABELS}
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        matched = False
        for lab, rx in LABEL_RE.items():
            m = rx.match(line)
            if m:
                matched = True
                val = (m.group(1) or "").strip()
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip(); i += 1
                out[lab] = val
                break
        i += 1 if matched else 1
    # Normalize
    out["Company"] = (out["Company"] or "").strip()
    out["First"]   = (out["First"] or "").strip()
    out["Email"]   = (out["Email"] or "").strip()
    out["Website"] = (out["Website"] or "").strip()
    return out

def get_card_comments(card_id):
    r = SESSION.get(f"{API}/cards/{card_id}/actions",
                    params={"filter":"commentCard","limit":100})
    r.raise_for_status()
    return r.json()

def add_comment(card_id, text):
    r = SESSION.post(f"{API}/cards/{card_id}/actions/comments",
                     params={"text": text})
    r.raise_for_status()

def already_sent_from_this_list(card_id, marker: str) -> bool:
    """Check comment stream for our unique token, e.g., [sent:day0]."""
    token = f"[sent:{marker}]"
    try:
        for a in get_card_comments(card_id):
            if (a.get("type") == "commentCard") and token in (a.get("data",{}).get("text") or ""):
                return True
    except Exception:
        # On API failure, be safe and assume sent to avoid duplicates.
        return True
    return False

# ---------- Email ----------
def build_message(to_email, subject, body_text, body_html=None):
    msg = MIMEMultipart("alternative")
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>" if FROM_NAME else FROM_EMAIL
    msg["To"]   = to_email
    msg["Subject"] = subject

    # Plain text is mandatory, HTML optional
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    return msg

def send_email(to_email, subject, body_text, body_html=None):
    msg = build_message(to_email, subject, body_text, body_html)
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, [to_email], msg.as_string())

# ---------- Fill templates ----------
def fill_templates(fields):
    company = fields.get("Company","").strip()
    first   = fields.get("First","").strip()
    website = fields.get("Website","").strip()

    if first:
        subject = SUBJECT_B.format(company=company, first=first, website=website, from_name=FROM_NAME)
        body    = BODY_B.format(company=company, first=first, website=website, from_name=FROM_NAME)
    else:
        subject = SUBJECT_A.format(company=company, first=first, website=website, from_name=FROM_NAME)
        body    = BODY_A.format(company=company, first=first, website=website, from_name=FROM_NAME)

    # Provide a very light HTML version (optional)
    body_html = f"""<div>{body.replace(chr(10), "<br>")}</div>"""
    return subject, body, body_html

# ---------- Main ----------
def main():
    require_env()
    sent_this_run = 0
    cards = get_cards_in_list(LIST_ID)

    for c in cards:
        if sent_this_run >= MAX_PER_RUN:
            break

        card_id = c["id"]
        desc = c.get("desc") or ""
        fields = parse_fields(desc)

        email = fields.get("Email","").strip()
        company = fields.get("Company","").strip()

        # Must have an email and a company
        if not email or "@" not in email or not company:
            continue

        # Only send once per list: skip if we already commented with [sent:MARKER]
        if already_sent_from_this_list(card_id, SENT_MARKER):
            continue

        # Build and send
        subject, body_text, body_html = fill_templates(fields)
        try:
            send_email(email, subject, body_text, body_html)
        except Exception as e:
            # Comment the failure so we can diagnose, but don’t mark as sent
            try:
                add_comment(card_id, f"Email ERROR ({SENT_MARKER}) to {email}: {e}")
            except Exception:
                pass
            continue

        # Mark this card as emailed for THIS list only
        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        marker = f"[sent:{SENT_MARKER}]"
        try:
            add_comment(card_id, f"Email sent ({SENT_MARKER}) to {email} at {ts} {marker}")
        except Exception:
            # If commenting fails, still avoid spamming by sleeping a bit
            time.sleep(1)

        sent_this_run += 1
        # Gentle pacing to be nice to SMTP/Trello
        time.sleep(1.0)

    print(f"Done. Emails sent this run: {sent_this_run}")

if __name__ == "__main__":
    main()
