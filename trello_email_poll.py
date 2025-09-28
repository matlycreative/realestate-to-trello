import os, re, json, time, smtplib, pathlib
from email.message import EmailMessage
from email.utils import formataddr
import requests

# ---------- env & defaults ----------
TRELLO_KEY   = os.getenv("TRELLO_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
LIST_ID      = os.getenv("TRELLO_LIST_ID_DAY0")

SMTP_HOST     = os.getenv("SMTP_HOST", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS  = (os.getenv("SMTP_USE_TLS", "1").strip() in ("1","true","yes","on"))

FROM_NAME  = os.getenv("FROM_NAME", "Outreach")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USERNAME or "noreply@example.com")

# Templates (A = company only; B = company + first)
SUBJECT_A = os.getenv("SUBJECT_A", "Quick idea for {company}")
BODY_A = os.getenv("BODY_A", """Hi there,

We help {company} get more qualified seller leads without paid ads.
If I send 2–3 ideas tailored to {company}, would you take a quick look?

Best,
{from_name}
""")

SUBJECT_B = os.getenv("SUBJECT_B", "Quick idea for {company}")
BODY_B = os.getenv("BODY_B", """Hi {first},

We help {company} get more qualified seller leads without paid ads.
If I send 2–3 ideas tailored to {company}, would you take a quick look?

Best,
{from_name}
""")

SENT_CACHE_FILE = os.getenv("SENT_CACHE_FILE", ".data/sent_day0.jsonl")
SENT_MARKER_TEXT = (os.getenv("SENT_MARKER_TEXT", "day0 email sent")).lower()

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
LABELS = ("Company","First","Email")

# ---------- helpers ----------
def die_if_missing():
    missing = [k for k in ("TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID_DAY0","SMTP_HOST","SMTP_USERNAME","SMTP_PASSWORD") if not os.getenv(k)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

def trello_get_cards_in_list(list_id):
    r = requests.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "name,desc"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def trello_get_comments(card_id):
    r = requests.get(
        f"https://api.trello.com/1/cards/{card_id}/actions",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "filter": "commentCard"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def trello_add_comment(card_id, text):
    requests.post(
        f"https://api.trello.com/1/cards/{card_id}/actions/comments",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "text": text},
        timeout=30,
    ).raise_for_status()

def parse_desc_fields(desc):
    """
    Pull Company / First / Email from the header block.
    Accepts either "Email: user@x.com" or Trello markdown "[user@x.com](mailto:user@x.com)".
    """
    vals = {"Company":"","First":"","Email":""}
    lines = (desc or "").replace("\r\n","\n").replace("\r","\n").splitlines()
    # scan for label lines
    for i, line in enumerate(lines):
        for lab in LABELS:
            if re.match(rf"(?mi)^\s*{re.escape(lab)}\s*:", line):
                val = line.split(":",1)[1].strip()
                # If visual value is on next line, capture it
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1].strip()
                    if nxt and not any(re.match(rf"(?mi)^\s*{re.escape(l)}\s*:", nxt) for l in LABELS):
                        val = nxt
                vals[lab] = val
    # normalize email from plain or markdown
    email_raw = vals.get("Email","")
    m = EMAIL_RE.search(email_raw)
    if not m:
        # try extract from a mailto: if present
        mt = re.search(r"mailto:([^)\s]+)", email_raw, flags=re.I)
        if mt:
            m = EMAIL_RE.search(mt.group(1))
    vals["Email"] = m.group(0) if m else ""
    # cleanup company/first
    vals["Company"] = vals.get("Company","").strip()
    vals["First"] = vals.get("First","").strip()
    return vals

def safe_fill(template: str, mapping: dict) -> str:
    """
    Replace {placeholders} that exist in mapping; unknown placeholders -> empty.
    """
    def repl(m):
        key = m.group(1)
        return str(mapping.get(key, ""))
    return re.sub(r"\{(\w+)\}", repl, template)

def sanitize_subject(s: str) -> str:
    # prevent header injection / line breaks
    return " ".join((s or "").splitlines()).strip()

def already_sent(card_id):
    """Return True if marker present in Trello comments OR in local cache."""
    # 1) check Trello comments
    try:
        for act in trello_get_comments(card_id):
            t = (act.get("data",{}).get("text") or "").lower()
            if SENT_MARKER_TEXT in t:
                return True
    except Exception:
        pass
    # 2) check local cache
    try:
        p = pathlib.Path(SENT_CACHE_FILE)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        js = json.loads(line)
                        if js.get("card_id") == card_id:
                            return True
                    except Exception:
                        continue
    except Exception:
        pass
    return False

def append_cache(card_id, to_email):
    pathlib.Path(SENT_CACHE_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(SENT_CACHE_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": int(time.time()), "card_id": card_id, "to": to_email}) + "\n")

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg["From"] = formataddr((FROM_NAME, FROM_EMAIL))
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_USE_TLS:
            s.starttls()
        s.login(SMTP_USERNAME, SMTP_PASSWORD)
        s.send_message(msg)

# ---------- main ----------
def main():
    die_if_missing()
    cards = trello_get_cards_in_list(LIST_ID)
    sent_count = 0

    for c in cards:
        card_id = c["id"]
        desc = c.get("desc","")

        if already_sent(card_id):
            continue

        vals = parse_desc_fields(desc)
        company = vals.get("Company","")
        first   = vals.get("First","")
        email   = vals.get("Email","")

        if not email:
            print(f"Skip '{c.get('name','(no name)')}': no email found in description.")
            continue
        if not company:
            print(f"Skip '{c.get('name','(no name)')}': no Company found in description.")
            continue

        # choose template: B if First present, else A
        is_b = bool(first)
        if is_b:
            subj_tpl = SUBJECT_B
            body_tpl = BODY_B
        else:
            subj_tpl = SUBJECT_A
            body_tpl = BODY_A

        mapping = {
            "company": company,
            "first": first,
            "from_name": FROM_NAME,
        }
        subject = safe_fill(subj_tpl, mapping)
        body    = safe_fill(body_tpl, mapping)

        try:
            send_email(email, subject, body)
            sent_count += 1
            print(f"Sent to {email}: {subject}")

            trello_add_comment(card_id, f"✅ {SENT_MARKER_TEXT} — to {email} ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})")
            append_cache(card_id, email)

        except Exception as e:
            print(f"Send failed for '{company}' to {email}: {e}")

    print(f"Done. Sent {sent_count} email(s).")

if __name__ == "__main__":
    main()
