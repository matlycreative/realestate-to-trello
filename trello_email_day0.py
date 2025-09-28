#!/usr/bin/env python3
"""
Send "Day 0" cold email when a Trello card sits in a specific list.

Uses 2 template types:
 - Type A: only {{company}} available (no first name)
 - Type B: {{company}} and {{first}} available

Templates (put these in GitHub Secrets or Variables):
  SUBJECT_A, BODY_A, SUBJECT_B, BODY_B

Other required secrets:
  TRELLO_KEY, TRELLO_TOKEN, TRELLO_LIST_ID_DAY0
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
  FROM_NAME, FROM_EMAIL

Optional:
  REPLY_TO, DRY_RUN (1/0), SENT_MARKER (default: emailed_day0)
"""

import os, re, smtplib, ssl, sys, time
from email.message import EmailMessage
from typing import Dict, Tuple, Optional
import requests

# ------------- helpers -------------
def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default if default is not None else ""
    return str(v)

def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except Exception:
        return default

def die(msg: str):
    print(msg)
    sys.exit(1)

TOKEN_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")

def render_template(tpl: str, ctx: Dict[str, str]) -> str:
    def repl(m):
        key = m.group(1)
        return str(ctx.get(key, ""))
    return TOKEN_RE.sub(repl, tpl or "")

def find_field(desc: str, name: str) -> str:
    """
    Looks for lines like 'Name: value'. If the value is blank,
    we also look at the following line (if it isn't another label line).
    """
    if not desc:
        return ""
    label_re = re.compile(rf"(?mi)^\s*{re.escape(name)}\s*:\s*(.*)$")
    lines = desc.replace("\r\n","\n").replace("\r","\n").splitlines()
    for i, line in enumerate(lines):
        m = label_re.match(line)
        if m:
            val = (m.group(1) or "").strip()
            if not val and (i+1) < len(lines):
                nxt = lines[i+1].strip()
                if not re.match(r"(?mi)^\s*[A-Za-z ]+\s*:\s*", nxt):
                    val = nxt
            return val.strip()
    return ""

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

def parse_recipients(raw: str):
    if not raw:
        return []
    # accept comma/semicolon separated or embedded in text
    parts = re.findall(EMAIL_RE, raw)
    # also include comma-splits if user typed "a@x.com, b@y.com"
    for sep in [",", ";"]:
        for chunk in raw.split(sep):
            chunk = chunk.strip()
            if EMAIL_RE.fullmatch(chunk) and chunk not in parts:
                parts.append(chunk)
    # de-dupe preserve order
    seen, out = set(), []
    for e in parts:
        el = e.lower()
        if el not in seen:
            out.append(e)
            seen.add(el)
    return out

# ------------- config -------------
TRELLO_KEY  = env("TRELLO_KEY")
TRELLO_TOKEN = env("TRELLO_TOKEN")
LIST_ID = env("TRELLO_LIST_ID_DAY0")

SMTP_HOST = env("SMTP_HOST")
SMTP_PORT = env_int("SMTP_PORT", 587)
SMTP_USER = env("SMTP_USER")
SMTP_PASS = env("SMTP_PASS")
FROM_NAME = env("FROM_NAME")
FROM_EMAIL = env("FROM_EMAIL")
REPLY_TO  = env("REPLY_TO", "")

# templates from secrets/variables (with safe defaults)
SUBJECT_A = env("SUBJECT_A", "{{company}} — quick question")
BODY_A    = env("BODY_A", "Hi there,\n\nI wanted to ask a quick question about {{company}}.\n\nBest,\n{{from_name}}")
SUBJECT_B = env("SUBJECT_B", "Hi {{first}} — quick question about {{company}}")
BODY_B    = env("BODY_B", "Hi {{first}},\n\nQuick question about {{company}}.\n\nBest,\n{{from_name}}")

SENT_MARKER = env("SENT_MARKER", "emailed_day0")
DRY_RUN = env("DRY_RUN", "0").strip() in ("1","true","yes","on")

if not all([TRELLO_KEY, TRELLO_TOKEN, LIST_ID]):
    miss = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID_DAY0"] if not env(n)]
    die(f"Missing env: {', '.join(miss)}")
if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, FROM_EMAIL]):
    miss = [n for n in ["SMTP_HOST","SMTP_USER","SMTP_PASS","FROM_EMAIL"] if not env(n)]
    die(f"Missing SMTP env: {', '.join(miss)}")

UA = f"TrelloEmailer/1.0 (+{FROM_EMAIL})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ------------- Trello API -------------
def trello_get_cards_in_list(list_id: str):
    r = SESS.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc,shortUrl"}
    )
    r.raise_for_status()
    return r.json()

def trello_update_desc(card_id: str, new_desc: str):
    r = SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": new_desc}
    )
    r.raise_for_status()

# ------------- email -------------
def send_email(to_addr: str, subject: str, body: str):
    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>" if FROM_NAME else FROM_EMAIL
    msg["To"] = to_addr
    msg["Subject"] = subject.strip().replace("\r","").replace("\n"," ")
    if REPLY_TO:
        msg["Reply-To"] = REPLY_TO
    msg.set_content(body)

    if DRY_RUN:
        print(f"[DRY_RUN] Would send to={to_addr} subject={msg['Subject']}\n---\n{body}\n---")
        return

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        # Use STARTTLS for 587; if you need 465, switch to SMTP_SSL and set port accordingly.
        server.starttls(context=context)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ------------- main logic -------------
def already_sent(desc: str) -> bool:
    if not desc:
        return False
    return SENT_MARKER.lower() in desc.lower()

def mark_sent(desc: str) -> str:
    stamp = time.strftime("%Y-%m-%d %H:%MZ", time.gmtime())
    prefix = "" if desc.endswith("\n") or not desc else "\n\n"
    return f"{desc}{prefix}---\n{SENT_MARKER} {stamp}\n"

def choose_type(company: str, first: str) -> str:
    return "B" if first else "A"

def main():
    cards = trello_get_cards_in_list(LIST_ID)
    if not cards:
        print("No cards in target list.")
        return

    sent_count = 0
    for c in cards:
        cid = c["id"]
        name = c.get("name","")
        desc = c.get("desc","") or ""
        url  = c.get("shortUrl","")

        if already_sent(desc):
            print(f"Skip card {name}: already marked as sent.")
            continue

        company = find_field(desc, "Company")
        first   = find_field(desc, "First")
        email_raw = find_field(desc, "Email")
        website = find_field(desc, "Website")

        to_list = parse_recipients(email_raw)
        if not to_list:
            print(f"Skip card {name}: no valid recipient email found.")
            continue

        t = choose_type(company, first)
        ctx = {
            "company": company or "",
            "first": first or "",
            "website": website or "",
            "card_name": name or "",
            "card_url": url or "",
            "from_name": FROM_NAME or "",
        }
        if t == "B":
            subject = render_template(SUBJECT_B, ctx)
            body    = render_template(BODY_B, ctx)
        else:
            subject = render_template(SUBJECT_A, ctx)
            body    = render_template(BODY_A, ctx)

        # send to the first address only (typical outreach); change to loop if you want multiples
        to_addr = to_list[0]
        try:
            send_email(to_addr, subject, body)
            sent_count += 1
            new_desc = mark_sent(desc)
            trello_update_desc(cid, new_desc)
            print(f"Sent to {to_addr} from card '{name}' using type {t}.")
        except Exception as e:
            print(f"ERROR sending to {to_addr} for card '{name}': {e}")

    print(f"Done. Emails sent: {sent_count}")

if __name__ == "__main__":
    main()
