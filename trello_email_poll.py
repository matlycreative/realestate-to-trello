# trello_email_day0.py
import os, re, smtplib, ssl, json
from email.mime.text import MIMEText
from email.headerregistry import Address
from datetime import datetime, timezone
import requests

# ============== env / config ==============
TRELLO_KEY   = os.getenv("TRELLO_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
LIST_ID      = os.getenv("TRELLO_LIST_ID_DAY0")           # required
ONLY_CARD_ID = os.getenv("ONLY_CARD_ID", "").strip()      # optional: test a single card
DEBUG        = (os.getenv("DEBUG","").strip().lower() in ("1","true","yes","on"))

SMTP_HOST  = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT  = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER  = os.getenv("SMTP_USER")         # full gmail address or your SMTP username
SMTP_PASS  = os.getenv("SMTP_PASS")         # app password or SMTP password
EMAIL_FROM = os.getenv("EMAIL_FROM") or SMTP_USER

SUBJ_A = os.getenv("EMAIL_DAY0_TYPE_A_SUBJECT", "Quick idea for {company}")
BODY_A = os.getenv("EMAIL_DAY0_TYPE_A_BODY", "Hi,\nWe help {company} …")
SUBJ_B = os.getenv("EMAIL_DAY0_TYPE_B_SUBJECT", "Hi {first} — quick idea for {company}")
BODY_B = os.getenv("EMAIL_DAY0_TYPE_B_BODY", "Hi {first},\nWe help {company} …")

DAILY_EMAIL_LIMIT = int(os.getenv("DAILY_EMAIL_LIMIT", "10"))

# ============== Trello helpers ==============
API = "https://api.trello.com/1"
def trello(path, method="GET", **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    r = requests.request(method, f"{API}{path}", params=params, timeout=30)
    r.raise_for_status()
    if r.text and r.headers.get("content-type","").startswith("application/json"):
        return r.json()
    return {}

def list_cards(list_id):
    return trello(f"/lists/{list_id}/cards", fields="id,name,desc,idList")

def get_desc(card_id):
    js = trello(f"/cards/{card_id}", fields="desc")
    return (js.get("desc") or "").replace("\r\n","\n").replace("\r","\n")

def add_comment(card_id, text):
    trello(f"/cards/{card_id}/actions/comments", method="POST", text=text)

def get_actions_comments(card_id):
    return trello(f"/cards/{card_id}/actions", filter="commentCard", limit=100) or []

def already_sent_day0(card_id):
    marker = "AUTO: day0 sent"
    acts = get_actions_comments(card_id)
    return any(marker in (a.get("data",{}).get("text","")) for a in acts)

def set_card_name(card_id, new_name):
    trello(f"/cards/{card_id}", method="PUT", name=new_name)

def get_list_name(list_id):
    try:
        js = trello(f"/lists/{list_id}", fields="name")
        return js.get("name","")
    except Exception:
        return ""

# ============== parsing ==============
TARGETS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in TARGETS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

def parse_desc_fields(desc: str):
    desc = (desc or "").replace("\r\n","\n").replace("\r","\n")
    lines = desc.splitlines()
    out = {k:"" for k in TARGETS}
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
                    if nxt.strip() and not any(r.match(nxt) for r in LABEL_RE.values()):
                        val = nxt.strip(); i += 1
                if not out[lab]:
                    out[lab] = val
                break
        i += 1

    # Fallback: if Email: missing, try to find first email anywhere in the desc
    if not out["Email"]:
        m = EMAIL_RE.search(desc)
        if m:
            out["Email"] = m.group(0)

    return out

def render_template(tpl: str, fields: dict) -> str:
    # normalize {{var}} to {var}
    t = re.sub(r"\{\{\s*(\w+)\s*\}\}", r"{\1}", tpl)
    dd = {}
    for k, v in fields.items():
        v = v or ""
        dd[k] = v
        dd[k.lower()] = v
        dd[k.upper()] = v
        dd[k.capitalize()] = v
    class DefaultDict(dict):
        def __missing__(self, key): return ""
    return t.format_map(DefaultDict(dd))

# ============== mail ==============
def mask_email(addr: str) -> str:
    try:
        local, dom = addr.split("@",1)
        if len(local) <= 2:
            local_mask = local[0] + "*"
        else:
            local_mask = local[0] + "*"*(len(local)-2) + local[-1]
        return f"{local_mask}@{dom}"
    except Exception:
        return addr

def send_mail(to_addr: str, subject: str, body: str):
    msg = MIMEText(body, _subtype="plain", _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = str(Address("", *EMAIL_FROM.split("@")))
    msg["To"] = to_addr

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

# ============== main ==============
def main():
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID_DAY0","SMTP_USER","SMTP_PASS"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    lname = get_list_name(LIST_ID)
    if DEBUG:
        print(f"[DEBUG] Polling list: {lname or '(unknown name)'} ({LIST_ID})")

    cards = list_cards(LIST_ID)
    if ONLY_CARD_ID:
        cards = [c for c in cards if c.get("id") == ONLY_CARD_ID]
        if DEBUG:
            print(f"[DEBUG] ONLY_CARD_ID set -> {len(cards)} card(s) to check)")

    sent = 0
    for c in cards:
        if sent >= DAILY_EMAIL_LIMIT:
            if DEBUG: print("[DEBUG] Hit DAILY_EMAIL_LIMIT")
            break

        cid = c["id"]
        cname = c.get("name","(no title)")
        desc = (c.get("desc") or "") or get_desc(cid)
        fields = parse_desc_fields(desc)

        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First") or "").strip()
        to_email= (fields.get("Email") or "").strip()
        hook    = (fields.get("Hook") or "").strip()
        variant = (fields.get("Variant") or "").strip()
        website = (fields.get("Website") or "").strip()

        if DEBUG:
            print(f"[DEBUG] Card: {cname} ({cid})")
            print(f"        Parsed -> Email={mask_email(to_email) if to_email else '(none)'} | Company='{company}' | First='{first}'")

        if already_sent_day0(cid):
            if DEBUG: print("        Skip: marker comment found (already sent)")
            continue

        if not to_email or "@" not in to_email:
            print(f"Skip: no valid Email in description — card '{cname}'")
            continue

        data = {"company": company, "first": first, "hook": hook, "variant": variant, "website": website}
        if first:
            subj = render_template(SUBJ_B, data)
            body = render_template(BODY_B, data)
        else:
            subj = render_template(SUBJ_A, data)
            body = render_template(BODY_A, data)

        try:
            send_mail(to_email, subj, body)
        except Exception as e:
            print(f"Send failed for '{cname}' to {mask_email(to_email)}: {e}")
            continue

        ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
        try:
            add_comment(cid, f"AUTO: day0 sent {ts} to {to_email}")
        except Exception:
            pass

        if company and cname != company:
            try:
                set_card_name(cid, company)
            except Exception:
                pass

        sent += 1
        print(f"Sent Day0 to {mask_email(to_email)} — {company or '(no company)'}")

    print(f"Done. Sent: {sent}")

if __name__ == "__main__":
    main()
