#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Day-0 — Poll Trello and send one email per card (PLAIN TEXT ONLY).

Design removed:
- No HTML
- No logo
- No card
- No images
- No styled links
"""

import os, re, time, json, html, unicodedata
from datetime import datetime
import requests

def log(*a): print(*a, flush=True)

# ----------------- utils -----------------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v and v.strip():
            return v.strip()
    return default

def _safe_id_from_email(email: str) -> str:
    return (email or "").lower().replace("@", "_").replace(".", "_")

def _slugify_company(name: str) -> str:
    s = unicodedata.normalize("NFKD", (name or "")).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]+", "", s.lower())
    return re.sub(r"[\s-]+","_",s).strip("_")

def choose_id(company: str, email: str) -> str:
    return _slugify_company(company) or _safe_id_from_email(email)

# ----------------- env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0")

FROM_NAME  = _get_env("FROM_NAME", default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST = _get_env("SMTP_HOST", default="smtp.gmail.com")
SMTP_PORT = int(_get_env("SMTP_PORT", default="587"))
SMTP_USER = _get_env("SMTP_USER", default=FROM_EMAIL)
SMTP_PASS = _get_env("SMTP_PASS")
SMTP_USE_TLS = True

BCC_TO = _get_env("BCC_TO", default="")

PUBLIC_BASE   = _get_env("PUBLIC_BASE")
PORTFOLIO_URL = _get_env("PORTFOLIO_URL", default=PUBLIC_BASE + "/portfolio")
UPLOAD_URL    = _get_env("UPLOAD_URL", default="https://matlycreative.com/upload")

# ----------------- SUBJECT + BODY (UNCHANGED) -----------------
SUBJECT_A = "Quick question about {Company}"
SUBJECT_B = "Quick question about {Company}"

BODY_A = """Hi there,
Quick question — are you currently doing anything with video for your property listings, or is that not a focus right now?

Saw a few of your recent listings and figured I’d ask.

Best,
Matthieu from Matly
"""

BODY_B = """Hey {First},
Quick question — are you currently doing anything with video for your property listings, or is that not a focus right now?

Saw a few of your recent listings and figured I’d ask.

Best,
Matthieu from Matly
"""

# ----------------- helpers -----------------
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

def clean_email(raw: str) -> str:
    m = EMAIL_RE.search(raw or "")
    return m.group(0) if m else ""

def fill(tpl: str, **kw):
    for k,v in kw.items():
        tpl = tpl.replace("{"+k+"}", v or "")
    return tpl

# ----------------- Trello -----------------
SESSION = requests.Session()

def trello_get(path, **params):
    params.update({"key":TRELLO_KEY,"token":TRELLO_TOKEN})
    r = SESSION.get("https://api.trello.com/1/"+path, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def trello_post(path, **params):
    params.update({"key":TRELLO_KEY,"token":TRELLO_TOKEN})
    r = SESSION.post("https://api.trello.com/1/"+path, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# ----------------- send -----------------
def send_email(
    to_email: str,
    subject: str,
    body_text: str,
    *,
    link_url: str,
    link_text: str,
    link_color: str
):
    from email.message import EmailMessage
    import smtplib

    # Plain-text body only
    body_pt = body_text or ""

    # Expand [here] token safely
    if "[here]" in body_pt:
        body_pt = body_pt.replace("[here]", UPLOAD_URL)

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)

    if BCC_TO:
        msg["Bcc"] = BCC_TO

    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_DEBUG:
                    s.set_debuglevel(1)
                if SMTP_USE_TLS:
                    s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            log(f"[WARN] SMTP attempt {attempt+1}/3 failed: {e}")
            if attempt == 2:
                raise
            time.sleep(1.0 * (attempt + 1))

# ----------------- main -----------------
def main():
    cards = trello_get(f"lists/{LIST_ID}/cards", fields="id,name,desc", limit=200)

    for c in cards:
        desc = c.get("desc","")
        email = clean_email(desc)
        if not email:
            continue

        company = c.get("name","")
        first = ""

        pid = choose_id(company, email)
        link = PORTFOLIO_URL

        subject = fill(SUBJECT_A, Company=company)
        body = fill(BODY_A, Company=company, First=first, link=link)

        send_email(email, subject, body)
        trello_post(f"cards/{c['id']}/actions/comments", text="Sent: Day0")
        time.sleep(1)

if __name__ == "__main__":
    main()
