#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Day-0 — Poll Trello and send one email per card.

STRICT RULES:
- Personalized ID = Company slug (fallback email-safe).
- READY -> link to personal page   : <PUBLIC_BASE>/p/?id=<id>
- NOT READY -> link to portfolio   : <PORTFOLIO_URL>  (defaults to <PUBLIC_BASE>/portfolio)
- With MATLY_POINTER_BASE: pointer must exist, be fresh, AND filename must contain 'sample'.
- No signature "Email me" line; no hidden overrides in send_email().

NEW:
- Add a clickable [here] link in the NOT READY path to UPLOAD_URL (default https://matlycreative.com/upload/).
"""

import os, re, time, json, html, unicodedata, mimetypes
from datetime import datetime, timezone, timedelta
from typing import Tuple
import requests

def log(*a): print(*a, flush=True)

# ----------------- tiny utils -----------------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

def _env_bool(name: str, default: str = "0") -> bool:
    val = os.getenv(name, default)
    return (val or "").strip().lower() in ("1","true","yes","on")

def _safe_id_from_email(email: str) -> str:
    return (email or "").strip().lower().replace("@", "_").replace(".", "_")

def _slugify_company(name: str) -> str:
    s = (name or "").strip()
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]+", "", s)
    s = re.sub(r"[\s-]+", "_", s).strip("_")
    return s or ""

def choose_id(company: str, email: str) -> str:
    sid = _slugify_company(company)
    return sid if sid else _safe_id_from_email(email)

def _norm_base(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")

# ----------------- env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME",  default="Matthieu from Matly")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlycreative.com")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")
SMTP_DEBUG   = _env_bool("SMTP_DEBUG", "0")
BCC_TO       = _get_env("BCC_TO", default="").strip()

PUBLIC_BASE   = _norm_base(_get_env("PUBLIC_BASE"))  # e.g., https://matlycreative.com
PORTFOLIO_URL = _norm_base(_get_env("PORTFOLIO_URL")) or (PUBLIC_BASE + "/portfolio")

# Upload page link for NOT READY path
UPLOAD_URL = _get_env("UPLOAD_URL", default="https://matlycreative.com/upload/").rstrip("/")

# Pointer readiness (recommended)
MATLY_POINTER_BASE = _get_env("MATLY_POINTER_BASE", default="").rstrip("/")
READY_MAX_AGE_DAYS = int(_get_env("READY_MAX_AGE_DAYS", default="30"))

# Link look
INCLUDE_PLAIN_URL = _env_bool("INCLUDE_PLAIN_URL", "0")
LINK_TEXT         = _get_env("LINK_TEXT",  default="See examples")
LINK_COLOR        = _get_env("LINK_COLOR", default="#858585")


# Send control
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", "SENT_MARKER", default="Sent: Day0")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_day0.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

log(f"[env] PUBLIC_BASE={PUBLIC_BASE} | PORTFOLIO_URL={PORTFOLIO_URL} | UPLOAD_URL={UPLOAD_URL} | POINTER_BASE={MATLY_POINTER_BASE or '(disabled)'}")

# ----------------- HTTP -----------------
UA = f"TrelloEmailer-Day0/6.2 (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ----------------- templates -----------------
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env("SUBJECT_A", default="Polished videos for {company}'s listings")
    SUBJECT_B = _get_env("SUBJECT_B", default="Polished videos for {company}'s listings")
    BODY_A = _get_env("BODY_A", default=
"""Hi there, hope you're doing well,

I’ve been looking at {company}’s listings and you’re working with some really strong properties.

We are a video editing company who works specifically with real estate agents and agencies: you send raw footage, we send back clean, polished listing videos that look high-end and are ready to post.

Here’s our portfolio with examples of how polished video can make properties more appealing to clients : {extra} : {link}.

{extra}

If you ever feel that editing is slowing you down, we can handle that part so you can focus on getting more listings and viewings booked.

Would you be open to testing this on one upcoming property?

Best,
Matthieu from Matly""")
    BODY_B = _get_env("BODY_B", default=
"""Hi {first},

I’ve been looking at {company}’s listings and you’re working with some really strong properties.

We are a video editing company who works specifically with real estate agents and agencies: you send raw footage, we send back clean, polished listing videos that look high-end and are ready to post.

Here’s our portfolio with examples of how polished video can make properties more appealing to clients : {extra} : {link}.

{extra}

If you ever feel that editing is slowing you down, we can handle that part so you can focus on getting more listings and viewings booked.

Would you be open to testing this on one upcoming property?

Best,
Matthieu from Matly""")
else:
    SUBJECT_A = "Polished videos for {company}'s listings"
    SUBJECT_B = "Polished videos for {company}'s listings"
    BODY_A = """Hi there, hope you're doing well,

I’ve been looking at {company}’s listings and you’re working with some really strong properties.

We are a video editing company who works specifically with real estate agents and agencies: you send raw footage, we send back clean, polished listing videos that look high-end and are ready to post.

Here’s our portfolio with examples of how polished video can make properties more appealing to clients : {extra} : {link}.

{extra}

If you ever feel that editing is slowing you down, we can handle that part so you can focus on getting more listings and viewings booked.

Would you be open to testing this on one upcoming property?

Best,
Matthieu from Matly"""
    BODY_B = """Hi {first},

I’ve been looking at {company}’s listings and you’re working with some really strong properties.

We are a video editing company who works specifically with real estate agents and agencies: you send raw footage, we send back clean, polished listing videos that look high-end and are ready to post.

Here’s our portfolio with examples of how polished video can make properties more appealing to clients : {extra} : {link}.

{extra}

If you ever feel that editing is slowing you down, we can handle that part so you can focus on getting more listings and viewings booked.

Would you be open to testing this on one upcoming property?

Best,
Matthieu from Matly"""

# ----------------- parsing -----------------
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
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

# ----------------- Trello I/O -----------------
def _trello_call(method, url_path, **params):
    for attempt in range(3):
        try:
            params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
            url = f"https://api.trello.com/1/{url_path.lstrip('/')}"
            r = (SESS.get if method == "GET" else SESS.post)(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: raise
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError("Unreachable")

def trello_get(url_path, **params):  return _trello_call("GET", url_path, **params)
def trello_post(url_path, **params): return _trello_call("POST", url_path, **params)

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
    if extra: text += f"\n{extra}"
    try:
        trello_post(f"cards/{card_id}/actions/comments", text=text)
    except Exception:
        pass

# ----------------- readiness -----------------
def is_sample_ready(pid: str) -> bool:
    """
    A sample is READY if /api/sample?id=<pid> returns 200
    and the JSON contains any non-empty src/streamUrl/signedUrl/url field.
    No Trello flags, no pointers – fully automatic.
    """
    check_url = f"{PUBLIC_BASE}/api/sample?id={pid}"
    try:
        r = SESS.get(check_url, timeout=12, headers={"Accept": "application/json"})
        if r.status_code != 200:
            log(f"[ready] id={pid} -> HTTP {r.status_code}")
            return False

        ctype = (r.headers.get("Content-Type") or "").lower()
        data = r.json() if "application/json" in ctype else {}
        if not isinstance(data, dict):
            log(f"[ready] id={pid} -> non-dict JSON")
            return False

        # Any of these fields counts as “we have a playable sample”
        src = (
            (data.get("src")
             or data.get("streamUrl")
             or data.get("signedUrl")
             or data.get("url")
             or "")
            .strip()
        )

        if not src:
            log(f"[ready] id={pid} -> no src/url field")
            return False

        log(f"[ready] id={pid} -> True (src={src[:80]}...)")
        return True

    except Exception as e:
        log(f"[ready] id={pid} -> error: {e}")
        return False


# ----------------- templating -----------------
def fill_template(tpl: str, *, company: str, first: str, from_name: str, link: str = "", extra: str = "") -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
# ----------------- templating -----------------
def fill_template(tpl: str, *, company: str, first: str, from_name: str, link: str = "", extra: str = "") -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        if key == "link":      return link or ""
        if key == "extra":     return extra or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name|link|extra)\s*}", repl, tpl, flags=re.I)

def fill_template_skip_extra(tpl: str, *, company: str, first: str, from_name: str, link: str) -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        if key == "link":      return link or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name|link)\s*}", repl, tpl, flags=re.I)

EXTRA_TOKEN = re.compile(r"\{\s*extra\s*\}", flags=re.I)

def fill_with_two_extras(
    tpl: str, *, company: str, first: str, from_name: str,
    link: str, is_ready: bool, extra_ready: str, extra_wait: str
) -> str:
    base = fill_template_skip_extra(
        tpl, company=company, first=first, from_name=from_name, link=link
    )
    if is_ready:
        step1 = EXTRA_TOKEN.sub(extra_ready, base, count=1)
        step2 = EXTRA_TOKEN.sub("",         step1, count=1)
    else:
        step1 = EXTRA_TOKEN.sub("",         base, count=1)
        step2 = EXTRA_TOKEN.sub(extra_wait, step1, count=1)
    final = EXTRA_TOKEN.sub("", step2)
    final = re.sub(r"\s*:\s+(?=(https?://|www\.|<))", " ", final)
    final = re.sub(r"\n{3,}", "\n\n", final).strip()
    return final

def sanitize_subject(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]

def text_to_html(text: str) -> str:
    """
    Turn plain text into paragraphs/br with Matly dark style.
    Returns inner HTML (card wrapper is added later).
    """
    esc = html.escape(text or "").replace("\r\n", "\n").replace("\r", "\n")
    esc = esc.replace("\n\n", "</p><p>").replace("\n", "<br>")

    # Bigger + bolder body text
    p_style = (
        "margin:0 0 16px 0;"
        "color:#f5f5f7 !important;"
        "font-size:16px !important;"
        "line-height:1.8;"
        "font-weight:400;"
    )

    esc = f'<p style="{p_style}">{esc}</p>'
    esc = esc.replace("<p>", f'<p style="{p_style}">')
    return esc


def wrap_html(inner: str) -> str:
    """
    Wrap inner HTML in a centered Matly-style dark card,
    with a colored header box (logo) at the top and a plain bar at the bottom.
    """
    inner = inner or ""
    wrapper_style = (
        'font-family:"Roboto",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;'
        "color:#f5f5f7 !important;"
        "font-size:16px;"
        "line-height:1.8;"
        "font-weight:400;"
        "-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;"
    )

    bar_color_top = "#292929"
    bar_color_bottom = "#292929"

    header_logo_url = (
        "http://matlycreative.com/wp-content/uploads/2025/11/signture_final_version.png"
    )

    return f"""
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#FCFCFC;padding:24px 24px;">
  <tr>
    <td align="center">
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:760px;border-radius:18px;overflow:hidden;background:#1e1e1e;border:2.8px solid #000000;box-shadow:1 18px 45px #000000;">
        <!-- Top colored box with logo -->
        <tr>
          <td style="padding:12px 12px;background:{bar_color_top};text-align:center;">
            <img src="{html.escape(header_logo_url)}"
                 alt="Matly Creative"
                 style="max-height:90px;display:inline-block;border:0;">
          </td>
        </tr>
        <!-- Main content -->
        <tr>
          <td style="padding:30px 30px 30px 30px;">
            <div style="{wrapper_style}">
              {inner}
            </div>
          </td>
        </tr>
        <!-- Bottom bar (no logo) -->
        <tr>
          <td style="padding:0;background:{bar_color_bottom};height:24px;line-height:0;font-size:0;">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>
</table>
""".strip()

# ----------------- signature (no 'Email me' line) -----------------
SIGNATURE_LOGO_URL    = "http://matlycreative.com/wp-content/uploads/2025/11/signture_final_version.png"
SIGNATURE_INLINE      = os.getenv("SIGNATURE_INLINE", "0").strip().lower() in ("1","true","yes","on")
SIGNATURE_MAX_W_PX    = int(os.getenv("SIGNATURE_MAX_W_PX", "100"))
SIGNATURE_ADD_NAME    = os.getenv("SIGNATURE_ADD_NAME", "1").strip().lower() in ("1","true","yes","on")
SIGNATURE_CUSTOM_TEXT = os.getenv("SIGNATURE_CUSTOM_TEXT", "").strip()

def signature_html(logo_cid: str | None) -> str:
    # Logo URL used for the signature
    logo_url = "http://matlycreative.com/wp-content/uploads/2025/11/signture_final_version.png"
    if not logo_url:
        return ""

    # Use a table row so email clients respect left alignment
    return (
        """
<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" border="0" style="margin-top:0px;">
  <tr>
    <td align="left" style="padding:0;">
      <img src="%s"
           alt="Matly Creative"
           style="max-width:90px;height:auto;border:0;display:block;vertical-align:middle;">
    </td>
  </tr>
</table>
""" % html.escape(logo_url)
    )

# ----------------- sender (uses the chosen_link exactly) -----------------
def send_email(to_email: str, subject: str, body_text: str, *, link_url: str, link_text: str, link_color: str):
    from email.message import EmailMessage
    import smtplib

    label = (link_text or "See examples").strip()
    if link_url and not re.match(r"^https?://", link_url, flags=re.I):
        link_url = "https://" + link_url

    full = link_url
    bare = re.sub(r"^https?://", "", full, flags=re.I) if full else ""
    esc_full = html.escape(full, quote=True) if full else ""
    esc_bare = html.escape(bare, quote=True) if full else ""

    # Plain text: also expand [here] → UPLOAD_URL
    body_pt = body_text
    if "[here]" in body_pt:
        body_pt = body_pt.replace("[here]", UPLOAD_URL)
    if full:
        if not INCLUDE_PLAIN_URL:
            for pat in (full, bare):
                if pat:
                    body_pt = body_pt.replace(pat, label)
        else:
            if full not in body_pt and bare not in body_pt:
                body_pt = (body_pt.rstrip() + "\n\n" + full).strip()

    # HTML with explicit markers
    MARK = "__LINK_MARKER__"
    body_marked = body_text
    for pat in (full, bare):
        if pat:
            body_marked = body_marked.replace(pat, MARK)

    html_core_inner = text_to_html(body_marked)
    html_core_inner = re.sub(re.escape(esc_full), MARK, html_core_inner)
    html_core_inner = re.sub(re.escape(esc_bare), MARK, html_core_inner)

    # Insert main anchor
    if full:
        style_attr = f' style="color:{html.escape(link_color or LINK_COLOR)};text-decoration:underline;"'
        anchor = f'<a{style_attr} href="{html.escape(full, quote=True)}">{html.escape(label)}</a>'
        html_core_inner = html_core_inner.replace(MARK, anchor) if MARK in html_core_inner else (html_core_inner + f"<p style=\"margin:0 0 14px 0;\">{anchor}</p>")

    # Convert [here] into clickable upload link
    if "[here]" in html_core_inner:
        upload_anchor = f'<a href="{html.escape(UPLOAD_URL, quote=True)}" style="color:{html.escape(link_color or LINK_COLOR)};text-decoration:underline;">here</a>'
        html_core_inner = html_core_inner.replace("[here]", upload_anchor)

    # Signature inside the content
    logo_cid = "siglogo@local"
    sig_inner = signature_html(logo_cid if SIGNATURE_INLINE else None)

    # Wrap in Matly card (top logo + bottom bar)
    html_full = wrap_html(html_core_inner + sig_inner)

    # ----- Build message -----
    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)
    msg.add_alternative(html_full, subtype="html")
    if BCC_TO:
        msg["Bcc"] = BCC_TO

    # Inline logo embed (if configured)
    if SIGNATURE_INLINE and SIGNATURE_LOGO_URL:
        try:
            r = requests.get(SIGNATURE_LOGO_URL, timeout=20)
            r.raise_for_status()
            data = r.content
            ctype = r.headers.get("Content-Type") or mimetypes.guess_type(SIGNATURE_LOGO_URL)[0] or "image/png"
            if not ctype.startswith("image/"):
                ctype = "image/png"
            maintype, subtype = ctype.split("/", 1)
            msg.get_payload()[-1].add_related(data, maintype=maintype, subtype=subtype, cid="siglogo@local")
        except Exception as e:
            log(f"Inline logo fetch failed, sending without embed: {e}")

    # ----- Send via SMTP -----
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

# ----------------- cache -----------------
def load_sent_cache():
    try:
        with open(SENT_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_sent_cache(ids):
    d = os.path.dirname(SENT_CACHE_FILE)
    if d: os.makedirs(d, exist_ok=True)
    try:
        with open(SENT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f)
    except Exception:
        pass

# ----------------- main -----------------
def main():
    missing = []
    for k in ("TRELLO_KEY","TRELLO_TOKEN","FROM_EMAIL","SMTP_PASS","PUBLIC_BASE"):
        if not globals()[k]:
            missing.append(k)
    if not LIST_ID: missing.append("TRELLO_LIST_ID_DAY0")
    if missing: raise SystemExit("Missing env: " + ", ".join(missing))

    sent_cache = load_sent_cache()
    cards = trello_get(f"lists/{LIST_ID}/cards", fields="id,name,desc", limit=200)
    if not isinstance(cards, list):
        log("No cards found or Trello error."); return

    processed = 0
    for c in cards:
        if MAX_SEND_PER_RUN and processed >= MAX_SEND_PER_RUN: break
        card_id = c.get("id"); title = c.get("name","(no title)")
        if not card_id or card_id in sent_cache: continue

        desc = c.get("desc") or ""
        fields  = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First")   or "").strip()
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)
        
        if not email_v:
            log(f"Skip: no valid Email on '{title}'."); continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            log(f"Skip: already marked '{SENT_MARKER_TEXT}' — {title}")
            sent_cache.add(card_id); continue

        pid   = choose_id(company, email_v)
        ready = is_sample_ready(pid)
        chosen_link = (f"{PUBLIC_BASE}/p/?id={pid}" if ready else PORTFOLIO_URL)
        log(f"[decide] id={pid} ready={ready} -> link={chosen_link}")

        use_b    = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B    if use_b else BODY_A

        subject = fill_template(subj_tpl, company=company, first=first, from_name=FROM_NAME, link=chosen_link)

        extra_ready = "as well as a free sample made with your content"
        # MUST CONTAIN [here] so we can link it to UPLOAD_URL
        extra_wait  = "If you can share 1–2 raw clips, I’ll cut a quick sample for you this week (free) — upload them [here]."

        body = fill_with_two_extras(
            body_tpl, company=company, first=first, from_name=FROM_NAME,
            link=chosen_link, is_ready=ready, extra_ready=extra_ready, extra_wait=extra_wait
        )

        # Safety net: ensure [here] exists in NOT-READY copy
        if not ready and "[here]" not in body:
            body += " — upload them [here]."

        link_label = "Portfolio + Sample (free)" if ready else LINK_TEXT

        try:
            send_email(email_v, subject, body, link_url=chosen_link, link_text=link_label, link_color=LINK_COLOR)
            processed += 1
            log(f"Sent to {email_v} — '{title}' — ready={ready} link={chosen_link}")
        except Exception as e:
            log(f"Send failed for '{title}' to {email_v}: {e}")
            continue

        mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        sent_cache.add(card_id); save_sent_cache(sent_cache); time.sleep(0.8)

    log(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
