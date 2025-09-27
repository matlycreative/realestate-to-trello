# realestate_to_trello.py
# Find real-estate leads and fill Trello template cards (1/minute).
# Only edits: Company / First / Email / Website.

import os, re, json, time, random
from datetime import date
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser

# Optional: load .env locally (ignored on GitHub Actions if not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- Config (env) ----------
DAILY_LIMIT     = int(os.getenv("DAILY_LIMIT", "10"))
PUSH_INTERVAL_S = int(os.getenv("PUSH_INTERVAL_S", "60"))     # 1 per minute
REQUEST_DELAY_S = float(os.getenv("REQUEST_DELAY_S", "1.0"))  # polite crawl delay
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")  # "Start here" list
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # optional: clone if no blank exists

FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")  # optional fallback

SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

OSM_FILTERS = [('office','estate_agent'), ('shop','estate_agent')]
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# Expanded, multilingual role keywords (editor/marketing-friendly)
ROLE_WORDS = list(set([
    # EN
    "owner","founder","managing director","managing partner","director","ceo",
    "principal","broker","agent","manager","marketing","content","social media","growth","editor",
    # DE
    "geschäftsführer","inhaber","inhaberin","leiter","verkauf","makler","agenturleitung","marketingleitung",
    # FR
    "propriétaire","fondateur","fondatrice","directeur","directrice","gérant","gérante","responsable","courtier","agent",
    # IT
    "titolare","fondatore","fondatrice","direttore","direttrice","amministratore","responsabile","agente","broker",
    # ES / PT
    "propietario","propietaria","fundador","fundadora","director","directora","gerente","responsable","agente",
    "corretor","corretora","proprietário","proprietária","diretor","diretora",
    # NL / BE
    "eigenaar","directeur","zaakvoerder","verantwoordelijke","makelaar",
    # Nordics
    "daglig leder","eier","direktør","markedsføring","markedschef","marknadsföring","ägare","vd",
    # HR
    "vlasnik","vlasnica","direktor","direktorica","voditelj","marketing",
]))

# --- Only your requested regions (rotate daily) ---
CITY_ROTATION = [
    # Switzerland
    ("Zurich","Switzerland"), ("Geneva","Switzerland"), ("Basel","Switzerland"), ("Lausanne","Switzerland"),
    # United Kingdom
    ("London","United Kingdom"), ("Manchester","United Kingdom"), ("Birmingham","United Kingdom"), ("Edinburgh","United Kingdom"),
    # United States
    ("New York","United States"), ("Los Angeles","United States"), ("Chicago","United States"),
    ("Miami","United States"), ("San Francisco","United States"), ("Dallas","United States"),
    # France
    ("Paris","France"), ("Lyon","France"), ("Marseille","France"), ("Toulouse","France"),
    # Germany
    ("Berlin","Germany"), ("Munich","Germany"), ("Hamburg","Germany"), ("Frankfurt","Germany"),
    # Italy
    ("Milan","Italy"), ("Rome","Italy"), ("Naples","Italy"), ("Turin","Italy"),
    # Norway
    ("Oslo","Norway"), ("Bergen","Norway"),
    # Denmark
    ("Copenhagen","Denmark"), ("Aarhus","Denmark"),
    # Austria
    ("Vienna","Austria"), ("Salzburg","Austria"), ("Graz","Austria"),
    # Spain
    ("Madrid","Spain"), ("Barcelona","Spain"), ("Valencia","Spain"),
    # Portugal
    ("Lisbon","Portugal"), ("Porto","Portugal"),
    # Netherlands
    ("Amsterdam","Netherlands"), ("Rotterdam","Netherlands"), ("The Hague","Netherlands"),
    # Belgium
    ("Brussels","Belgium"), ("Antwerp","Belgium"), ("Ghent","Belgium"),
    # Luxembourg
    ("Luxembourg City","Luxembourg"),
    # Croatia
    ("Zagreb","Croatia"), ("Split","Croatia"), ("Rijeka","Croatia"),
    # Dubai (UAE)
    ("Dubai","United Arab Emirates"),
    # Indonesia
    ("Jakarta","Indonesia"), ("Surabaya","Indonesia"), ("Bandung","Indonesia"), ("Denpasar","Indonesia"),
    # Canada
    ("Toronto","Canada"), ("Vancouver","Canada"), ("Montreal","Canada"), ("Calgary","Canada"), ("Ottawa","Canada"),
]

GENERIC_MAIL_PROVIDERS = {
    "gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com",
    "proton.me","protonmail.com","aol.com","live.com","msn.com"
}
GENERIC_MAILBOXES = {
    "info","contact","hello","hi","office","team","sales","support",
    "admin","enquiries","marketing","media","press","service","mail"
}

NAME_RE = re.compile(r"\b([A-ZÀ-ÖØ-Ý][a-zà-öø-ÿ'’\-]+)\s+([A-ZÀ-ÖØ-Ý][a-zà-öø-ÿ'’\-]+)\b")

def _sleep(): time.sleep(REQUEST_DELAY_S)
def pick_today_city(): return CITY_ROTATION[date.today().toordinal() % len(CITY_ROTATION)]

# ----------------- OSM geocode + search -----------------
def geocode_city(city, country):
    r = SESS.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": f"{city}, {country}", "format":"json", "limit":1},
        headers={"Referer":"https://nominatim.org"},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"Nominatim couldn't find {city}, {country}")
    south, north, west, east = map(float, data[0]["boundingbox"])
    return south, west, north, east

def overpass_estate_agents(bbox):
    south, west, north, east = bbox
    parts = []
    for k, v in OSM_FILTERS:
        for t in ("node","way","relation"):
            parts.append(f'{t}["{k}"="{v}"]({south},{west},{north},{east});')
    q = f"""[out:json][timeout:25];({ ' '.join(parts) });out center tags;"""
    r = SESS.post("https://overpass-api.de/api/interpreter", data=q.encode("utf-8"), timeout=60)
    r.raise_for_status()
    js = r.json()
    rows = []
    for el in js.get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name")
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        email = tags.get("email") or tags.get("contact:email")
        if name:
            rows.append({
                "business_name": name.strip(),
                "website": normalize_url(website) if website else None,
                "email": email
            })
    # dedupe by (name, domain)
    dedup = {}
    for r0 in rows:
        key = (r0["business_name"].lower(), domain_from_url(r0["website"] or ""))
        if key not in dedup:
            dedup[key] = r0
    out = list(dedup.values())
    random.shuffle(out)
    return out

# ----------------- Foursquare fallback (website) -----------------
def fsq_find_website(name, lat, lon):
    if not FOURSQUARE_API_KEY:
        return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept":"application/json"}
    try:
        params = {"query": name, "ll": f"{lat},{lon}", "limit": 1, "radius": 50000, "fields": "website"}
        r = requests.get("https://api.foursquare.com/v3/places/search",
                         headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            data = r.json()
            results = data.get("results") or []
            if results:
                first = results[0]
                website = first.get("website")
                if website:
                    return normalize_url(website)
                fsq_id = first.get("fsq_id")
                if fsq_id:
                    d = requests.get(f"https://api.foursquare.com/v3/places/{fsq_id}",
                                     headers=headers, params={"fields":"website"}, timeout=20)
                    if d.status_code == 200:
                        w = d.json().get("website")
                        if w: return normalize_url(w)
    except Exception:
        return None
    return None

# ----------------- Scraping helpers -----------------
def normalize_url(u):
    if not u: return None
    if u.startswith("mailto:"): return None
    parsed = urlparse(u)
    if not parsed.scheme:
        u = "https://" + u.strip("/")
    return u

def domain_from_url(u):
    try:
        ext = tldextract.extract(u)
        if not ext.domain: return ""
        return ".".join([ext.domain, ext.suffix]) if ext.suffix else ext.domain
    except Exception:
        return ""

def allowed_by_robots(base_url, path="/"):
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(base_url, "/robots.txt"))
        rp.read()
        return rp.can_fetch(UA, urljoin(base_url, path))
    except Exception:
        return True

def fetch(url):
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return r

def gather_candidate_pages(base):
    pages = [base]
    common = [
        "/about","/about-us","/who-we-are","/our-story",
        "/team","/our-team","/meet-the-team","/leadership","/staff",
        "/agents","/our-agents","/brokers","/consultants","/people",
        "/contact","/contact-us",
        "/impressum","/kontakt","/ueber-uns","/uber-uns",
        "/equipe","/equipe-comercial","/equipo"
    ]
    for p in common:
        pages.append(urljoin(base, p))
    return pages

def extract_emails(text):
    return list(set(m.group(0) for m in EMAIL_RE.finditer(text or "")))

def extract_people_jsonld_deep(soup):
    """Find Person names in any ld+json, including @graph and nested fields."""
    people = []
    def walk(obj):
        if isinstance(obj, dict):
            t = obj.get("@type")
            if (t == "Person") or (isinstance(t, list) and "Person" in t):
                nm = obj.get("name")
                if isinstance(nm, str):
                    people.append((nm.strip(), (obj.get("jobTitle") or "").strip()))
            for k in ["founder","founders","employee","employees","member","members","contactPoint","author"]:
                if k in obj:
                    walk(obj[k])
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj: walk(it)

    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            data = json.loads(script.string)
            walk(data)
        except Exception:
            continue
    return people

def extract_people_heuristic(soup):
    text = soup.get_text(" ").strip()
    hits = []
    for role in ROLE_WORDS:
        pat = re.compile(rf"{role}[:\s\-–]*([A-ZÀ-ÖØ-Ý][a-zA-ZÀ-ÖØ-öø-ÿ'’\-]+(?:\s+[A-ZÀ-ÖØ-Ý][a-zA-ZÀ-ÖØ-öø-ÿ'’\-]+){{1,2}})", re.I)
        for m in pat.finditer(text):
            hits.append((m.group(1).strip(), role))
    for tag in soup.select("[class*='team'],[class*='person'],[class*='member'],[class*='agent']"):
        nm = tag.get_text(" ", strip=True)
        m = re.search(r"([A-ZÀ-ÖØ-Ý][a-zA-ZÀ-ÖØ-öø-ÿ'’\-]+(?:\s+[A-ZÀ-ÖØ-Ý][a-zA-ZÀ-ÖØ-öø-ÿ'’\-]+){1,2})", nm)
        if m:
            hits.append((m.group(1).strip(), "team"))
    # de-dup
    return list({(n, r) for n, r in hits})

def extract_hcard_names(soup):
    names = set()
    selectors = [
        ".vcard .fn", ".vcard .given-name", ".vcard .n .given-name",
        ".h-card .p-name", ".h-card .p-given-name",
        "[itemtype*='Person'] [itemprop='name']",
        "[class*='agent-name']", "[class*='broker-name']",
        "[class*='team-member'] [class*='name']",
    ]
    for sel in selectors:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            m = NAME_RE.search(txt)
            if m: names.add(m.group(0))
    return [(n, "hcard") for n in names]

def extract_mailto_context(soup):
    """If a mailto link has name-like text, capture it."""
    out = []
    for a in soup.select('a[href^="mailto:"]'):
        txt = a.get_text(" ", strip=True)
        for cand in [txt, a.parent.get_text(" ", strip=True) if a.parent else ""]:
            m = NAME_RE.search(cand)
            if m:
                out.append((m.group(0), "mailto"))
    return list({t[0]: t for t in out}.values())

def score_person(name, role_hint_text):
    score = 0
    for r in ROLE_WORDS:
        if r.lower() in role_hint_text:
            score += 2; break
    if len(name.split()) == 2: score += 1
    return score

def choose_best_person(soup, people):
    if not people: return None
    text = soup.get_text(" ").lower()
    ranked = sorted(people, key=lambda p: score_person(p[0], text), reverse=True)
    return ranked[0][0]

def choose_email(emails):
    if not emails: return None
    prefs = ["owner","ceo","md","sales","hello","contact","media","marketing"]
    def key(e):
        local = e.split("@")[0].lower()
        return (not any(local.startswith(p) for p in prefs), local.startswith("info"))
    return sorted(emails, key=key)[0]

def guess_first_from_email(email: str) -> str:
    """Best-effort first-name guess from email local part; returns '' if unsure."""
    if not email or "@" not in email:
        return ""
    local = email.split("@", 1)[0]
    local = re.sub(r"^(info|contact|hello|hi|team|office|sales|support|admin|marketing|media|press|service)[._\-+]*",
                   "", local, flags=re.I)
    parts = [p for p in re.split(r"[._\-+0-9]+", local) if p]
    if parts and parts[0].lower() in GENERIC_MAILBOXES:
        return ""
    for p in parts[:2]:
        if len(p) >= 2 and not re.fullmatch(r"[A-Za-z]", p):
            return p.capitalize()
    if len(parts) == 1 and len(parts[0]) >= 3:
        return parts[0].capitalize()
    return ""

def crawl_contact(site_url):
    out = {"first": "", "email": ""}
    if not site_url:
        return out
    for url in gather_candidate_pages(site_url):
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}/"
        path = urlparse(url).path or "/"
        if not allowed_by_robots(base, path):
            continue
        try:
            resp = fetch(url)
        except Exception:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")

        # collect emails
        emails = set(extract_emails(resp.text))
        for a in soup.select('a[href^="mailto:"]'):
            m = EMAIL_RE.search(a.get("href",""))
            if m: emails.add(m.group(0))
        chosen_email = choose_email(list(emails))

        # collect people from multiple signals
        people = []
        people += extract_people_jsonld_deep(soup)
        people += extract_people_heuristic(soup)
        people += extract_hcard_names(soup)
        people += extract_mailto_context(soup)

        full_name = choose_best_person(soup, people) if people else None
        if full_name and not out["first"]:
            out["first"] = full_name.split()[0].strip("-,;:()")

        if chosen_email and not out["email"]:
            out["email"] = chosen_email

        if out["email"] and out["first"]:
            break
        _sleep()

    if not out["email"]:
        dom = domain_from_url(site_url)
        if dom: out["email"] = f"info@{dom}"
    return out

# ---------- Trello (only these four lines) ----------
_FIELD_PATTERNS = {
    "Company": re.compile(r"(?mi)^(?P<prefix>\s*Company\s*:\s*)(?P<value>.*)$"),
    "First":   re.compile(r"(?mi)^(?P<prefix>\s*First\s*:\s*)(?P<value>.*)$"),
    "Email":   re.compile(r"(?mi)^(?P<prefix>\s*Email\s*:\s*)(?P<value>.*)$"),
    "Website": re.compile(r"(?mi)^(?P<prefix>\s*Website\s*:\s*)(?P<value>.*)$"),
}

# A line that looks like a field label, e.g., "Hook:" or "Website:"
FIELD_LABEL_LINE_RE = re.compile(r'^\s*[A-Za-z][A-Za-z0-9 _/\-]{0,40}\s*:\s*$', re.I)

def trello_get_card_desc(card_id):
    r = SESS.get(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "desc"},
                 timeout=30)
    r.raise_for_status()
    return r.json().get("desc") or ""

def replace_line(desc, label, new_value):
    """
    Safer replacer:
      - Writes the value inline on the same line as 'Label:'.
      - Only collapses a next line if it looks like a standalone *value* (no colon).
      - Never removes a line that looks like another field label (e.g., 'Hook:', 'Variant:', etc.).
    """
    lines = desc.splitlines()
    pat = re.compile(rf'^\s*{re.escape(label)}\s*:\s*(.*)$', re.I)

    for i, line in enumerate(lines):
        m = pat.match(line)
        if not m:
            continue

        trailing = (m.group(1) or "").rstrip()
        lines[i] = f"{label}: {new_value or ''}"

        if not trailing and (i + 1) < len(lines):
            nxt = lines[i + 1]
            if not FIELD_LABEL_LINE_RE.match(nxt):
                if nxt.strip() and ":" not in nxt:
                    lines.pop(i + 1)

        return "\n".join(lines), True

    return desc, False

def update_card_fields(card_id, company, first, email, website):
    """Only touches Company/First/Email/Website lines. Everything else remains unchanged."""
    desc = trello_get_card_desc(card_id)
    changed = False
    for label, value in [
        ("Company", company or ""),
        ("First",   first   or ""),
        ("Email",   email   or ""),
        ("Website", website or ""),
    ]:
        desc, did = replace_line(desc, label, value)
        changed = changed or did
    if not changed:
        return False
    r = SESS.put(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": desc},
                 timeout=30)
    r.raise_for_status()
    return True

def find_empty_template_cards(list_id, max_needed=1):
    """Find cards whose 'Company:' line exists but is blank (next template to fill)."""
    r = SESS.get(f"https://api.trello.com/1/lists/{list_id}/cards",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
                 timeout=30)
    r.raise_for_status()
    cards = r.json()
    empties = []
    for c in cards:
        desc = c.get("desc") or ""
        if re.search(r"(?mi)^ *Company\s*:\s*$", desc):
            empties.append(c["id"])
        if len(empties) >= max_needed:
            break
    return empties

def clone_template_into_list(template_card_id, list_id, name="Lead (auto)"):
    if not template_card_id:
        return None
    r = SESS.post("https://api.trello.com/1/cards",
                  params={"key":TRELLO_KEY,"token":TRELLO_TOKEN,
                          "idList":list_id,"idCardSource":template_card_id,"name":name},
                  timeout=30)
    r.raise_for_status()
    return r.json()["id"]

# --------------------------- Main ---------------------------
def main():
    missing = [name for name in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    # 1) Choose today's city and get agencies
    city, country = pick_today_city()
    south, west, north, east = geocode_city(city, country)
    lat = (south + north) / 2.0
    lon = (west + east) / 2.0

    candidates = overpass_estate_agents((south, west, north, east))
    leads = []

    for biz in candidates:
        if len(leads) >= DAILY_LIMIT:
            break

        website = biz.get("website")
        if not website:
            website = fsq_find_website(biz["business_name"], lat, lon)

        if not website and biz.get("email"):
            dom = biz["email"].split("@", 1)[-1].lower()
            if dom and dom not in GENERIC_MAIL_PROVIDERS:
                website = f"https://{dom}"

        if not website:
            continue  # need a site to scrape for contact

        contact = crawl_contact(website)
        first = contact.get("first") or ""
        email = contact.get("email") or ""

        if not first:
            first = guess_first_from_email(email)

        # Need '@' to trigger Butler; skip if no email at all
        if not email or "@" not in email:
            continue

        if not website and "@" in email:
            dom = email.split("@", 1)[-1].lower()
            if dom and dom not in GENERIC_MAIL_PROVIDERS:
                website = f"https://{dom}"

        leads.append({
            "Company": biz["business_name"],
            "First": first,
            "Email": email,
            "Website": website or ""
        })
        _sleep()

    # 2) Push to Trello — one per minute, always re-finding the next empty card
    pushed = 0
    for lead in leads:
        empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties and TRELLO_TEMPLATE_CARD_ID:
            clone_template_into_list(TRELLO_TEMPLATE_CARD_ID, TRELLO_LIST_ID)
            empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties:
            print("No empty template card available; skipping this lead.")
            continue

        card_id = empties[0]
        changed = update_card_fields(
            card_id=card_id,
            company=lead["Company"],
            first=lead["First"],
            email=lead["Email"],
            website=lead["Website"],
        )
        if changed:
            pushed += 1
            print(f"[{pushed}/{DAILY_LIMIT}] Filled card: {lead['Company']} — {lead['First']} — {lead['Email']} — {lead['Website']}")
            time.sleep(PUSH_INTERVAL_S)  # give Butler time to move + duplicate
        else:
            print("Card unchanged (labels missing or already filled); trying next lead.")

    print(f"Done. Leads pushed: {pushed}/{len(leads)}")

if __name__ == "__main__":
    main()
