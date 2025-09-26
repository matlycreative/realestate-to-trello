# realestate_to_trello.py
# Daily: find 10 real-estate leads worldwide, update Trello template cards (1/minute).
# Only edits three lines: Company / First / Email (atomic update). Respects robots.txt.

import os, re, json, time, random
from datetime import date
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser

# Optional: load local .env when running on your laptop (ignored on GitHub Actions)
try:
    from dotenv import load_dotenv  # pip install python-dotenv (only if running locally)
    load_dotenv()
except Exception:
    pass

# ---------- Config (env) ----------
DAILY_LIMIT     = int(os.getenv("DAILY_LIMIT", "10"))
PUSH_INTERVAL_S = int(os.getenv("PUSH_INTERVAL_S", "60"))    # 1 per minute
REQUEST_DELAY_S = float(os.getenv("REQUEST_DELAY_S", "1.0")) # polite crawl delay
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")  # your "Start here" list
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # optional, to clone if none are blank

FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")  # optional fallback to get website

SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

OSM_FILTERS = [('office','estate_agent'), ('shop','estate_agent')]
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
ROLE_WORDS = [
    "owner","founder","managing director","managing partner","director","ceo",
    "principal","broker","agent","manager","geschäftsführer","inhaber","leiter",
    "marketing","content","social media"
]

# Only search these countries/cities (rotate daily)
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

def _sleep(): time.sleep(REQUEST_DELAY_S)

def pick_today_city():
    return CITY_ROTATION[date.today().toordinal() % len(CITY_ROTATION)]

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
    """Best-effort: search by name near city center, return website URL if found."""
    if not FOURSQUARE_API_KEY:
        return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept":"application/json"}
    try:
        params = {
            "query": name,
            "ll": f"{lat},{lon}",
            "limit": 1,
            "radius": 50000,
            "fields": "website"
        }
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
    for p in ["/about","/team","/impressum","/kontakt","/contact","/ueber-uns","/uber-uns","/staff"]:
        pages.append(urljoin(base, p))
    return pages

def extract_emails(text):
    return list(set(m.group(0) for m in EMAIL_RE.finditer(text or "")))

def extract_people_jsonld(soup):
    people = []
    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if isinstance(it, dict) and it.get("@type") in ("Person","Employee","Organization"):
                nm = it.get("name"); job = it.get("jobTitle") or it.get("description")
                if nm and isinstance(nm, str):
                    people.append((nm.strip(), (job or "").strip()))
    return people

def extract_people_heuristic(soup):
    text = soup.get_text(" ").strip()
    hits = []
    for role in ROLE_WORDS:
        pat = re.compile(rf"{role}[:\s\-–]*([A-ZÄÖÜ][a-zA-Zäöüß\-']+(?:\s+[A-ZÄÖÜ][a-zA-Zäöüß\-']+){{1,2}})", re.I)
        for m in pat.finditer(text):
            hits.append((m.group(1).strip(), role))
    for tag in soup.select("[class*='team'],[class*='person'],[class*='member']"):
        nm = tag.get_text(" ", strip=True)
        m = re.search(r"([A-ZÄÖÜ][a-zA-Zäöüß\-']+(?:\s+[A-ZÄÖÜ][a-zA-Zäöüß\-']+){1,2})", nm)
        if m:
            hits.append((m.group(1).strip(), "team"))
    return list({(n, r) for n, r in hits})

def choose_email(emails):
    if not emails: return None
    prefs = ["owner","ceo","md","sales","hello","contact","media","marketing"]
    def key(e):
        local = e.split("@")[0].lower()
        return (not any(local.startswith(p) for p in prefs), local.startswith("info"))
    return sorted(emails, key=key)[0]

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

        emails = set(extract_emails(resp.text))
        for a in soup.select('a[href^="mailto:"]'):
            m = EMAIL_RE.search(a.get("href",""))
            if m: emails.add(m.group(0))

        people = extract_people_jsonld(soup) + extract_people_heuristic(soup)
        contact_name = None
        if people:
            chosen = None
            for p, r in people:
                if any(k in (r or "").lower() for k in ["marketing","content","social","manager","owner","director","broker"]):
                    chosen = (p, r); break
            contact_name = (chosen or people[0])[0]
        chosen_email = choose_email(list(emails))
        if chosen_email and not out["email"]:
            out["email"] = chosen_email
        if contact_name and not out["first"]:
            out["first"] = contact_name.split(" ")[0]

        if out["email"] and out["first"]:
            break
        _sleep()

    if not out["email"]:
        dom = domain_from_url(site_url)
        if dom: out["email"] = f"info@{dom}"
    return out

# ---------- Trello (STRICT: only 3 lines) ----------
_FIELD_PATTERNS = {
    "Company": re.compile(r"(?mi)^(?P<prefix>\s*Company\s*:\s*)(?P<value>.*)$"),
    "First":   re.compile(r"(?mi)^(?P<prefix>\s*First\s*:\s*)(?P<value>.*)$"),
    "Email":   re.compile(r"(?mi)^(?P<prefix>\s*Email\s*:\s*)(?P<value>.*)$"),
}

def trello_get_card_desc(card_id):
    r = SESS.get(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "desc"},
                 timeout=30)
    r.raise_for_status()
    return r.json().get("desc") or ""

def replace_line(desc, label, new_value):
    """Replace exactly one existing 'Label: ...' line. If missing, leave desc unchanged."""
    pat = _FIELD_PATTERNS[label]
    if pat.search(desc):
        return pat.sub(lambda m: f"{m.group('prefix')}{new_value or ''}", desc, count=1), True
    return desc, False

def update_card_company_first_email(card_id, company, first, email):
    """Only touches Company/First/Email lines. Everything else remains unchanged. Single PUT (atomic)."""
    desc = trello_get_card_desc(card_id)
    changed = False
    for label, value in [("Company", company or ""), ("First", first or ""), ("Email", email or "")]:
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
    """If no empty card exists yet, clone a known template card into the list."""
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
    # Helpful missing-keys message
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

        # Ensure we have a website: Foursquare fallback if missing
        website = biz.get("website")
        if not website:
            website = fsq_find_website(biz["business_name"], lat, lon)

        if not website:
            continue  # skip if no site to scrape

        contact = crawl_contact(website)
        first = contact.get("first") or ""
        email = contact.get("email") or ""

        # Need an '@' to trigger Butler; skip if no email at all
        if not email or "@" not in email:
            continue

        leads.append({
            "Company": biz["business_name"],
            "First": first,
            "Email": email
        })
        _sleep()

    # 2) Push to Trello — one per minute, always refinding the next empty card
    pushed = 0
    for lead in leads:
        # find (or create) the next empty template card
        empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties and TRELLO_TEMPLATE_CARD_ID:
            clone_template_into_list(TRELLO_TEMPLATE_CARD_ID, TRELLO_LIST_ID)
            empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties:
            print("No empty template card available; skipping this lead.")
            continue

        card_id = empties[0]
        changed = update_card_company_first_email(
            card_id=card_id,
            company=lead["Company"],
            first=lead["First"],
            email=lead["Email"]
        )
        if changed:
            pushed += 1
            print(f"[{pushed}/{DAILY_LIMIT}] Filled card: {lead['Company']} — {lead['First']} — {lead['Email']}")
            # Important: give Butler time to move + duplicate before grabbing the next blank
            time.sleep(PUSH_INTERVAL_S)
        else:
            print("Card unchanged (labels missing or already filled); trying next lead.")

    print(f"Done. Leads pushed: {pushed}/{len(leads)}")

if __name__ == "__main__":
    main() 
