# realestate_to_trello.py
# Fills Trello template cards with Company / Email / Website.
# Rewrites the header block to enforce this order (one label per line):
# Company, First, Email, Hook, Variant, Website.
# First/Hook/Variant are preserved (not modified); other fields unchanged.

import os, re, json, time, random
from datetime import date
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser

# Optional for local use
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- Config ----------
DAILY_LIMIT     = int(os.getenv("DAILY_LIMIT", "10"))
PUSH_INTERVAL_S = int(os.getenv("PUSH_INTERVAL_S", "60"))     # 1 per minute
REQUEST_DELAY_S = float(os.getenv("REQUEST_DELAY_S", "1.0"))
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # optional

FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")            # optional

SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

OSM_FILTERS = [('office','estate_agent'), ('shop','estate_agent')]
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# --- Countries/cities to rotate ---
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
        "/contact","/contact-us","/about","/about-us","/who-we-are","/our-story",
        "/team","/our-team","/agents","/our-agents","/brokers","/staff",
        "/impressum","/kontakt","/ueber-uns","/uber-uns","/equipe","/equipo"
    ]
    for p in common:
        pages.append(urljoin(base, p))
    return pages

def extract_emails(text):
    return list(set(m.group(0) for m in EMAIL_RE.finditer(text or "")))

def crawl_contact(site_url):
    """Return only an email (no first name)."""
    out = {"email": ""}
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

        if emails:
            # prefer non-info/mailbox addresses
            prefs = ["owner","ceo","md","sales","hello","contact","media","marketing"]
            def key(e):
                local = e.split("@")[0].lower()
                return (not any(local.startswith(p) for p in prefs), local.startswith("info"))
            out["email"] = sorted(emails, key=key)[0]
            break

        _sleep()

    if not out["email"]:
        dom = domain_from_url(site_url)
        if dom: out["email"] = f"info@{dom}"
    return out

# ---------- Trello helpers (header normalization) ----------
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}

def trello_get_card_desc(card_id):
    r = SESS.get(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "desc"},
                 timeout=30)
    r.raise_for_status()
    return r.json().get("desc") or ""

def normalize_header_block(desc, company, email, website):
    """
    Build a clean header block at the very top with fixed order:
    Company, First, Email, Hook, Variant, Website
    - Preserve original values for First/Hook/Variant if present.
    - Remove any previous instances of those header lines (and their immediate
      value-only next line) from the rest of the description.
    - Keep everything else exactly as-is.
    """
    lines = desc.splitlines()
    preserved = {"First": "", "Hook": "", "Variant": ""}

    keep = []
    i = 0
    while i < len(lines):
        line = lines[i]
        matched = False
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                matched = True
                val = (m.group(1) or "").strip()
                # If the value was on the next line, capture it and drop that next line
                if not val and (i + 1) < len(lines):
                    nxt = lines[i + 1]
                    # treat next line as a value-only line if it has text and doesn't look like another label
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip()
                        i += 1  # skip the next line too
                if lab in preserved and not preserved[lab]:
                    preserved[lab] = val
                break
        if not matched:
            keep.append(line)
        i += 1

    # Trim extra blank lines at the very top of remaining content
    while keep and keep[0].strip() == "":
        keep.pop(0)

    block = [
        f"Company: {company or ''}",
        f"First: {preserved['First']}",
        f"Email: {email or ''}",
        f"Hook: {preserved['Hook']}",
        f"Variant: {preserved['Variant']}",
        f"Website: {website or ''}",
    ]

    # Assemble new description: header block + a blank line + rest
    new_desc = "\n".join(block + ([""] if keep else []) + keep)
    return new_desc

def update_card_header(card_id, company, email, website):
    desc_old = trello_get_card_desc(card_id)
    desc_new = normalize_header_block(desc_old, company, email, website)
    if desc_new == desc_old:
        return False
    r = SESS.put(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": desc_new},
                 timeout=30)
    r.raise_for_status()
    return True

def find_empty_template_cards(list_id, max_needed=1):
    r = SESS.get(f"https://api.trello.com/1/lists/{list_id}/cards",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
                 timeout=30)
    r.raise_for_status()
    cards = r.json()
    empties = []
    for c in cards:
        desc = c.get("desc") or ""
        # empty company (line exists but no value on same line or next value-only line)
        if re.search(r"(?mi)^\s*Company\s*:\s*$", desc):
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
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    # 1) Pick city & fetch candidates
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

        # last resort from OSM email domain
        if not website and biz.get("email"):
            dom = biz["email"].split("@", 1)[-1].lower()
            if dom and dom not in GENERIC_MAIL_PROVIDERS:
                website = f"https://{dom}"

        if not website:
            continue  # need a site to scrape contact

        contact = crawl_contact(website)
        email = contact.get("email") or ""

        if not email or "@" not in email:
            continue

        # one more website fallback from email domain
        if not website and "@" in email:
            dom = email.split("@", 1)[-1].lower()
            if dom and dom not in GENERIC_MAIL_PROVIDERS:
                website = f"https://{dom}"

        leads.append({
            "Company": biz["business_name"],
            "Email": email,
            "Website": website or ""
        })
        _sleep()

    # 2) Push to Trello — one per minute so Butler can move/duplicate
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
        changed = update_card_header(
            card_id=card_id,
            company=lead["Company"],
            email=lead["Email"],
            website=lead["Website"],
        )
        if changed:
            pushed += 1
            print(f"[{pushed}/{DAILY_LIMIT}] Filled card: {lead['Company']} — {lead['Email']} — {lead['Website']}")
            time.sleep(PUSH_INTERVAL_S)
        else:
            print("Card unchanged; trying next lead.")

    print(f"Done. Leads pushed: {pushed}/{len(leads)}")

if __name__ == "__main__":
    main()