# realestate_to_trello.py
# Fill Trello template cards with Company / Email / Website.
# Header is normalized at the top as:
# Company, First, Email, Hook, Variant, Website (First/Hook/Variant preserved).

import os, re, json, time, random
from datetime import date
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser

# ---- optional local .env ----
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ========= Config =========
DAILY_LIMIT     = int(os.getenv("DAILY_LIMIT", "10"))
PUSH_INTERVAL_S = int(os.getenv("PUSH_INTERVAL_S", "60"))     # 1 per minute
REQUEST_DELAY_S = float(os.getenv("REQUEST_DELAY_S", "1.0"))
QUALITY_MIN     = float(os.getenv("QUALITY_MIN", "2.5"))      # set 3.0 for stricter
SEEN_FILE       = os.getenv("SEEN_FILE", "seen_domains.txt")

# Optional behavior
REQUIRE_EXPLICIT_EMAIL = os.getenv("REQUIRE_EXPLICIT_EMAIL","0") == "1"  # skip info@ fallback
ADD_SIGNALS_NOTE       = os.getenv("ADD_SIGNALS_NOTE","0") == "1"        # append one-liner

NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # optional

FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")             # website discovery

# Official/registry sources (optional but recommended)
USE_COMPANIES_HOUSE = os.getenv("USE_COMPANIES_HOUSE", "0") == "1"  # UK
CH_API_KEY          = os.getenv("CH_API_KEY")

USE_SIRENE          = os.getenv("USE_SIRENE", "0") == "1"           # France (OAuth2)
SIRENE_KEY          = os.getenv("SIRENE_KEY")       # INSEE client_id
SIRENE_SECRET       = os.getenv("SIRENE_SECRET")    # INSEE client_secret

USE_OPENCORP        = os.getenv("USE_OPENCORP", "0") == "1"         # US/CA/DE
OPENCORP_API_KEY    = os.getenv("OPENCORP_API_KEY")                 # optional

USE_ZEFIX           = os.getenv("USE_ZEFIX", "0") == "1"            # Switzerland (no key)

# ======== HTTP session ========
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

OSM_FILTERS = [('office','estate_agent'), ('shop','estate_agent')]
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# Countries/cities rotation
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

# ========= small utils =========
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

def email_domain(email: str) -> str:
    try:
        return email.split("@", 1)[1].lower().strip()
    except Exception:
        return ""

def etld1_from_url(u: str) -> str:
    try:
        ex = tldextract.extract(u or "")
        if ex.domain:
            return f"{ex.domain}.{ex.suffix}" if ex.suffix else ex.domain
    except Exception:
        pass
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

def extract_emails(text):
    return list(set(m.group(0) for m in EMAIL_RE.finditer(text or "")))

# ---- Quality + hygiene helpers ----
import dns.resolver
try:
    import whois as pywhois
except Exception:
    pywhois = None

EDITORIAL_PREFS = [
    "marketing", "content", "editor", "editorial", "press", "media",
    "owner", "ceo", "md", "sales", "hello", "contact"
]

def looks_parked(html: str) -> bool:
    hay = (html or "").lower()
    red_flags = [
        "this domain is for sale", "coming soon", "sedo", "godaddy",
        "namecheap", "parking", "parked domain"
    ]
    return any(p in hay for p in red_flags)

def domain_has_mx(domain: str) -> bool:
    if not domain: return False
    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=5.0)
        return bool(answers)
    except Exception:
        return False

def domain_has_dmarc(domain: str) -> bool:
    if not domain: return False
    try:
        name = f"_dmarc.{domain}"
        answers = dns.resolver.resolve(name, "TXT", lifetime=5.0)
        for r in answers:
            txt = b"".join(r.strings).decode("utf-8", "ignore").lower()
            if txt.startswith("v=dmarc"):
                return True
    except Exception:
        return False

def domain_age_years(domain: str) -> float:
    """Rough domain age in years. Returns 0 if unknown."""
    if not domain or not pywhois:
        return 0.0
    try:
        w = pywhois.whois(domain)
        cd = w.creation_date
        if isinstance(cd, list): cd = cd[0]
        if not cd: return 0.0
        return max(0.0, (time.time() - cd.timestamp()) / (365.25*24*3600))
    except Exception:
        return 0.0

def has_listings_signals(soup: BeautifulSoup) -> bool:
    needles = [
        "for sale", "for rent", "to let", "buy", "sell", "rent",
        "listings", "properties", "our properties", "immobili", "immobilier",
        "angebote", "objekte"
    ]
    txt = soup.get_text(" ").lower()
    if any(n in txt for n in needles):
        return True
    for a in soup.find_all("a", href=True):
        h = a.get("href","").lower()
        if any(x in h for x in ["/listings", "/properties", "/property", "/immobili", "/angebote"]):
            return True
    return False

def has_recent_content(soup: BeautifulSoup, max_days=365) -> bool:
    import datetime as dt
    text = soup.get_text(" ")
    patterns = [
        r"(20[2-9][0-9])",
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}",
        r"\b\d{1,2}[/-]\d{1,2}[/-](20[2-9][0-9])"
    ]
    now = dt.datetime.utcnow()
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            s = m.group(0)
            try:
                if re.fullmatch(r"20[2-9][0-9]", s):
                    d = dt.datetime(int(s), 12, 31)
                elif re.match(r"[A-Za-z]{3,}\s+\d{4}", s):
                    parts = s.split()
                    y = int(parts[-1]); mon = parts[0][:3].title()
                    month_num = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].index(mon)+1
                    d = dt.datetime(y, month_num, 1)
                else:
                    y = int(re.findall(r"\d+", s)[-1])
                    d = dt.datetime(y, 1, 1)
                if (now - d).days <= max_days:
                    return True
            except Exception:
                continue
    return False

def count_team_members(soup: BeautifulSoup) -> int:
    sels = "[class*='team'],[class*='agent'],[class*='member'],[class*='broker']"
    blocks = soup.select(sels)
    count = 0
    for b in blocks:
        txt = b.get_text(" ").lower()
        if any(k in txt for k in ["agent", "broker", "team", "associate", "advisor"]):
            count += 1
    return count

def choose_best_email(emails):
    if not emails: return None
    bad = ("noreply","no-reply","donotreply","do-not-reply")
    def score(e):
        local = e.split("@")[0].lower()
        if any(b in local for b in bad):
            return (999, 1, len(local))
        pref = 0
        for i, p in enumerate(EDITORIAL_PREFS):
            if local.startswith(p): pref = 100 - i; break
        penalty = 1 if local.startswith("info") else 0
        return (-(pref), penalty, len(local))
    return sorted(emails, key=score)[0]

def uses_https(url: str) -> bool:
    try:
        return urlparse(url or "").scheme == "https"
    except Exception:
        return False

def rss_recent(soup: BeautifulSoup, html: str, max_days=365) -> bool:
    for l in soup.find_all("link"):
        if (l.get("type") or "").lower() in ("application/rss+xml","application/atom+xml"):
            return True
    if has_recent_content(soup, max_days=max_days):
        return True
    return False

def quality_score(website: str, html: str, soup: BeautifulSoup, email: str) -> float:
    """
    0..5 score. Keep >= QUALITY_MIN (default 2.5). Consider setting 3.0 for stricter.
    """
    score = 0.0
    if not looks_parked(html): score += 1.0
    if has_listings_signals(soup): score += 1.0
    if has_recent_content(soup, max_days=365): score += 0.7
    site_dom = etld1_from_url(website)
    mail_dom = email_domain(email)
    if site_dom and mail_dom == site_dom: score += 0.7
    if domain_has_mx(mail_dom or site_dom): score += 0.6
    age = domain_age_years(site_dom)
    if age >= 1.0: score += 0.4
    tm = count_team_members(soup)
    if tm >= 3: score += 0.3
    # New minor signals
    if domain_has_dmarc(mail_dom or site_dom): score += 0.3
    if uses_https(website): score += 0.2
    if rss_recent(soup, html, max_days=365): score += 0.3
    return min(score, 5.0)

def summarize_signals(q, website, email, soup):
    bits = []
    if has_listings_signals(soup): bits.append("listings")
    if has_recent_content(soup, 365): bits.append("recent-content")
    dom = email_domain(email) or etld1_from_url(website)
    if domain_has_mx(dom): bits.append("mx")
    if domain_has_dmarc(dom): bits.append("dmarc")
    if uses_https(website): bits.append("https")
    tm = count_team_members(soup)
    if tm >= 3: bits.append(f"team~{tm}")
    return f"Signals: q={q:.2f}; " + ", ".join(bits)

# ========= Geo & OSM =========
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

# ========= Foursquare website finder =========
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

# ========= Crawl contact page for email =========
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

def crawl_contact(site_url):
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
            out["email"] = choose_best_email(list(emails))
            break

        _sleep()

    if not out["email"]:
        dom = domain_from_url(site_url)
        if dom: out["email"] = f"info@{dom}"
    return out

# ========= Official / registry sources =========
def uk_companies_house():
    if not (USE_COMPANIES_HOUSE and CH_API_KEY):
        return []
    url = "https://api.company-information.service.gov.uk/advanced-search/companies"
    try:
        r = requests.get(url, params={"sic_codes":"68310", "size":50}, auth=(CH_API_KEY, ""), timeout=30)
        if r.status_code != 200: return []
        data = r.json()
        items = data.get("items") or []
        out = []
        for it in items:
            nm = (it.get("company_name") or "").strip()
            if nm:
                out.append({"business_name": nm, "website": None, "email": None})
        random.shuffle(out)
        return out
    except Exception:
        return []

# --- SIRENE OAuth2 token cache ---
_SIRENE_TOKEN_CACHE = {"token": None, "expires_at": 0}
def sirene_get_token():
    """Fetch & cache a SIRENE OAuth2 token using client_credentials."""
    if not (SIRENE_KEY and SIRENE_SECRET):
        return None
    if _SIRENE_TOKEN_CACHE["token"] and time.time() < _SIRENE_TOKEN_CACHE["expires_at"] - 60:
        return _SIRENE_TOKEN_CACHE["token"]
    r = requests.post(
        "https://api.insee.fr/token",
        data={"grant_type": "client_credentials"},
        auth=(SIRENE_KEY, SIRENE_SECRET),
        timeout=30,
    )
    r.raise_for_status()
    js = r.json()
    tok = js["access_token"]
    ttl = js.get("expires_in", 3600)
    _SIRENE_TOKEN_CACHE["token"] = tok
    _SIRENE_TOKEN_CACHE["expires_at"] = time.time() + ttl
    return tok

def fr_sirene(city=None):
    """INSEE SIRENE V3, NAF 68.31Z. Returns names; website via FSQ later."""
    if not USE_SIRENE:
        return []
    tok = sirene_get_token()
    if not tok:
        return []
    url = "https://api.insee.fr/entreprises/sirene/V3/siret"
    headers = {"Authorization": f"Bearer {tok}"}
    q = 'activitePrincipaleUniteLegale:"68.31Z"'
    if city:
        q += f' AND libelleCommuneEtablissement:"{city}"'
    try:
        r = requests.get(url, headers=headers, params={"q": q, "nombre": 50}, timeout=30)
        if r.status_code != 200:
            return []
        data = r.json()
        etabs = (data.get("etablissements") or [])
        out = []
        for e in etabs:
            ul = e.get("uniteLegale") or {}
            nm = (ul.get("denominationUniteLegale") or ul.get("nomUniteLegale") or "").strip()
            if not nm:
                nm = (e.get("periodesEtablissement") or [{}])[-1].get("enseigne1Etablissement") or ""
                nm = nm.strip()
            if nm:
                out.append({"business_name": nm, "website": None, "email": None})
        random.shuffle(out)
        return out
    except Exception:
        return []

def opencorp_search(country_code):
    """
    OpenCorporates search without requiring an API key.
    If OPENCORP_API_KEY is present, we include it (higher quota); otherwise unauthenticated.
    Handles 429 (rate-limited) with small backoff.
    """
    if not USE_OPENCORP:
        return []

    url = "https://api.opencorporates.com/v0.4/companies/search"

    queries = [
        'real estate',
        'realtor OR brokerage',
        'immobilier',   # FR/CA-QC
        'immobilien',   # DE/AT/CH
    ]
    q = random.choice(queries)

    params = {"q": q, "country_code": country_code, "per_page": 40, "order": "score"}
    if OPENCORP_API_KEY:
        params["api_token"] = OPENCORP_API_KEY

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
        except Exception:
            return []
        if r.status_code == 429:
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code != 200:
            return []
        try:
            data = r.json()
        except Exception:
            return []
        results = (data.get("results") or {}).get("companies") or []
        out = []
        for c in results:
            co = c.get("company") or {}
            nm = (co.get("name") or "").strip()
            if nm:
                out.append({"business_name": nm, "website": None, "email": None})
        # de-dupe by name
        seen, uniq = set(), []
        for x in out:
            k = x["business_name"].lower()
            if k not in seen:
                uniq.append(x); seen.add(k)
        random.shuffle(uniq)
        return uniq

    return []

def ch_zefix():
    """
    ZEFIX (Swiss central company index) — name search; no API key required.
    Returns names only; website via FSQ later.
    """
    if not USE_ZEFIX:
        return []
    terms = ["immobilien", "real estate", "immobilier", "agenzia immobiliare", "makler"]
    langs = ["de","fr","it","en"]
    out = []
    try:
        for term in terms:
            for lang in langs:
                try:
                    r = requests.get(
                        "https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                        params={"name": term, "maxEntries": 50, "language": lang},
                        timeout=30,
                    )
                    if r.status_code != 200:
                        r = requests.get(
                            "https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                            params={"queryString": term, "maxEntries": 50, "language": lang},
                            timeout=30,
                        )
                        if r.status_code != 200:
                            continue
                    data = r.json()
                    items = data if isinstance(data, list) else data.get("list") or data.get("items") or []
                    for it in items:
                        nm = (it.get("name") or it.get("companyName") or it.get("firmName") or "").strip()
                        if nm:
                            out.append({"business_name": nm, "website": None, "email": None})
                    if len(out) >= 50:
                        break
                except Exception:
                    continue
            if len(out) >= 50:
                break
    except Exception:
        return []
    # dedupe by lowercase name
    seen, uniq = set(), []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen:
            uniq.append(x); seen.add(k)
    random.shuffle(uniq)
    return uniq

def official_sources(city, country, lat, lon):
    out = []
    try:
        if country in ("United Kingdom",):
            out += uk_companies_house()
        elif country in ("France",):
            out += fr_sirene(city)
        elif country in ("United States",):
            out += opencorp_search("us")
        elif country in ("Canada",):
            out += opencorp_search("ca")
        elif country in ("Germany",):
            out += opencorp_search("de")
        elif country in ("Switzerland",):
            out += ch_zefix()
    except Exception:
        pass
    # unique by name
    seen = set(); uniq = []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen:
            uniq.append(x); seen.add(k)
    return uniq

# ========= Trello: header normalization =========
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
    Build header block at top in fixed order.
    Preserve existing First/Hook/Variant values; keep the rest of the description.
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
                if not val and (i + 1) < len(lines):
                    nxt = lines[i + 1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip()
                        i += 1
                if lab in preserved and not preserved[lab]:
                    preserved[lab] = val
                break
        if not matched:
            keep.append(line)
        i += 1

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
    new_desc = "\n".join(block + ([""] if keep else []) + keep)
    return new_desc

def update_card_header(card_id, company, email, website):
    desc_old = trello_get_card_desc(card_id)
    # keep email aligned with website domain when possible
    site_dom = etld1_from_url(website)
    if site_dom and (not email_domain(email) or email_domain(email) != site_dom):
        email = f"info@{site_dom}"
    desc_new = normalize_header_block(desc_old, company, email, website)
    if desc_new == desc_old:
        return False
    r = SESS.put(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": desc_new},
                 timeout=30)
    r.raise_for_status()
    return True

def append_note(card_id, note):
    if not note: return
    desc = trello_get_card_desc(card_id)
    if "Signals:" in desc:
        return
    new_desc = desc + ("\n\n" if not desc.endswith("\n") else "\n") + note
    r = SESS.put(f"https://api.trello.com/1/cards/{card_id}",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": new_desc},
                 timeout=30)
    r.raise_for_status()

def find_empty_template_cards(list_id, max_needed=1):
    r = SESS.get(f"https://api.trello.com/1/lists/{list_id}/cards",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
                 timeout=30)
    r.raise_for_status()
    cards = r.json()
    empties = []
    for c in cards:
        desc = c.get("desc") or ""
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

# ---- dedupe helpers ----
def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(l.strip().lower() for l in f if l.strip())
    except Exception:
        return set()

def save_seen(seen):
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            for d in sorted(seen):
                f.write(d+"\n")
    except Exception:
        pass

# ========= Main =========
def main():
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    city, country = pick_today_city()
    south, west, north, east = geocode_city(city, country)
    lat = (south + north) / 2.0
    lon = (west + east) / 2.0

    leads = []
    seen = load_seen()

    # 0) Official sources first (country-specific)
    off = official_sources(city, country, lat, lon)
    for biz in off:
        if len(leads) >= DAILY_LIMIT: break

        website = biz.get("website")
        if not website:
            website = fsq_find_website(biz["business_name"], lat, lon)
        if not website:
            continue

        site_dom = etld1_from_url(website)
        if site_dom in seen:
            continue

        # robots + homepage fetch
        if not allowed_by_robots(website, "/"):
            continue
        try:
            home = fetch(website)
        except Exception:
            continue
        soup_home = BeautifulSoup(home.text, "html.parser")

        contact = crawl_contact(website)
        email = (contact.get("email") or "").strip()

        # align/clean email
        if not email or "@" not in email:
            if site_dom:
                email = f"info@{site_dom}"
        if email_domain(email) in GENERIC_MAIL_PROVIDERS and site_dom:
            email = f"info@{site_dom}"
        if "@" in email and not domain_has_mx(email_domain(email)):
            if site_dom and domain_has_mx(site_dom):
                email = f"info@{site_dom}"
            else:
                continue

        # Optional: require explicit on-site email (no info@ fallback)
        if REQUIRE_EXPLICIT_EMAIL and 'info@' in email.lower():
            continue

        # Quality filter
        q = quality_score(website, home.text, soup_home, email)
        if q < QUALITY_MIN:
            continue

        leads.append({
            "Company": biz["business_name"],
            "Email": email,
            "Website": website,
            "q": q,
            "signals": summarize_signals(q, website, email, soup_home)
        })
        seen.add(site_dom)
        _sleep()

    # 1) If still short, use OSM around the city
    if len(leads) < DAILY_LIMIT:
        candidates = overpass_estate_agents((south, west, north, east))
        for biz in candidates:
            if len(leads) >= DAILY_LIMIT: break

            website = biz.get("website")
            if not website:
                website = fsq_find_website(biz["business_name"], lat, lon)
            if not website and biz.get("email"):
                dom = email_domain(biz["email"])
                if dom and dom not in GENERIC_MAIL_PROVIDERS:
                    website = f"https://{dom}"
            if not website:
                continue

            site_dom = etld1_from_url(website)
            if site_dom in seen:
                continue

            if not allowed_by_robots(website, "/"):
                continue
            try:
                home = fetch(website)
            except Exception:
                continue
            soup_home = BeautifulSoup(home.text, "html.parser")

            contact = crawl_contact(website)
            email = (contact.get("email") or "").strip()
            if not email or "@" not in email:
                if site_dom:
                    email = f"info@{site_dom}"
            if email_domain(email) in GENERIC_MAIL_PROVIDERS and site_dom:
                email = f"info@{site_dom}"
            if "@" in email and not domain_has_mx(email_domain(email)):
                if site_dom and domain_has_mx(site_dom):
                    email = f"info@{site_dom}"
                else:
                    continue

            if REQUIRE_EXPLICIT_EMAIL and 'info@' in email.lower():
                continue

            q = quality_score(website, home.text, soup_home, email)
            if q < QUALITY_MIN:
                continue

            leads.append({
                "Company": biz["business_name"],
                "Email": email,
                "Website": website,
                "q": q,
                "signals": summarize_signals(q, website, email, soup_home)
            })
            seen.add(site_dom)
            _sleep()

    # keep best-scoring leads
    if leads:
        leads.sort(key=lambda x: x.get("q", 0), reverse=True)
        leads = leads[:DAILY_LIMIT]

    # save dedupe file (persist via commit/cache in workflow if you want cross-day dedupe)
    save_seen(seen)

    # 2) Push to Trello — one per minute
    pushed = 0
    for lead in leads:
        empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties and TRELLO_TEMPLATE_CARD_ID:
            clone_template_into_list(TRELLO_TEMPLATE_CARD_ID, TRELLO_LIST_ID)
            empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties:
            print("No empty template card available; skipping.")
            continue

        changed = update_card_header(
            card_id=empties[0],
            company=lead["Company"],
            email=lead["Email"],
            website=lead["Website"],
        )
        if changed:
            pushed += 1
            print(f"[{pushed}/{DAILY_LIMIT}] q={lead.get('q',0):.2f} — {lead['Company']} — {lead['Email']} — {lead['Website']}")
            if ADD_SIGNALS_NOTE:
                append_note(empties[0], lead.get("signals",""))
            time.sleep(PUSH_INTERVAL_S)
        else:
            print("Card unchanged; trying next lead.")

    print(f"Done. Leads pushed: {pushed}/{len(leads)} (city={city}, country={country})")

if __name__ == "__main__":
    main()
