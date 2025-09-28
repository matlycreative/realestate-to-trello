# realestate_to_trello.py
# Fills Trello template cards with Company / Email / Website (1/min).
# Header order is enforced and other fields are preserved:
# Company, First, Email, Hook, Variant, Website
#
# Now also renames the card TITLE to the Company value.
# + Fixes:
#   1) mkdir-safe seen file writes
#   2) extract_label_value() to read Website from the card
#   3) always persist domain to seen file (even if card unchanged)

import os, re, json, time, random, csv, pathlib
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser
from functools import lru_cache

# ---------- optional local .env ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- env helpers ----------
def env_int(name, default):
    v = (os.getenv(name) or "").strip()
    try:
        return int(v)
    except Exception:
        return int(default)

def env_float(name, default):
    v = (os.getenv(name) or "").strip()
    try:
        return float(v)
    except Exception:
        return float(default)

def env_on(name, default=False):
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1","true","yes","on"): return True
    if v in ("0","false","no","off"): return False
    return bool(default)

# ---------- config ----------
DAILY_LIMIT      = env_int("DAILY_LIMIT", 10)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 60)     # 1/min
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 1.0)
QUALITY_MIN      = env_float("QUALITY_MIN", 3.0)
SEEN_FILE        = os.getenv("SEEN_FILE", ".data/seen_domains.txt")  # unified default path

# extra grace so Butler can move/duplicate after each push
BUTLER_GRACE_S   = 20

# behavior / quality
REQUIRE_EXPLICIT_EMAIL  = env_on("REQUIRE_EXPLICIT_EMAIL", False)
ADD_SIGNALS_NOTE        = env_on("ADD_SIGNALS_NOTE", False)
SKIP_GENERIC_EMAILS     = env_on("SKIP_GENERIC_EMAILS", False)
REQUIRE_BUSINESS_DOMAIN = env_on("REQUIRE_BUSINESS_DOMAIN", False)
ALLOW_FREEMAIL          = env_on("ALLOW_FREEMAIL", True)
FREEMAIL_EXTRA_Q        = env_float("FREEMAIL_EXTRA_Q", 0.3)

# debug / performance
DEBUG      = env_on("DEBUG", False)
USE_WHOIS  = env_on("USE_WHOIS", False)

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

STATS = {
    "off_candidates": 0, "osm_candidates": 0,
    "skip_no_website": 0, "skip_dupe_domain": 0, "skip_robots": 0, "skip_fetch": 0,
    "skip_no_email": 0, "skip_freemail_reject": 0, "skip_generic_local": 0,
    "skip_mx": 0, "skip_explicit_required": 0, "skip_quality": 0
}
def dbg(msg):
    if DEBUG: print(msg)

# country / city
COUNTRY_WHITELIST = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
CITY_MODE     = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY    = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS     = 5  # try up to N cities per run

NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

# Trello
TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # used only if PRECLONE=1

# Discovery (Foursquare v3 = single API Key)
FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")

# Official sources
USE_COMPANIES_HOUSE = env_on("USE_COMPANIES_HOUSE", False); CH_API_KEY = os.getenv("CH_API_KEY")
USE_SIRENE          = env_on("USE_SIRENE", False); SIRENE_KEY = os.getenv("SIRENE_KEY"); SIRENE_SECRET = os.getenv("SIRENE_SECRET")
USE_OPENCORP        = env_on("USE_OPENCORP", False); OPENCORP_API_KEY = os.getenv("OPENCORP_API_KEY")
USE_ZEFIX           = env_on("USE_ZEFIX", False)

# ---------- HTTP ----------
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

# add retries for robustness — GET only (avoid retrying POST to Trello)
try:
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
except TypeError:
    # older urllib3
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=frozenset({"GET"}),
    )
SESS.mount("https://", HTTPAdapter(max_retries=_retries))
SESS.mount("http://", HTTPAdapter(max_retries=_retries))

OSM_FILTERS = [('office','estate_agent'), ('shop','estate_agent')]
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

# freemail detection
FREEMAIL_PATTERNS = [
    r"gmail\.com$", r"googlemail\.com$",
    r"outlook\.com$", r"hotmail\.com$", r"live\.com$",
    r"outlook\.[a-z]{2,}$", r"hotmail\.[a-z]{2,}$", r"live\.[a-z]{2,}$",
    r"yahoo\.com$", r"yahoo\.[a-z]{2,}$",
    r"icloud\.com$", r"proton\.me$", r"protonmail\.com$",
    r"aol\.com$", r"gmx\.(?:com|net|de|at|ch)$",
    r"web\.de$", r"yandex\.(?:ru|com|ua|kz|by)$", r"mail\.ru$"
]
def is_freemail(domain: str) -> bool:
    d = (domain or "").lower()
    return any(re.search(p, d) for p in FREEMAIL_PATTERNS)

# cities
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
    # UAE
    ("Dubai","United Arab Emirates"),
    # Indonesia
    ("Jakarta","Indonesia"), ("Surabaya","Indonesia"), ("Bandung","Indonesia"), ("Denpasar","Indonesia"),
    # Canada
    ("Toronto","Canada"), ("Vancouver","Canada"), ("Montreal","Canada"), ("Calgary","Canada"), ("Ottawa","Canada"),
]

GENERIC_MAILBOX_PREFIXES = {
    "info","contact","hello","support","service","sales","office","admin",
    "enquiries","inquiries","booking","mail","team","general","kundenservice"
}
def is_generic_mailbox_local(local: str) -> bool:
    if not local: return True
    L = local.lower()
    if L in ("noreply","no-reply","donotreply","do-not-reply"): return True
    return any(L.startswith(p) for p in GENERIC_MAILBOX_PREFIXES)

EDITORIAL_PREFS = ["marketing","content","editor","editorial","press","media","owner","ceo","md","sales","hello","contact"]

def _sleep(): time.sleep(REQUEST_DELAY_S)

def iter_cities():
    pool = CITY_ROTATION[:]
    # case-insensitive whitelist
    if COUNTRY_WHITELIST:
        wl = {c.lower() for c in COUNTRY_WHITELIST}
        pool = [c for c in pool if c[1].lower() in wl]
    if FORCE_COUNTRY:
        pool = [c for c in pool if c[1].lower() == FORCE_COUNTRY.lower()]
    if FORCE_CITY:
        pool = [c for c in pool if c[0].lower() == FORCE_CITY.lower()]
    if not pool:
        pool = CITY_ROTATION
    if CITY_MODE.lower() == "random":
        random.shuffle(pool)
        for c in pool[:CITY_HOPS]:
            yield c
    else:
        start = date.today().toordinal() % len(pool)
        hops = min(CITY_HOPS, len(pool))
        for i in range(hops):
            yield pool[(start + i) % len(pool)]

# ---------- utils ----------
def normalize_url(u):
    if not u: return None
    u = u.strip()
    if u.startswith("mailto:"): return None
    p = urlparse(u)
    if not p.scheme:
        u = "https://" + u.strip("/")
    return u

def etld1_from_url(u: str) -> str:
    try:
        ex = tldextract.extract(u or "")
        if ex.domain:
            return f"{ex.domain}.{ex.suffix}" if ex.suffix else ex.domain
    except Exception:
        pass
    return ""

def email_domain(email: str) -> str:
    try:
        return email.split("@", 1)[1].lower().strip()
    except Exception:
        return ""

@lru_cache(maxsize=4096)
def _allowed_by_robots_cached(base: str, path: str, ua: str) -> bool:
    # Fetch robots.txt with timeout and parse lines to avoid rp.read() blocking
    rp = robotparser.RobotFileParser()
    try:
        resp = SESS.get(urljoin(base, "/robots.txt"), timeout=10)
        rp.parse(resp.text.splitlines())
    except Exception:
        return True  # default allow on failure
    return rp.can_fetch(ua, urljoin(base, path))

def allowed_by_robots(base_url, path="/"):
    try:
        p = urlparse(base_url)
        base = f"{p.scheme}://{p.netloc}"
        return _allowed_by_robots_cached(base, path or "/", UA)
    except Exception:
        return True

def fetch(url):
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return r

def extract_emails(text):
    return list(set(m.group(0) for m in EMAIL_RE.finditer(text or "")))

# ---------- quality helpers ----------
try:
    import dns.resolver as _dnsresolver
except Exception:
    _dnsresolver = None

HAS_DNS = _dnsresolver is not None  # <— gate DNS-dependent behavior

try:
    import whois as pywhois
except Exception:
    pywhois = None

def looks_parked(html: str) -> bool:
    hay = (html or "").lower()
    # dropped plain "parking" to reduce false positives
    red_flags = ["this domain is for sale","coming soon","sedo","godaddy","namecheap","parked domain"]
    return any(p in hay for p in red_flags)

def _idna(domain: str) -> str:
    try:
        return (domain or "").encode("idna").decode("ascii")
    except Exception:
        return domain or ""

@lru_cache(maxsize=4096)
def domain_has_mx(domain: str):
    """Return True/False if DNS is available; otherwise None (unknown)."""
    if not HAS_DNS:
        return None
    d = _idna(domain)
    if not d:
        return False
    try:
        _dnsresolver.resolve(d, 'MX', lifetime=5.0)
        return True
    except Exception:
        return False

@lru_cache(maxsize=4096)
def domain_has_dmarc(domain: str):
    """Return True/False if DNS is available; otherwise None (unknown)."""
    if not HAS_DNS:
        return None
    d = _idna(domain)
    if not d:
        return False
    try:
        for r in _dnsresolver.resolve(f"_dmarc.{d}", "TXT", lifetime=5.0):
            txts = getattr(r, "strings", None)
            if txts:
                txt = b"".join(txts).decode("utf-8", "ignore").lower()
            else:
                t = r.to_text()
                if t.startswith('"') and t.endswith('"'):
                    t = t[1:-1]
                txt = t.replace('" "', "").replace('"', "").lower()
            if txt.strip().startswith("v=dmarc"):
                return True
    except Exception:
        return False

def domain_age_years(domain: str) -> float:
    if not domain or not USE_WHOIS or not pywhois:
        return 0.0
    try:
        w = pywhois.whois(domain)
        cd = w.creation_date
        if isinstance(cd, list): cd = cd[0]
        if not cd: return 0.0
        if isinstance(cd, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y", "%Y.%m.%d %H:%M:%S"):
                try:
                    cd = datetime.strptime(cd, fmt)
                    break
                except Exception:
                    pass
            if isinstance(cd, str):  # still str
                return 0.0
        from datetime import date as _date
        if isinstance(cd, _date) and not isinstance(cd, datetime):
            cd = datetime(cd.year, cd.month, cd.day)
        return max(0.0, (time.time() - cd.timestamp()) / (365.25*24*3600))
    except Exception:
        return 0.0

def has_listings_signals(soup: BeautifulSoup) -> bool:
    needles = ["for sale","for rent","to let","buy","sell","rent","listings","properties",
               "our properties","immobili","immobilier","angebote","objekte"]
    txt = soup.get_text(" ").lower()
    if any(n in txt for n in needles): return True
    for a in soup.find_all("a", href=True):
        h = a.get("href","").lower()
        if any(x in h for x in ["/listings","/properties","/property","/immobili","/angebote"]):
            return True
    return False

def has_recent_content(soup: BeautifulSoup, max_days=365) -> bool:
    import datetime as dt
    text = soup.get_text(" ")
    patterns = [r"(20[2-9][0-9])",
                r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}",
                r"\b\d{1,2}[/-]\d{1,2}[/-](20[2-9][0-9])"]
    now = dt.datetime.utcnow()
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            s = m.group(0)
            try:
                if re.fullmatch(r"20[2-9][0-9]", s): d = dt.datetime(int(s), 12, 31)
                elif re.match(r"[A-Za-z]{3,}\s+\d{4}", s):
                    parts = s.split(); y = int(parts[-1]); mon = parts[0][:3].title()
                    month_num = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].index(mon)+1
                    d = dt.datetime(y, month_num, 1)
                else:
                    y = int(re.findall(r"\d+", s)[-1]); d = dt.datetime(y,1,1)
                if (now - d).days <= max_days: return True
            except Exception:
                continue
    return False

def count_team_members(soup: BeautifulSoup) -> int:
    blocks = soup.select("[class*='team'],[class*='agent'],[class*='member'],[class*='broker']")
    return sum(1 for b in blocks if any(k in b.get_text(" ").lower() for k in ["agent","broker","team","associate","advisor"]))

def choose_best_email(emails):
    if not emails: return None
    cleaned = []
    for e in emails:
        if "@" not in e: continue
        local, dom = e.split("@", 1)
        dom = dom.lower()
        if not ALLOW_FREEMAIL and is_freemail(dom):
            continue
        if SKIP_GENERIC_EMAILS and is_generic_mailbox_local(local):
            continue
        cleaned.append(f"{local}@{dom}")
    if SKIP_GENERIC_EMAILS and not cleaned:
        return None
    pool = cleaned or emails
    bad = ("noreply","no-reply","donotreply","do-not-reply")
    def score(e):
        local = e.split("@",1)[0].lower()
        pref=0
        for i,p in enumerate(EDITORIAL_PREFS):
            if local.startswith(p): pref = 100 - i; break
        penalty = 10 if local.startswith("info") else 0
        person_bonus = 1 if (("." in local) or ("-" in local)) else 0
        if any(b in local for b in bad): return (999,1,0)
        return (-(pref), penalty, -person_bonus, len(local))
    return sorted(pool, key=score)[0]

def uses_https(url: str) -> bool:
    return (urlparse(url or "").scheme == "https")

def rss_recent(soup: BeautifulSoup, max_days=365) -> bool:
    for l in soup.find_all("link"):
        if (l.get("type") or "").lower() in ("application/rss+xml","application/atom+xml"):
            return True
    return has_recent_content(soup, max_days=max_days)

def quality_score(website: str, html: str, soup: BeautifulSoup, email: str) -> float:
    score = 0.0
    if not looks_parked(html): score += 1.0
    if has_listings_signals(soup): score += 1.0
    if has_recent_content(soup, 365): score += 0.7
    site_dom = etld1_from_url(website); mail_dom = email_domain(email)

    # DNS-gated signals
    mx_signal    = (domain_has_mx(mail_dom or site_dom) is True) if HAS_DNS else False
    dmarc_signal = (domain_has_dmarc(mail_dom or site_dom) is True) if HAS_DNS else False

    if site_dom and mail_dom == site_dom: score += 0.7
    if mx_signal: score += 0.6
    age = domain_age_years(site_dom)
    if age >= 1.0: score += 0.4
    if count_team_members(soup) >= 3: score += 0.3
    if dmarc_signal: score += 0.3
    if uses_https(website): score += 0.2
    if rss_recent(soup, 365): score += 0.3
    return min(score, 5.0)

def summarize_signals(q, website, email, soup):
    bits = []
    if has_listings_signals(soup): bits.append("listings")
    if has_recent_content(soup, 365): bits.append("recent-content")
    dom = email_domain(email) or etld1_from_url(website)
    if HAS_DNS and (domain_has_mx(dom) is True): bits.append("mx")
    if HAS_DNS and (domain_has_dmarc(dom) is True): bits.append("dmarc")
    if uses_https(website): bits.append("https")
    tm = count_team_members(soup)
    if tm >= 3: bits.append(f"team~{tm}")
    return f"Signals: q={q:.2f}; " + ", ".join(bits)

# helper for MX decision (gates on DNS availability)
def _mx_ok(domain: str) -> bool:
    return True if not HAS_DNS else (domain_has_mx(domain) is True)

# ---------- geo & OSM ----------
def geocode_city(city, country):
    r = SESS.get("https://nominatim.openstreetmap.org/search",
        params={"q": f"{city}, {country}", "format":"json", "limit":1},
        headers={"Referer":"https://nominatim.org"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data: raise RuntimeError(f"Nominatim couldn't find {city}, {country}")
    south, north, west, east = map(float, data[0]["boundingbox"])
    return south, west, north, east

def overpass_estate_agents(bbox):
    south, west, north, east = bbox
    parts = []
    for k,v in OSM_FILTERS:
        for t in ("node","way","relation"):
            parts.append(f'{t}["{k}"="{v}"]({south},{west},{north},{east});')
    q = f"""[out:json][timeout:25];({ ' '.join(parts) });out tags center;"""

    js = None
    for url in ("https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter"):
        try:
            r = SESS.post(url, data=q.encode("utf-8"), timeout=60)
            if r.status_code == 200:
                js = r.json()
                break
        except Exception:
            continue
    if js is None:
        return []

    rows = []
    for el in js.get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name")
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        email = tags.get("email") or tags.get("contact:email")
        if name:
            rows.append({"business_name": name.strip(),
                         "website": normalize_url(website) if website else None,
                         "email": email})
    dedup = {}
    for r0 in rows:
        key = (r0["business_name"].lower(), etld1_from_url(r0["website"] or ""))
        if key not in dedup: dedup[key] = r0
    out = list(dedup.values()); random.shuffle(out)
    return out

# ---------- Foursquare website finder (v3) ----------
def fsq_find_website(name, lat, lon):
    if not FOURSQUARE_API_KEY: return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept":"application/json"}
    try:
        params = {"query": name, "ll": f"{lat},{lon}", "limit": 1, "radius": 50000}
        r = SESS.get("https://api.foursquare.com/v3/places/search",
                     headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            results = (r.json().get("results") or [])
            if results:
                first = results[0]
                website = first.get("website")
                if website: return normalize_url(website)
                fsq_id = first.get("fsq_id")
                if fsq_id:
                    d = SESS.get(f"https://api.foursquare.com/v3/places/{fsq_id}",
                                 headers=headers, params={"fields":"website"}, timeout=20)
                    if d.status_code == 200:
                        w = d.json().get("website")
                        if w: return normalize_url(w)
    except Exception:
        return None
    return None

# ---------- contact crawl ----------
def cf_decode(hexstr: str) -> str:
    try:
        b = bytes.fromhex(hexstr); key = b[0]
        return ''.join(chr(c ^ key) for c in b[1:])
    except Exception:
        return ""

def gather_candidate_pages(base):
    pages = [base]
    common = ["/contact","/contact-us","/about","/about-us","/who-we-are","/our-story",
              "/team","/our-team","/agents","/our-agents","/brokers","/staff",
              "/impressum","/kontakt","/ueber-uns","/uber-uns","/equipe","/equipo"]
    for p in common: pages.append(urljoin(base, p))
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
            STATS["skip_fetch"] += 1
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        emails = set(extract_emails(resp.text))

        # mailto: links
        for a in soup.select('a[href^="mailto:"]'):
            href = a.get("href", "")
            m = EMAIL_RE.search(href)
            if m:
                emails.add(m.group(0))

        # Cloudflare-protected emails
        for sp in soup.select("span.__cf_email__, [data-cfemail]"):
            enc = sp.get("data-cfemail")
            dec = cf_decode(enc) if enc else ""
            if dec and EMAIL_RE.search(dec):
                emails.add(dec)

        if emails:
            chosen = choose_best_email(list(emails))
            if chosen:
                out["email"] = chosen
                break

        _sleep()

    # fallback only if allowed (not strict explicit + not skipping generic)
    if not out["email"] and not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
        dom = etld1_from_url(site_url)
        if dom:
            out["email"] = f"info@{dom}"

    return out

# ---------- official / registry ----------
def uk_companies_house():
    if not (USE_COMPANIES_HOUSE and CH_API_KEY): return []
    url = "https://api.company-information.service.gov.uk/advanced-search/companies"
    try:
        r = SESS.get(url, params={"sic_codes":"68310", "size":50}, auth=(CH_API_KEY, ""), timeout=30)
        if r.status_code != 200: return []
        items = r.json().get("items") or []
        out = []
        for it in items:
            nm = (it.get("company_name") or "").strip()
            if nm: out.append({"business_name": nm, "website": None, "email": None})
        random.shuffle(out); return out
    except Exception:
        return []

_SIRENE_TOKEN_CACHE = {"token": None, "expires_at": 0}
def sirene_get_token():
    if not (SIRENE_KEY and SIRENE_SECRET): return None
    if _SIRENE_TOKEN_CACHE["token"] and time.time() < _SIRENE_TOKEN_CACHE["expires_at"] - 60:
        return _SIRENE_TOKEN_CACHE["token"]
    r = SESS.post("https://api.insee.fr/token",
        data={"grant_type": "client_credentials"}, auth=(SIRENE_KEY, SIRENE_SECRET), timeout=30)
    r.raise_for_status()
    js = r.json()
    _SIRENE_TOKEN_CACHE["token"] = js["access_token"]
    _SIRENE_TOKEN_CACHE["expires_at"] = time.time() + js.get("expires_in", 3600)
    return _SIRENE_TOKEN_CACHE["token"]

def fr_sirene(city=None):
    if not USE_SIRENE: return []
    tok = sirene_get_token()
    if not tok: return []
    url = "https://api.insee.fr/entreprises/sirene/V3/siret"
    headers = {"Authorization": f"Bearer {tok}"}
    q = 'activitePrincipaleUniteLegale:"68.31Z"'
    if city: q += f' AND libelleCommuneEtablissement:"{city}"'
    try:
        r = SESS.get(url, headers=headers, params={"q": q, "nombre": 50}, timeout=30)
        if r.status_code != 200: return []
        etabs = (r.json().get("etablissements") or [])
        out = []
        for e in etabs:
            ul = e.get("uniteLegale") or {}
            nm = (ul.get("denominationUniteLegale") or ul.get("nomUniteLegale") or "").strip()
            if not nm:
                nm = (e.get("periodesEtablissement") or [{}])[-1].get("enseigne1Etablissement") or ""
                nm = nm.strip()
            if nm: out.append({"business_name": nm, "website": None, "email": None})
        random.shuffle(out); return out
    except Exception:
        return []

def opencorp_search(country_code):
    if not USE_OPENCORP: return []
    url = "https://api.opencorporates.com/v0.4/companies/search"
    queries = ['real estate','realtor OR brokerage','immobilier','immobilien']
    q = random.choice(queries)
    params = {"q": q, "country_code": country_code, "per_page": 40, "order": "score"}
    if OPENCORP_API_KEY: params["api_token"] = OPENCORP_API_KEY
    for attempt in range(3):
        try:
            r = SESS.get(url, params=params, timeout=30)
        except Exception:
            return []
        if r.status_code == 429:
            time.sleep(3*(attempt+1)); continue
        if r.status_code != 200: return []
        results = (r.json().get("results") or {}).get("companies") or []
        out = []
        for c in results:
            nm = (c.get("company") or {}).get("name") or ""
            nm = nm.strip()
            if nm: out.append({"business_name": nm, "website": None, "email": None})
        seen, uniq = set(), []
        for x in out:
            k = x["business_name"].lower()
            if k not in seen:
                uniq.append(x); seen.add(k)
        random.shuffle(uniq); return uniq
    return []

def ch_zefix():
    if not USE_ZEFIX: return []
    terms = ["immobilien","real estate","immobilier","agenzia immobiliare","makler"]
    langs = ["de","fr","it","en"]; out = []
    try:
        for term in terms:
            for lang in langs:
                try:
                    r = SESS.get("https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                                 params={"name": term, "maxEntries": 50, "language": lang}, timeout=30)
                    if r.status_code != 200:
                        r = SESS.get("https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                                     params={"queryString": term, "maxEntries": 50, "language": lang}, timeout=30)
                        if r.status_code != 200: continue
                    data = r.json()
                    items = data if isinstance(data, list) else data.get("list") or data.get("items") or []
                    for it in items:
                        nm = (it.get("name") or it.get("companyName") or it.get("firmName") or "").strip()
                        if nm: out.append({"business_name": nm, "website": None, "email": None})
                    if len(out) >= 50: break
                except Exception:
                    continue
            if len(out) >= 50: break
    except Exception:
        return []
    seen, uniq = set(), []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen: uniq.append(x); seen.add(k)
    random.shuffle(uniq); return uniq

def official_sources(city, country, lat, lon):
    out = []
    try:
        if country in ("United Kingdom",): out += uk_companies_house()
        elif country in ("France",):       out += fr_sirene(city)
        elif country in ("United States",):out += opencorp_search("us")
        elif country in ("Canada",):       out += opencorp_search("ca")
        elif country in ("Germany",):      out += opencorp_search("de")
        elif country in ("Switzerland",):  out += ch_zefix()
    except Exception:
        pass
    seen = set(); uniq = []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen:
            uniq.append(x); seen.add(k)
    return uniq

# ---------- Trello helpers ----------
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}

def trello_get_card(card_id):
    r = SESS.get(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "name,desc"},
        timeout=30,
    )
    r.raise_for_status()
    js = r.json()
    desc = (js.get("desc") or "").replace("\r\n", "\n").replace("\r", "\n")
    name = js.get("name") or ""
    return {"name": name, "desc": desc}

def extract_label_value(desc: str, label: str) -> str:
    """
    Extract the value of a 'Label: value' line from the description.
    Supports the next-line value style too.
    """
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        m = LABEL_RE[label].match(line)
        if m:
            val = (m.group(1) or "").strip()
            if not val and (i + 1) < len(lines):
                nxt = lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    val = nxt.strip()
                    i += 1
            return val
        i += 1
    return ""

def normalize_header_block(desc, company, email, website):
    desc = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = desc.splitlines()
    preserved = {"First": "", "Hook": "", "Variant": ""}
    keep = []; i = 0
    while i < len(lines):
        line = lines[i]; matched = False
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                matched = True
                val = (m.group(1) or "").strip()
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip(); i += 1
                if lab in preserved and not preserved[lab]:
                    preserved[lab] = val
                break
        if not matched:
            keep.append(line)
        i += 1

    while keep and keep[0].strip() == "": keep.pop(0)

    def hard(line: str) -> str:
        return (line or "").rstrip() + "  "

    block_lines = [
        hard(f"Company: {company or ''}"),
        hard(f"First: {preserved['First']}"),
        hard(f"Email: {email or ''}"),
        hard(f"Hook: {preserved['Hook']}"),
        hard(f"Variant: {preserved['Variant']}"),
        hard(f"Website: {website or ''}"),
    ]
    return "\n".join(block_lines + ([""] if keep else []) + keep)

def update_card_header(card_id, company, email, website, new_name=None):
    cur = trello_get_card(card_id)
    desc_old = cur["desc"]
    name_old = cur["name"]

    # If email domain != site domain, normalize to info@site (when allowed)
    site_dom = etld1_from_url(website)
    if site_dom and (not email_domain(email) or email_domain(email) != site_dom):
        if not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
            email = f"info@{site_dom}"

    # Build new header block
    desc_new = normalize_header_block(desc_old, company, email, website)

    # Only send fields that changed
    payload = {}
    if desc_new != desc_old:
        payload["desc"] = desc_new

    desired_name = (new_name or "").strip()
    if desired_name and desired_name != name_old.strip():
        payload["name"] = desired_name

    if not payload:
        return False  # nothing to change

    r = SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, **payload},
        timeout=30,
    )
    r.raise_for_status()
    return True

def append_note(card_id, note):
    if not note: return
    cur = trello_get_card(card_id)
    desc = cur["desc"]
    if "signals:" in desc.lower(): return  # case-insensitive
    new_desc = desc + ("\n\n" if not desc.endswith("\n") else "\n") + note
    SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "desc": new_desc},
        timeout=30
    ).raise_for_status()

def is_template_blank(desc: str) -> bool:
    """
    Consider a card 'blank template' if:
      - it has 'Company:' with nothing after the colon on that visual line, OR
      - it has 'Email:' line with no '@' on that line, OR
      - it contains the header labels (Company/Email/Website) but no '@' anywhere.
    """
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")

    if re.search(r"(?mi)^\s*Company\s*:\s*$", d):
        return True

    for m in LABEL_RE["Email"].finditer(d):
        val = (m.group(1) or "").strip()
        if "@" not in val:
            return True

    dl = d.lower()
    if ("company:" in dl and "email:" in dl and "website:" in dl and "@" not in dl):
        return True

    return False

def find_empty_template_cards(list_id, max_needed=1):
    r = SESS.get(f"https://api.trello.com/1/lists/{list_id}/cards",
                 params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
                 timeout=30)
    r.raise_for_status()
    empties = []
    for c in r.json():
        if is_template_blank(c.get("desc") or ""):
            empties.append(c["id"])
        if len(empties) >= max_needed:
            break
    return empties

def clone_template_into_list(template_card_id, list_id, name="Lead (auto)"):
    if not template_card_id: return None
    r = SESS.post("https://api.trello.com/1/cards",
                  params={"key":TRELLO_KEY,"token":TRELLO_TOKEN,
                          "idList":list_id,"idCardSource":template_card_id,"name":name},
                  timeout=30)
    r.raise_for_status()
    return r.json()["id"]

def ensure_min_blank_templates(list_id, template_id, need):
    """Ensure at least `need` empty template cards exist in the list."""
    if need <= 0 or not template_id: return
    empties = find_empty_template_cards(list_id, max_needed=need)
    missing = max(0, need - len(empties))
    for i in range(missing):
        clone_template_into_list(template_id, list_id, name=f"Lead (auto) {int(time.time())%100000}-{i+1}")
        time.sleep(1.0)  # avoid Trello rate-limits

# --- seen_domains backfill helpers ---
URL_RE = re.compile(r"https?://[^\s)>\]]+", re.I)

def any_url_in_text(text: str) -> str:
    m = URL_RE.search(text or "")
    return m.group(0) if m else ""

def seen_domains(domain: str):
    if not domain:
        return
    d = domain.strip().lower()
    try:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "a", encoding="utf-8") as f:
            f.write(d + "\n")
    except Exception:
        pass

def trello_list_cards_full(list_id: str) -> list:
    r = SESS.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
        timeout=30
    )
    r.raise_for_status()
    js = r.json()
    return js if isinstance(js, list) else []

def domain_from_card_desc(desc: str) -> str:
    # 1) Prefer Website: from header
    website = extract_label_value(desc, "Website")
    if website:
        if not website.lower().startswith(("http://","https://")):
            website = "https://" + website.strip()
        d = etld1_from_url(website)
        if d: return d
    # 2) Any URL present
    url = any_url_in_text(desc)
    if url:
        d = etld1_from_url(url)
        if d: return d
    # 3) Business email domain (skip freemail)
    dom = email_domain_from_text(extract_label_value(desc, "Email") or desc)
    return dom

def backfill_seen_from_list(list_id: str, seen: set) -> int:
    """Scan a list, collect website/email domains, add to 'seen' set."""
    added = 0
    for c in trello_list_cards_full(list_id):
        dom = domain_from_card_desc(c.get("desc") or "")
        if dom and dom not in seen
