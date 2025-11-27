# realestate_to_trello.py
# Fills Trello template cards with Company / Email / Website (paced).
# Header order is enforced and other fields are preserved:
# Company, First, Email, Hook, Variant, Website
#
# Also renames the card TITLE to the Company value.
# Fixes included:
#   1) mkdir-safe seen file writes
#   2) extract_label_value() to read Website from the card
#   3) always persist domain to seen file (even if card unchanged)
#   4) immediate append to seen_domains.txt when a lead is discovered and when pushed
#
# Additional hardening:
#   5) Robots caching is per-base (no refetch per path)
#   6) Trello header rewrite only touches the top header block (never nukes body lines)
#   7) Same-site logic uses eTLD+1 so www/non-www works
#   8) Per-service throttles for Nominatim + Overpass (and a few other external calls)

import os, re, json, time, random, csv, pathlib, html, math
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional

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
DAILY_LIMIT      = env_int("DAILY_LIMIT", 25)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 20)     # base pace (you also add BUTLER_GRACE_S)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.2)
QUALITY_MIN      = env_float("QUALITY_MIN", 1.2)
SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")  # root file by default

# extra grace so Butler can move/duplicate after each push
BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 10)

# behavior / quality
REQUIRE_EXPLICIT_EMAIL  = env_on("REQUIRE_EXPLICIT_EMAIL", 0)
ADD_SIGNALS_NOTE        = env_on("ADD_SIGNALS_NOTE", False)
SKIP_GENERIC_EMAILS     = env_on("SKIP_GENERIC_EMAILS", 0)
REQUIRE_BUSINESS_DOMAIN = env_on("REQUIRE_BUSINESS_DOMAIN", 0)
ALLOW_FREEMAIL          = env_on("ALLOW_FREEMAIL", 1)
FREEMAIL_EXTRA_Q        = env_float("FREEMAIL_EXTRA_Q", 0.3)

# debug / performance
DEBUG      = env_on("DEBUG", False)
USE_WHOIS  = env_on("USE_WHOIS", 0)

VERIFY_MX  = env_on("VERIFY_MX", 0)  # default OFF for volume

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

STATS = {
    "off_candidates": 0, "osm_candidates": 0,
    "skip_no_website": 0, "skip_dupe_domain": 0, "skip_robots": 0, "skip_fetch": 0,
    "skip_no_email": 0, "skip_freemail_reject": 0, "skip_generic_local": 0,
    "skip_mx": 0, "skip_explicit_required": 0, "skip_quality": 0
}
STATS.setdefault("website_direct", 0)
STATS.setdefault("website_overpass_name", 0)
STATS.setdefault("website_nominatim", 0)
STATS.setdefault("website_fsq", 0)
STATS.setdefault("website_wikidata", 0)

def dbg(msg):
    if DEBUG:
        print(msg, flush=True)

# country / city
COUNTRY_WHITELIST = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
CITY_MODE     = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY    = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS = env_int("CITY_HOPS", 8)
OSM_RADIUS_M = env_int("OSM_RADIUS_M", 2500)

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

MAX_CONTACT_PAGES = env_int("MAX_CONTACT_PAGES", 1)

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
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=frozenset({"GET"}),
    )
SESS.mount("https://", HTTPAdapter(max_retries=_retries))
SESS.mount("http://", HTTPAdapter(max_retries=_retries))

# ---------- per-service throttling ----------
_LAST_CALL = {}
def throttle(key: str, min_interval_s: float):
    now = time.monotonic()
    last = _LAST_CALL.get(key, 0.0)
    wait = (min_interval_s - (now - last))
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[key] = time.monotonic()

OSM_FILTERS = [
    ("office","estate_agent"),
    ("office","real_estate"),
    ("office","property_management"),
    ("shop","estate_agent"),
    ("shop","real_estate"),
]
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
    ("Zurich","Switzerland"), ("Geneva","Switzerland"), ("Basel","Switzerland"), ("Lausanne","Switzerland"),
    ("London","United Kingdom"), ("Manchester","United Kingdom"), ("Birmingham","United Kingdom"), ("Edinburgh","United Kingdom"),
    ("New York","United States"), ("Los Angeles","United States"), ("Chicago","United States"),
    ("Miami","United States"), ("San Francisco","United States"), ("Dallas","United States"),
    ("Paris","France"), ("Lyon","France"), ("Marseille","France"), ("Toulouse","France"),
    ("Berlin","Germany"), ("Munich","Germany"), ("Hamburg","Germany"), ("Frankfurt","Germany"),
    ("Milan","Italy"), ("Rome","Italy"), ("Naples","Italy"), ("Turin","Italy"),
    ("Oslo","Norway"), ("Bergen","Norway"),
    ("Copenhagen","Denmark"), ("Aarhus","Denmark"),
    ("Vienna","Austria"), ("Salzburg","Austria"), ("Graz","Austria"),
    ("Madrid","Spain"), ("Barcelona","Spain"), ("Valencia","Spain"),
    ("Lisbon","Portugal"), ("Porto","Portugal"),
    ("Amsterdam","Netherlands"), ("Rotterdam","Netherlands"), ("The Hague","Netherlands"),
    ("Brussels","Belgium"), ("Antwerp","Belgium"), ("Ghent","Belgium"),
    ("Luxembourg City","Luxembourg"),
    ("Zagreb","Croatia"), ("Split","Croatia"), ("Rijeka","Croatia"),
    ("Dubai","United Arab Emirates"),
    ("Jakarta","Indonesia"), ("Surabaya","Indonesia"), ("Bandung","Indonesia"), ("Denpasar","Indonesia"),
    ("Toronto","Canada"), ("Vancouver","Canada"), ("Montreal","Canada"), ("Calgary","Canada"), ("Ottawa","Canada"),
]

GENERIC_MAILBOX_PREFIXES = {
    "info","contact","hello","support","service","sales","office","admin",
    "enquiries","inquiries","booking","mail","team","general","kundenservice"
}
def is_generic_mailbox_local(local: str) -> bool:
    if not local:
        return True
    L = local.lower()
    if L in ("noreply","no-reply","donotreply","do-not-reply"):
        return True
    return any(L.startswith(p) for p in GENERIC_MAILBOX_PREFIXES)

EDITORIAL_PREFS = ["marketing","content","editor","editorial","press","media","owner","ceo","md","sales","hello","contact"]

def _sleep():
    time.sleep(REQUEST_DELAY_S)

def iter_cities():
    pool = CITY_ROTATION[:]
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
        start = random.randint(0, len(pool) - 1)
        hops = min(CITY_HOPS, len(pool))
        for i in range(hops):
            yield pool[(start + i) % len(pool)]

# ---------- utils ----------
def normalize_url(u):
    if not u:
        return None
    u = u.strip()
    if u.startswith("mailto:"):
        return None
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

# ---------- robots (cached per-base) ----------
@lru_cache(maxsize=2048)
def _robots_parser_for_base(base: str) -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    try:
        resp = SESS.get(urljoin(base, "/robots.txt"), timeout=10)
        if resp.status_code != 200:
            rp.parse([])  # allow all
            return rp
        rp.parse(resp.text.splitlines())
        return rp
    except Exception:
        rp.parse([])  # allow all
        return rp

def allowed_by_robots(base_url: str, path: str = "/") -> bool:
    try:
        p = urlparse(base_url)
        base = f"{p.scheme}://{p.netloc}"
        path0 = path or "/"
        if not path0.startswith("/"):
            path0 = "/" + path0
        rp = _robots_parser_for_base(base)
        return rp.can_fetch(UA, urljoin(base, path0))
    except Exception:
        return True

def fetch(url):
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return r

OBFUSCATIONS = [
    (r"\s*\[?\s*at\s*\]?\s*", "@"),  (r"\s*\(at\)\s*", "@"),  (r"\s+at\s+", "@"),
    (r"\s*\[?\s*dot\s*\]?\s*", "."), (r"\s*\(dot\)\s*", "."), (r"\s+dot\s+", "."),
]
def extract_emails_loose(text: str):
    t2 = html.unescape(text or "")
    for pat, rep in OBFUSCATIONS:
        t2 = re.sub(pat, rep, t2, flags=re.I)
    t2 = t2.replace("&#64;", "@").replace("&#46;", ".").replace("&#x40;", "@").replace("&#x2e;", ".")
    return list(set(m.group(0) for m in EMAIL_RE.finditer(t2)))

# ---------- quality helpers ----------
try:
    import dns.resolver as _dnsresolver
except Exception:
    _dnsresolver = None

HAS_DNS = _dnsresolver is not None

try:
    import whois as pywhois
except Exception:
    pywhois = None

def looks_parked(html_text: str) -> bool:
    hay = (html_text or "").lower()
    red_flags = ["this domain is for sale","coming soon","sedo","godaddy","namecheap","parked domain"]
    return any(p in hay for p in red_flags)

def _idna(domain: str) -> str:
    try:
        return (domain or "").encode("idna").decode("ascii")
    except Exception:
        return domain or ""

@lru_cache(maxsize=4096)
def domain_has_mx(domain: str):
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
            if isinstance(cd, str):
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
    if any(n in txt for n in needles):
        return True
    for a in soup.find_all("a", href=True):
        h = a.get("href","").lower()
        if any(x in h for x in ["/listings","/properties","/property","/immobili","/angebote"]):
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
                    y = int(parts[-1])
                    mon = parts[0][:3].title()
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
    blocks = soup.select("[class*='team'],[class*='agent'],[class*='member'],[class*='broker']")
    return sum(
        1 for b in blocks
        if any(k in b.get_text(" ").lower() for k in ["agent","broker","team","associate","advisor"])
    )

def choose_best_email(emails):
    if not emails:
        return None
    cleaned = []
    for e in emails:
        if "@" not in e:
            continue
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
        pref = 0
        for i, p in enumerate(EDITORIAL_PREFS):
            if local.startswith(p):
                pref = 100 - i
                break
        penalty = 10 if local.startswith("info") else 0
        person_bonus = 1 if (("." in local) or ("-" in local)) else 0
        if any(b in local for b in bad):
            return (999, 1, 0)
        return (-(pref), penalty, -person_bonus, len(local))

    return sorted(pool, key=score)[0]

def uses_https(url: str) -> bool:
    return (urlparse(url or "").scheme == "https")

def rss_recent(soup: BeautifulSoup, max_days=365) -> bool:
    for l in soup.find_all("link"):
        if (l.get("type") or "").lower() in ("application/rss+xml","application/atom+xml"):
            return True
    return has_recent_content(soup, max_days=max_days)

def quality_score(website: str, html_text: str, soup: BeautifulSoup, email: str) -> float:
    score = 0.0
    if not looks_parked(html_text): score += 1.0
    if has_listings_signals(soup): score += 1.0
    if has_recent_content(soup, 365): score += 0.7

    site_dom = etld1_from_url(website)
    mail_dom = email_domain(email)

    mx_signal    = (domain_has_mx(mail_dom or site_dom) is True) if HAS_DNS else False
    dmarc_signal = (domain_has_dmarc(mail_dom or site_dom) is True) if HAS_DNS else False

    if site_dom and mail_dom == site_dom: score += 0.7
    if mx_signal: score += 0.6
    if domain_age_years(site_dom) >= 1.0: score += 0.4
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

def _mx_ok(domain: str) -> bool:
    if not VERIFY_MX:
        return True
    return True if not HAS_DNS else (domain_has_mx(domain) is True)

# ---------- geo & OSM ----------
def geocode_city(city, country):
    throttle("nominatim", 1.1)
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

def overpass_estate_agents(lat: float, lon: float, radius_m: int):
    parts = []
    for k, v in OSM_FILTERS:
        for t in ("node", "way", "relation"):
            parts.append(f'{t}(around:{radius_m},{lat},{lon})["{k}"="{v}"];')
    q = f"""[out:json][timeout:25];({ ' '.join(parts) });out tags center;"""

    js = None
    for url in ("https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter"):
        try:
            throttle("overpass", 2.0)
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
        wikidata = tags.get("wikidata")

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        if name:
            rows.append({
                "business_name": name.strip(),
                "website": normalize_url(website) if website else None,
                "email": email,
                "wikidata": wikidata,
                "lat": lat2,
                "lon": lon2,
            })

    dedup = {}
    for r0 in rows:
        key = (r0["business_name"].lower(), etld1_from_url(r0["website"] or ""))
        if key not in dedup:
            dedup[key] = r0

    out = list(dedup.values())
    random.shuffle(out)
    return out

# ---------- Foursquare website finder (v3) ----------
def fsq_find_website(name, lat, lon):
    if not FOURSQUARE_API_KEY:
        return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept":"application/json"}
    try:
        throttle("foursquare", 0.6)
        params = {"query": name, "ll": f"{lat},{lon}", "limit": 1, "radius": 50000}
        r = SESS.get("https://api.foursquare.com/v3/places/search",
                     headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            results = (r.json().get("results") or [])
            if results:
                first = results[0]
                website = first.get("website")
                if website:
                    return normalize_url(website)
                fsq_id = first.get("fsq_id")
                if fsq_id:
                    throttle("foursquare", 0.6)
                    d = SESS.get(f"https://api.foursquare.com/v3/places/{fsq_id}",
                                 headers=headers, params={"fields":"website"}, timeout=20)
                    if d.status_code == 200:
                        w = d.json().get("website")
                        if w:
                            return normalize_url(w)
    except Exception:
        return None
    return None

LEGAL_SUFFIXES = [
    "ag","gmbh","sa","sarl","sàrl","llc","ltd","limited","inc","corp","s.p.a","spa","bv","nv",
    "kg","ohg","ug","gbr","kft","sro","s.r.o","oy","ab","as","aps"
]

def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[\u2019'`\".,:;()\-_/\\]+", " ", s)
    parts = [p for p in s.split() if p and p not in LEGAL_SUFFIXES]
    return " ".join(parts)

# ✅ FIXED: proper regex escaping for Overpass regex fragments
def _escape_overpass_regex(s: str) -> str:
    return re.sub(r'([.^$*+?{}\[\]\\|()])', r'\\\1', s)
    
def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 999999.0
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2-lat1)
    dl   = math.radians(lon2-lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def overpass_lookup_website_by_name(name: str, lat: float, lon: float, radius_m: int = 20000) -> Optional[str]:
    if not name or lat is None or lon is None:
        return None
    n = _norm_name(name)
    if not n:
        return None

    tokens = [t for t in n.split() if len(t) >= 3] or n.split()
    tokens = tokens[:5]
    pattern = ".*".join(_escape_overpass_regex(t) for t in tokens)

    q = f"""
[out:json][timeout:25];
(
  node(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  way(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  relation(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
);
out tags center;
"""
    js = None
    for url in ("https://overpass-api.de/api/interpreter",
                "https://overpass.kumi.systems/api/interpreter"):
        try:
            throttle("overpass", 2.0)
            r = SESS.post(url, data=q.encode("utf-8"), timeout=60)
            if r.status_code == 200:
                js = r.json()
                break
        except Exception:
            continue
    if not js:
        return None

    best = None
    best_score = -1e9

    for el in js.get("elements", []):
        tags = el.get("tags", {}) or {}
        nm = (tags.get("name") or "").strip()
        w  = tags.get("website") or tags.get("contact:website") or tags.get("url")
        if not w:
            continue

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        dist = _haversine_km(lat, lon, lat2, lon2)
        nm_norm = _norm_name(nm)

        score = 0.0
        if nm_norm == n:
            score += 50
        elif n and n in nm_norm:
            score += 30
        else:
            overlap = len(set(n.split()) & set(nm_norm.split()))
            score += overlap * 6

        score += max(0.0, 20.0 - dist)

        w0 = normalize_url(w)
        dom = etld1_from_url(w0 or "")
        if dom:
            score += 5

        if score > best_score:
            best_score = score
            best = w

    return normalize_url(best) if best else None

def nominatim_lookup_website(name: str, city: str, country: str, limit: int = 5) -> Optional[str]:
    if not name:
        return None
    try:
        throttle("nominatim", 1.1)
        q = f"{name}, {city}, {country}".strip(", ")
        r = SESS.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format":"jsonv2", "limit": limit, "extratags": 1},
            headers={"Referer":"https://nominatim.org"},
            timeout=30
        )
        if r.status_code != 200:
            return None
        items = r.json() or []
        for it in items:
            xt = it.get("extratags") or {}
            w = xt.get("website") or xt.get("contact:website") or xt.get("url")
            w = normalize_url(w)
            if w:
                return w
    except Exception:
        return None
    return None

@lru_cache(maxsize=4096)
def wikidata_website_from_qid(qid: str) -> Optional[str]:
    if not qid or not qid.startswith("Q"):
        return None
    try:
        throttle("wikidata", 0.6)
        r = SESS.get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=20)
        if r.status_code != 200:
            return None
        js = r.json()
        ent = (js.get("entities") or {}).get(qid) or {}
        claims = ent.get("claims") or {}
        for cl in claims.get("P856", []):
            dv = (((cl.get("mainsnak") or {}).get("datavalue") or {}).get("value") or "")
            w = normalize_url(dv)
            if w:
                return w
    except Exception:
        return None
    return None

def resolve_website(biz_name: str, city: str, country: str, lat: float, lon: float,
                    direct: Optional[str], wikidata_qid: Optional[str] = None) -> Optional[str]:
    w = normalize_url(direct)
    if w:
        STATS["website_direct"] += 1
        return w

    w = wikidata_website_from_qid(wikidata_qid or "")
    if w:
        STATS["website_wikidata"] += 1
        return w

    w = overpass_lookup_website_by_name(biz_name, lat, lon, radius_m=20000)
    if w:
        STATS["website_overpass_name"] += 1
        return w

    w = nominatim_lookup_website(biz_name, city, country, limit=5)
    if w:
        STATS["website_nominatim"] += 1
        return w

    w = normalize_url(fsq_find_website(biz_name, lat, lon))
    if w:
        STATS["website_fsq"] += 1
        return w

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
              "/impressum","/kontakt","/ueber-uns","/uber-uns","/equipe","/equipo",
              "/legal","/privacy","/datenschutz","/mentions-legales"]
    for p in common:
        pages.append(urljoin(base, p))
    return pages

CONTACT_KEYWORDS = [
    "contact", "kontakt", "impressum", "about", "ueber", "uber", "who-we-are",
    "team", "agents", "brokers", "staff", "agency", "company", "legal", "privacy",
    "mentions-legales", "equipe", "equipo"
]

def _same_host(url_a: str, url_b: str) -> bool:
    try:
        dom_a = etld1_from_url(url_a)
        dom_b = etld1_from_url(url_b)
        if dom_a and dom_b:
            return dom_a.lower() == dom_b.lower()
        return urlparse(url_a).netloc.lower() == urlparse(url_b).netloc.lower()
    except Exception:
        return False

def _score_contact_link(href: str, text: str) -> int:
    h = (href or "").lower()
    t = (text or "").lower()
    score = 0
    for kw in CONTACT_KEYWORDS:
        if kw in h: score += 8
        if kw in t: score += 5
    score += max(0, 6 - h.count("/"))
    if any(bad in h for bad in ["javascript:", "#", "tel:", "mailto:", ".jpg", ".png", ".pdf"]):
        score -= 50
    return score

def discover_contact_urls_from_home(base_url: str, home_html: str, limit: int = 10) -> list:
    if not home_html:
        return []
    soup = BeautifulSoup(home_html, "html.parser")
    cand = {}
    for a in soup.find_all("a", href=True):
        href = (a.get("href", "") or "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        if not abs_url.lower().startswith(("http://", "https://")):
            continue
        if not _same_host(abs_url, base_url):
            continue
        text = a.get_text(" ", strip=True)
        s = _score_contact_link(abs_url, text)
        if s <= 0:
            continue
        cand[abs_url] = max(cand.get(abs_url, -10**9), s)

    ranked = sorted(cand.items(), key=lambda kv: kv[1], reverse=True)
    return [u for (u, _) in ranked[:limit]]

def discover_sitemap_urls(base_url: str, limit: int = 10) -> list:
    out = []
    try:
        p = urlparse(base_url)
        root = f"{p.scheme}://{p.netloc}"
        rob = SESS.get(urljoin(root, "/robots.txt"), timeout=10)
        if rob.status_code == 200:
            for line in rob.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm:
                        out.append(sm)
    except Exception:
        pass

    try:
        p = urlparse(base_url)
        root = f"{p.scheme}://{p.netloc}"
        out.append(urljoin(root, "/sitemap.xml"))
    except Exception:
        pass

    picked, seen = [], set()
    for sm_url in out:
        try:
            r = SESS.get(sm_url, timeout=15)
            if r.status_code != 200:
                continue
            urls = re.findall(r"<loc>\s*(https?://[^<\s]+)\s*</loc>", r.text, flags=re.I)
            for u in urls:
                ul = u.lower()
                if any(k in ul for k in CONTACT_KEYWORDS):
                    if _same_host(u, base_url) and u not in seen:
                        picked.append(u); seen.add(u)
                        if len(picked) >= limit:
                            return picked
        except Exception:
            continue
    return picked

def crawl_contact(site_url, home_html=None):
    out = {"email": ""}
    if not site_url:
        return out

    candidates = []

    try:
        if not home_html:
            home_html = fetch(site_url).text
        candidates += discover_contact_urls_from_home(site_url, home_html, limit=10)
    except Exception:
        pass

    try:
        candidates += discover_sitemap_urls(site_url, limit=8)
    except Exception:
        pass

    candidates += gather_candidate_pages(site_url)

    seen_u, ordered = set(), []
    for u in candidates:
        if u and u not in seen_u:
            ordered.append(u); seen_u.add(u)

    for idx, url in enumerate(ordered):
        if idx >= MAX_CONTACT_PAGES:
            break

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
        emails = set(extract_emails_loose(resp.text))

        for sc in soup.find_all("script", type=lambda x: (x or "").lower() == "application/ld+json"):
            emails.update(extract_emails_loose(sc.get_text(" ") or ""))

        for a in soup.select('a[href^="mailto:"]'):
            href = a.get("href", "")
            m = EMAIL_RE.search(href)
            if m:
                emails.add(m.group(0))

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

    if not out["email"] and not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
        dom = etld1_from_url(site_url)
        if dom:
            out["email"] = f"info@{dom}"

    return out

# ---------- official / registry ----------
def uk_companies_house():
    if not (USE_COMPANIES_HOUSE and CH_API_KEY):
        return []
    url = "https://api.company-information.service.gov.uk/advanced-search/companies"
    try:
        throttle("companies_house", 0.4)
        r = SESS.get(url, params={"sic_codes":"68310", "size":50}, auth=(CH_API_KEY, ""), timeout=30)
        if r.status_code != 200:
            return []
        items = r.json().get("items") or []
        out = []
        for it in items:
            nm = (it.get("company_name") or "").strip()
            if nm:
                out.append({"business_name": nm, "website": None, "email": None})
        random.shuffle(out)
        return out
    except Exception:
        return []

_SIRENE_TOKEN_CACHE = {"token": None, "expires_at": 0}
def sirene_get_token():
    if not (SIRENE_KEY and SIRENE_SECRET):
        return None
    if _SIRENE_TOKEN_CACHE["token"] and time.time() < _SIRENE_TOKEN_CACHE["expires_at"] - 60:
        return _SIRENE_TOKEN_CACHE["token"]
    throttle("sirene", 0.6)
    r = SESS.post("https://api.insee.fr/token",
        data={"grant_type": "client_credentials"},
        auth=(SIRENE_KEY, SIRENE_SECRET),
        timeout=30
    )
    r.raise_for_status()
    js = r.json()
    _SIRENE_TOKEN_CACHE["token"] = js["access_token"]
    _SIRENE_TOKEN_CACHE["expires_at"] = time.time() + js.get("expires_in", 3600)
    return _SIRENE_TOKEN_CACHE["token"]

def fr_sirene(city=None):
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
        throttle("sirene", 0.6)
        r = SESS.get(url, headers=headers, params={"q": q, "nombre": 50}, timeout=30)
        if r.status_code != 200:
            return []
        etabs = (r.json().get("etablissements") or [])
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
    if not USE_OPENCORP:
        return []
    url = "https://api.opencorporates.com/v0.4/companies/search"
    queries = ['real estate','realtor OR brokerage','immobilier','immobilien']
    q = random.choice(queries)
    params = {"q": q, "country_code": country_code, "per_page": 40, "order": "score"}
    if OPENCORP_API_KEY:
        params["api_token"] = OPENCORP_API_KEY
    for attempt in range(3):
        try:
            throttle("opencorp", 0.6)
            r = SESS.get(url, params=params, timeout=30)
        except Exception:
            return []
        if r.status_code == 429:
            time.sleep(3*(attempt+1))
            continue
        if r.status_code != 200:
            return []
        results = (r.json().get("results") or {}).get("companies") or []
        out = []
        for c in results:
            nm = (c.get("company") or {}).get("name") or ""
            nm = nm.strip()
            if nm:
                out.append({"business_name": nm, "website": None, "email": None})
        seen, uniq = set(), []
        for x in out:
            k = x["business_name"].lower()
            if k not in seen:
                uniq.append(x)
                seen.add(k)
        random.shuffle(uniq)
        return uniq
    return []

def ch_zefix():
    if not USE_ZEFIX:
        return []
    terms = ["immobilien","real estate","immobilier","agenzia immobiliare","makler"]
    langs = ["de","fr","it","en"]
    out = []
    try:
        for term in terms:
            for lang in langs:
                try:
                    throttle("zefix", 0.4)
                    r = SESS.get(
                        "https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                        params={"name": term, "maxEntries": 50, "language": lang},
                        timeout=30
                    )
                    if r.status_code != 200:
                        throttle("zefix", 0.4)
                        r = SESS.get(
                            "https://www.zefix.admin.ch/ZefixPublicREST/api/v1/firm/search.json",
                            params={"queryString": term, "maxEntries": 50, "language": lang},
                            timeout=30
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

    seen, uniq = set(), []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen:
            uniq.append(x)
            seen.add(k)
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

    seen = set()
    uniq = []
    for x in out:
        k = x["business_name"].lower()
        if k not in seen:
            uniq.append(x)
            seen.add(k)
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

def _split_header_rest(desc: str):
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = d.splitlines()

    i = 0
    while i < len(lines) and lines[i].strip() == "":
        i += 1

    if i >= len(lines) or not any(LABEL_RE[lab].match(lines[i]) for lab in TARGET_LABELS):
        return [], lines

    header = []
    seen_labels = set()
    started = False

    while i < len(lines):
        line = lines[i]

        m_lab = None
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                m_lab = lab
                break

        if m_lab:
            started = True
            header.append(line)
            seen_labels.add(m_lab)

            val = (LABEL_RE[m_lab].match(line).group(1) or "").strip()
            if not val and (i + 1) < len(lines):
                nxt = lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    header.append(nxt)
                    i += 1

            i += 1
            continue

        if line.strip() == "":
            header.append(line)
            i += 1
            if "Website" in seen_labels:
                break
            continue

        if started:
            break

        i += 1

    rest = lines[i:]
    return header, rest

def normalize_header_block(desc, company, email, website):
    desc = (desc or "").replace("\r\n", "\n").replace("\r", "\n")

    header_lines, rest_lines = _split_header_rest(desc)
    preserved = {"First": "", "Hook": "", "Variant": ""}

    i = 0
    while i < len(header_lines):
        line = header_lines[i]
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if not m:
                continue
            val = (m.group(1) or "").strip()
            if not val and (i + 1) < len(header_lines):
                nxt = header_lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    val = nxt.strip()
                    i += 1
            if lab in preserved and not preserved[lab]:
                preserved[lab] = val
            break
        i += 1

    def hard(line: str) -> str:
        return (line or "").rstrip() + "  "

    new_header = [
        hard(f"Company: {company or ''}"),
        hard(f"First: {preserved['First']}"),
        hard(f"Email: {email or ''}"),
        hard(f"Hook: {preserved['Hook']}"),
        hard(f"Variant: {preserved['Variant']}"),
        hard(f"Website: {website or ''}"),
        "",
    ]
    return "\n".join(new_header + rest_lines)

def update_card_header(card_id, company, email, website, new_name=None):
    cur = trello_get_card(card_id)
    desc_old = cur["desc"]
    name_old = cur["name"]

    site_dom = etld1_from_url(website)
    if site_dom and (not email or "@" not in email):
        if not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
            email = f"info@{site_dom}"

    desc_new = normalize_header_block(desc_old, company, email, website)

    payload = {}
    if desc_new != desc_old:
        payload["desc"] = desc_new

    desired_name = (new_name or "").strip()
    if desired_name and desired_name != name_old.strip():
        payload["name"] = desired_name

    if not payload:
        return False

    r = SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN},
        data=payload,
        timeout=30,
    )
    r.raise_for_status()
    return True

def append_note(card_id, note):
    if not note:
        return
    cur = trello_get_card(card_id)
    desc = cur["desc"]
    if "signals:" in desc.lower():
        return
    new_desc = desc + ("\n\n" if not desc.endswith("\n") else "\n") + note
    SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN},
        data={"desc": new_desc},
        timeout=30
    ).raise_for_status()

def is_template_blank(desc: str) -> bool:
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
    r = SESS.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
        timeout=30
    )
    r.raise_for_status()
    empties = []
    for c in r.json():
        if is_template_blank(c.get("desc") or ""):
            empties.append(c["id"])
        if len(empties) >= max_needed:
            break
    return empties

def clone_template_into_list(template_card_id, list_id, name="Lead (auto)"):
    if not template_card_id:
        return None
    r = SESS.post(
        "https://api.trello.com/1/cards",
        params={"key":TRELLO_KEY,"token":TRELLO_TOKEN,
                "idList":list_id,"idCardSource":template_card_id,"name":name},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["id"]

def ensure_min_blank_templates(list_id, template_id, need):
    if need <= 0 or not template_id:
        return
    empties = find_empty_template_cards(list_id, max_needed=need)
    missing = max(0, need - len(empties))
    for i in range(missing):
        clone_template_into_list(template_id, list_id, name=f"Lead (auto) {int(time.time())%100000}-{i+1}")
        time.sleep(1.0)

# --- seen_domains backfill helpers ---
URL_RE = re.compile(r"https?://[^\s)>\]]+", re.I)

def any_url_in_text(text: str) -> str:
    m = URL_RE.search(text or "")
    return m.group(0) if m else ""

def email_domain_from_text(text: str) -> str:
    m = EMAIL_RE.search(text or "")
    if not m:
        return ""
    dom = (m.group(0).split("@",1)[1] or "").lower().strip()
    return "" if is_freemail(dom) else dom

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
    website = extract_label_value(desc, "Website")
    if website:
        if not website.lower().startswith(("http://","https://")):
            website = "https://" + website.strip()
        d = etld1_from_url(website)
        if d:
            return d
    url = any_url_in_text(desc)
    if url:
        d = etld1_from_url(url)
        if d:
            return d
    dom = email_domain_from_text(extract_label_value(desc, "Email") or desc)
    return dom

def backfill_seen_from_list(list_id: str, seen: set) -> int:
    added = 0
    for c in trello_list_cards_full(list_id):
        dom = domain_from_card_desc(c.get("desc") or "")
        if dom and dom not in seen:
            seen_domains(dom)
            seen.add(dom)
            added += 1
    return added

# ---------- dedupe + CSV ----------
def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(l.strip().lower() for l in f if l.strip())
    except Exception:
        return set()

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

def save_seen(seen):
    try:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            for d in sorted(seen):
                f.write(d + "\n")
    except Exception:
        pass

def append_csv(leads, city, country):
    if not leads:
        return
    fname = os.getenv("LEADS_CSV", f"leads_{date.today().isoformat()}.csv")
    file_exists = pathlib.Path(fname).exists()
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp","city","country","company","email","website","q"])
        ts = datetime.utcnow().isoformat(timespec="seconds")+"Z"
        for L in leads:
            w.writerow([ts, city, country, L["Company"], L["Email"], L["Website"], f'{L.get("q",0):.2f}'])

# ---------- main ----------
def main():
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    leads = []
    seen = load_seen()
    last_city = ""
    last_country = ""

    for (city, country) in iter_cities():
        print(f"\n=== CITY START: {city}, {country} ===", flush=True)
        t_city = time.time()
        last_city, last_country = city, country

        # --- geocode ---
        try:
            t_geo = time.time()
            south, west, north, east = geocode_city(city, country)
            lat = (south + north) / 2.0
            lon = (west + east) / 2.0
            print(f"[{city}, {country}] geocode OK -> {lat:.5f},{lon:.5f} (took {time.time()-t_geo:.1f}s)", flush=True)
        except Exception as e:
            print(f"[{city}, {country}] geocode FAILED: {e}", flush=True)
            continue

        # --- official sources ---
        t_off = time.time()
        off = official_sources(city, country, lat, lon)
        STATS["off_candidates"] += len(off)
        print(f"[{city}, {country}] official candidates: {len(off)} (took {time.time()-t_off:.1f}s)", flush=True)

        leads_before_city = len(leads)

        for i, biz in enumerate(off, start=1):
            if len(leads) >= DAILY_LIMIT:
                break

            if i % 5 == 0:
                print(f"[{city}] official progress: {i}/{len(off)} | leads={len(leads)}/{DAILY_LIMIT}", flush=True)

            website = resolve_website(
                biz_name=biz["business_name"],
                city=city,
                country=country,
                lat=lat,
                lon=lon,
                direct=biz.get("website"),
                wikidata_qid=biz.get("wikidata"),
            )
            if not website:
                STATS["skip_no_website"] += 1
                continue

            site_dom = etld1_from_url(website)
            if site_dom in seen:
                STATS["skip_dupe_domain"] += 1
                continue

            p = urlparse(website)
            base = f"{p.scheme}://{p.netloc}/"
            if not allowed_by_robots(base, "/"):
                STATS["skip_robots"] += 1
                continue

            try:
                home = fetch(website)
            except Exception:
                STATS["skip_fetch"] += 1
                continue

            soup_home = BeautifulSoup(home.text, "html.parser")
            contact = crawl_contact(website, home.text)
            email = (contact.get("email") or "").strip()

            # email policy
            if is_freemail(email_domain(email)):
                if REQUIRE_BUSINESS_DOMAIN:
                    STATS["skip_freemail_reject"] += 1
                    email = ""
                elif not ALLOW_FREEMAIL:
                    if site_dom and not (SKIP_GENERIC_EMAILS or REQUIRE_EXPLICIT_EMAIL):
                        email = f"info@{site_dom}"
                    else:
                        STATS["skip_freemail_reject"] += 1
                        email = ""

            if email and SKIP_GENERIC_EMAILS and is_generic_mailbox_local(email.split("@",1)[0]):
                STATS["skip_generic_local"] += 1
                email = ""

            if email and REQUIRE_BUSINESS_DOMAIN and email_domain(email) != site_dom:
                STATS["skip_freemail_reject"] += 1
                email = ""

            if email and "@" in email and not _mx_ok(email_domain(email)):
                fallback_ok = (site_dom and _mx_ok(site_dom))
                if not (SKIP_GENERIC_EMAILS or REQUIRE_EXPLICIT_EMAIL) and fallback_ok:
                    email = f"info@{site_dom}"
                else:
                    STATS["skip_mx"] += 1
                    email = ""

            if REQUIRE_EXPLICIT_EMAIL and (not email or "@" not in email):
                STATS["skip_explicit_required"] += 1
                email = ""

            if not email or "@" not in email:
                if site_dom and not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
                    email = f"info@{site_dom}"
                else:
                    STATS["skip_no_email"] += 1
                    continue

            q = quality_score(website, home.text, soup_home, email)
            if ALLOW_FREEMAIL and is_freemail(email_domain(email)) and q < QUALITY_MIN + FREEMAIL_EXTRA_Q:
                STATS["skip_quality"] += 1
                continue
            if q < QUALITY_MIN:
                STATS["skip_quality"] += 1
                continue

            leads.append({
                "Company": biz["business_name"],
                "Email": email,
                "Website": website,
                "q": q,
                "signals": summarize_signals(q, website, email, soup_home),
            })

            if site_dom:
                seen_domains(site_dom)
                seen.add(site_dom)

            _sleep()

        print(f"[{city}] official done: +{len(leads)-leads_before_city} leads", flush=True)

        # --- OSM fallback ---
        if len(leads) < DAILY_LIMIT:
            t_osm = time.time()
            print(f"[{city}] OSM search starting...", flush=True)
            cands = overpass_estate_agents(lat, lon, OSM_RADIUS_M)
            STATS["osm_candidates"] += len(cands)
            print(f"[{city}] OSM candidates: {len(cands)} (took {time.time()-t_osm:.1f}s)", flush=True)

            leads_before_osm = len(leads)

            for j, biz in enumerate(cands, start=1):
                if len(leads) >= DAILY_LIMIT:
                    break

                if j % 25 == 0:
                    print(f"[{city}] OSM progress: {j}/{len(cands)} | leads={len(leads)}/{DAILY_LIMIT}", flush=True)

                lat0 = biz.get("lat") or lat
                lon0 = biz.get("lon") or lon

                website = resolve_website(
                    biz_name=biz["business_name"],
                    city=city,
                    country=country,
                    lat=lat0,
                    lon=lon0,
                    direct=biz.get("website"),
                    wikidata_qid=biz.get("wikidata"),
                )

                if not website and biz.get("email"):
                    dom0 = email_domain(biz["email"])
                    if dom0 and not is_freemail(dom0):
                        website = normalize_url(f"https://{dom0}")

                if not website:
                    STATS["skip_no_website"] += 1
                    continue

                site_dom = etld1_from_url(website)
                if site_dom in seen:
                    STATS["skip_dupe_domain"] += 1
                    continue

                p = urlparse(website)
                base = f"{p.scheme}://{p.netloc}/"
                if not allowed_by_robots(base, "/"):
                    STATS["skip_robots"] += 1
                    continue

                try:
                    home = fetch(website)
                except Exception:
                    STATS["skip_fetch"] += 1
                    continue

                soup_home = BeautifulSoup(home.text, "html.parser")
                contact = crawl_contact(website, home.text)
                email = (contact.get("email") or "").strip()

                if is_freemail(email_domain(email)):
                    if REQUIRE_BUSINESS_DOMAIN:
                        STATS["skip_freemail_reject"] += 1
                        email = ""
                    elif not ALLOW_FREEMAIL:
                        if site_dom and not (SKIP_GENERIC_EMAILS or REQUIRE_EXPLICIT_EMAIL):
                            email = f"info@{site_dom}"
                        else:
                            STATS["skip_freemail_reject"] += 1
                            email = ""

                if email and SKIP_GENERIC_EMAILS and is_generic_mailbox_local(email.split("@",1)[0]):
                    STATS["skip_generic_local"] += 1
                    email = ""

                if email and REQUIRE_BUSINESS_DOMAIN and email_domain(email) != site_dom:
                    STATS["skip_freemail_reject"] += 1
                    email = ""

                if email and "@" in email and not _mx_ok(email_domain(email)):
                    fallback_ok = (site_dom and _mx_ok(site_dom))
                    if not (SKIP_GENERIC_EMAILS or REQUIRE_EXPLICIT_EMAIL) and fallback_ok:
                        email = f"info@{site_dom}"
                    else:
                        STATS["skip_mx"] += 1
                        email = ""

                if REQUIRE_EXPLICIT_EMAIL and (not email or "@" not in email):
                    STATS["skip_explicit_required"] += 1
                    email = ""

                if not email or "@" not in email:
                    if site_dom and not SKIP_GENERIC_EMAILS and not REQUIRE_EXPLICIT_EMAIL:
                        email = f"info@{site_dom}"
                    else:
                        STATS["skip_no_email"] += 1
                        continue

                q = quality_score(website, home.text, soup_home, email)
                if ALLOW_FREEMAIL and is_freemail(email_domain(email)) and q < QUALITY_MIN + FREEMAIL_EXTRA_Q:
                    STATS["skip_quality"] += 1
                    continue
                if q < QUALITY_MIN:
                    STATS["skip_quality"] += 1
                    continue

                leads.append({
                    "Company": biz["business_name"],
                    "Email": email,
                    "Website": website,
                    "q": q,
                    "signals": summarize_signals(q, website, email, soup_home),
                })

                if site_dom:
                    seen_domains(site_dom)
                    seen.add(site_dom)

                _sleep()

            print(f"[{city}] OSM done: +{len(leads)-leads_before_osm} leads", flush=True)

        print(f"=== CITY END: {city} in {time.time()-t_city:.1f}s | total leads={len(leads)}/{DAILY_LIMIT} ===", flush=True)

        if len(leads) >= DAILY_LIMIT:
            break

    if leads:
        leads.sort(key=lambda x: x.get("q", 0), reverse=True)
        leads = leads[:DAILY_LIMIT]

    need = min(DAILY_LIMIT, len(leads))
    if PRECLONE and need > 0 and TRELLO_TEMPLATE_CARD_ID:
        ensure_min_blank_templates(TRELLO_LIST_ID, TRELLO_TEMPLATE_CARD_ID, need)

    if leads and last_city and last_country:
        append_csv(leads, last_city, last_country)

    def push_one_lead(lead: dict, seen: set) -> bool:
        if SKIP_GENERIC_EMAILS and "@" in (lead.get("Email") or ""):
            local = lead["Email"].split("@", 1)[0]
            if is_generic_mailbox_local(local):
                print(f"Skip generic mailbox: {lead['Email']} — {lead['Company']}", flush=True)
                return False

        empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties:
            print("No empty template card available; skipping push.", flush=True)
            return False

        card_id = empties[0]
        changed = update_card_header(
            card_id=card_id,
            company=lead["Company"],
            email=lead["Email"],
            website=lead["Website"],
            new_name=lead["Company"],
        )

        cur = trello_get_card(card_id)
        website_on_card = extract_label_value(cur["desc"], "Website") or (lead.get("Website") or "")
        website_on_card = normalize_url(website_on_card)
        site_dom = etld1_from_url(website_on_card)

        if not site_dom:
            em_dom = email_domain(lead.get("Email") or "")
            if em_dom and not is_freemail(em_dom):
                site_dom = em_dom

        if site_dom:
            seen_domains(site_dom)
            seen.add(site_dom)

        if changed:
            print(f"PUSHED ✅ q={lead.get('q',0):.2f} — {lead['Company']} — {lead['Email']} — {lead['Website']}", flush=True)
            if ADD_SIGNALS_NOTE:
                append_note(card_id, lead.get("signals", ""))
        else:
            print(f"UNCHANGED ℹ️ (still recorded domain) — {lead['Company']}", flush=True)

        return True

    pushed = 0
    if leads:
        leads.sort(key=lambda x: x.get("q", 0), reverse=True)

    for lead in leads:
        if pushed >= DAILY_LIMIT:
            break
        ok = push_one_lead(lead, seen)
        if ok:
            pushed += 1
            time.sleep(max(0, PUSH_INTERVAL_S) + max(0, BUTLER_GRACE_S))

    if DEBUG:
        print("Skip summary:", json.dumps(STATS, indent=2), flush=True)

    list_for_backfill = os.getenv("TRELLO_LIST_ID_SEENSYNC") or TRELLO_LIST_ID
    try:
        added = backfill_seen_from_list(list_for_backfill, seen)
        print(f"Backfill: added {added} domain(s) from list {list_for_backfill}", flush=True)
    except Exception as e:
        print(f"Backfill skipped due to error: {e}", flush=True)

    save_seen(seen)

    print(f"SEEN_FILE path: {os.path.abspath(SEEN_FILE)} — total domains in set: {len(seen)}", flush=True)
    print(f"Done. Leads pushed: {pushed}/{min(len(leads), DAILY_LIMIT)}", flush=True)

if __name__ == "__main__":
    main()
