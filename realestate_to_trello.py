# realestate_to_trello.py
# Collects real-estate businesses per city and fills Trello template cards:
# Company, First, Email, Hook, Variant, Website
#
# Fixes for "0 leads" due to Overpass timeouts:
#  - IMPORTANT: If Overpass returns a timeout/runtime error remark and NO elements,
#    we DO NOT accept it; we try other Overpass mirrors instead.
#  - Much cheaper Overpass queries:
#      * value-regex instead of dozens/hundreds of ["k"="v"] clauses
#      * (if: ...) for tag existence instead of repeated selectors
#  - Around-radius capped (default max 10km) + fallback to geocodeArea queries
#  - out result caps to avoid gigantic payloads
#
# NOTE: Email crawling is disabled by default in your workflow (Email="").
# This script focuses on finding WEBSITES and pushing to Trello.

import os, re, json, time, random, csv, pathlib, html, math
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, Dict, Any, List, Tuple

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
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return bool(default)

# ---------- CI detection ----------
IS_CI = bool((os.getenv("GITHUB_ACTIONS") or "").strip()) or bool((os.getenv("CI") or "").strip())

# ---------- config ----------
DAILY_LIMIT      = env_int("DAILY_LIMIT", 25)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 20)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.2)
QUALITY_MIN      = env_float("QUALITY_MIN", 1.0)
SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")
BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 10)

# behavior / quality (email crawling disabled by default)
COLLECT_EMAIL           = env_on("COLLECT_EMAIL", 0)
REQUIRE_EXPLICIT_EMAIL  = env_on("REQUIRE_EXPLICIT_EMAIL", 0)
ADD_SIGNALS_NOTE        = env_on("ADD_SIGNALS_NOTE", False)
SKIP_GENERIC_EMAILS     = env_on("SKIP_GENERIC_EMAILS", 0)
REQUIRE_BUSINESS_DOMAIN = env_on("REQUIRE_BUSINESS_DOMAIN", 0)
ALLOW_FREEMAIL          = env_on("ALLOW_FREEMAIL", 1)
FREEMAIL_EXTRA_Q        = env_float("FREEMAIL_EXTRA_Q", 0.3)

# debug / performance
DEBUG      = env_on("DEBUG", False)
USE_WHOIS  = env_on("USE_WHOIS", 0)
VERIFY_MX  = env_on("VERIFY_MX", 0)

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

# OSM/Overpass self-healing knobs
OSM_ADAPTIVE_RADIUS       = env_on("OSM_ADAPTIVE_RADIUS", True)
OSM_MAX_RADIUS_M          = env_int("OSM_MAX_RADIUS_M", 10000)   # IMPORTANT: cap around radius (10km default)
OSM_RADIUS_M              = env_int("OSM_RADIUS_M", 2500)        # base radius
OSM_RADIUS_STEPS          = env_int("OSM_RADIUS_STEPS", 3)       # 2.5km -> 5km -> 10km (default)
OSM_STRICT_MIN_CANDIDATES = env_int("OSM_STRICT_MIN_CANDIDATES", 3)

# Defaults: strict off in CI so you don’t stall on giant overpass unions
OSM_REQUIRE_DIRECT_CONTACT = env_on("OSM_REQUIRE_DIRECT_CONTACT", (not IS_CI))
OSM_ALLOW_NAME_FALLBACK    = env_on("OSM_ALLOW_NAME_FALLBACK", IS_CI)

# Safety cap for huge cities (parse + slice)
OSM_MAX_CANDIDATES = env_int("OSM_MAX_CANDIDATES", 400)

# Overpass timeouts
OVERPASS_TIMEOUT_S        = env_int("OVERPASS_TIMEOUT_S", 70)
OVERPASS_LOOKUP_TIMEOUT_S = env_int("OVERPASS_LOOKUP_TIMEOUT_S", 25)

# Result cap in Overpass output (prevents enormous payloads)
OVERPASS_OUT_LIMIT = env_int("OVERPASS_OUT_LIMIT", 600)

# Nominatim
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

# Trello
TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")

# Discovery (Foursquare v3 key optional)
FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")

# Official sources toggles (optional)
USE_COMPANIES_HOUSE = env_on("USE_COMPANIES_HOUSE", False); CH_API_KEY = os.getenv("CH_API_KEY")
USE_SIRENE          = env_on("USE_SIRENE", False); SIRENE_KEY = os.getenv("SIRENE_KEY"); SIRENE_SECRET = os.getenv("SIRENE_SECRET")
USE_OPENCORP        = env_on("USE_OPENCORP", False); OPENCORP_API_KEY = os.getenv("OPENCORP_API_KEY")
USE_ZEFIX           = env_on("USE_ZEFIX", False)
USE_OFFICIAL_SOURCES = any([USE_COMPANIES_HOUSE, USE_SIRENE, USE_OPENCORP, USE_ZEFIX])

STATS = {
    "off_candidates": 0,
    "osm_candidates": 0,
    "skip_no_website": 0,
    "skip_dupe_domain": 0,
    "skip_robots": 0,
    "skip_fetch": 0,
    "skip_no_email": 0,
    "skip_freemail_reject": 0,
    "skip_generic_local": 0,
    "skip_mx": 0,
    "skip_explicit_required": 0,
    "skip_quality": 0,
    "website_direct": 0,
    "website_overpass_name": 0,
    "website_nominatim": 0,
    "website_fsq": 0,
    "website_wikidata": 0,
}

def dbg(msg: str):
    if DEBUG:
        print(msg, flush=True)

# ---------- country / city ----------
COUNTRY_WHITELIST = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
CITY_MODE     = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY    = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS     = env_int("CITY_HOPS", 8)

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

    hops = min(CITY_HOPS, len(pool))
    if CITY_MODE.lower() == "random":
        random.shuffle(pool)
        for c in pool[:hops]:
            yield c
    else:
        start = random.randint(0, len(pool) - 1)
        for i in range(hops):
            yield pool[(start + i) % len(pool)]

# ---------- HTTP ----------
SESS = requests.Session()
SESS.headers.update({
    "User-Agent": UA,
    "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
})

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

# ---------- throttling ----------
_LAST_CALL: Dict[str, float] = {}

def throttle(key: str, min_interval_s: float):
    now = time.monotonic()
    last = _LAST_CALL.get(key, 0.0)
    wait = (min_interval_s - (now - last))
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[key] = time.monotonic()

def _sleep():
    time.sleep(REQUEST_DELAY_S)

# ---------- utils ----------
def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u or u.startswith("mailto:"):
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

def uses_https(url: str) -> bool:
    return (urlparse(url or "").scheme == "https")

def fetch(url: str) -> requests.Response:
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return r

# ---------- robots (cached per-base) ----------
@lru_cache(maxsize=2048)
def _robots_parser_for_base(base: str) -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    try:
        resp = SESS.get(urljoin(base, "/robots.txt"), timeout=10)
        if resp.status_code != 200:
            rp.parse([])  # allow
            return rp
        rp.parse(resp.text.splitlines())
        return rp
    except Exception:
        rp.parse([])  # allow
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

# ---------- email helpers (mostly unused in your current workflow) ----------
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

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

GENERIC_MAILBOX_PREFIXES = {
    "info","contact","hello","support","service","sales","office","admin",
    "enquiries","inquiries","booking","mail","team","general","kundenservice"
}

def is_generic_mailbox_local(local: str) -> bool:
    if not local:
        return True
    L = local.lower()
    if L in ("noreply", "no-reply", "donotreply", "do-not-reply"):
        return True
    return any(L.startswith(p) for p in GENERIC_MAILBOX_PREFIXES)

# ---------- DNS/W whois optional quality helpers ----------
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

def has_listings_signals(soup: BeautifulSoup) -> bool:
    needles = [
        "for sale","for rent","to let","buy","sell","rent",
        "listings","properties","our properties",
        "immobili","immobilier","angebote","objekte",
        "biens","wohnungen","mieten","kaufen"
    ]
    txt = soup.get_text(" ").lower()
    if any(n in txt for n in needles):
        return True
    for a in soup.find_all("a", href=True):
        h = (a.get("href","") or "").lower()
        if any(x in h for x in ["/listings","/properties","/property","/immobili","/angebote","/objekte","/biens"]):
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

def rss_recent(soup: BeautifulSoup, max_days=365) -> bool:
    for l in soup.find_all("link"):
        if (l.get("type") or "").lower() in ("application/rss+xml","application/atom+xml"):
            return True
    return has_recent_content(soup, max_days=max_days)

def quality_score(website: str, html_text: str, soup: BeautifulSoup, email: str) -> float:
    score = 0.0
    if not looks_parked(html_text):
        score += 1.0
    if has_listings_signals(soup):
        score += 1.0
    if has_recent_content(soup, 365):
        score += 0.7
    if uses_https(website):
        score += 0.2
    if rss_recent(soup, 365):
        score += 0.3
    if count_team_members(soup) >= 3:
        score += 0.3
    return min(score, 5.0)

def summarize_signals(q: float, website: str, email: str, soup: BeautifulSoup) -> str:
    bits = []
    if has_listings_signals(soup):
        bits.append("listings")
    if has_recent_content(soup, 365):
        bits.append("recent-content")
    if uses_https(website):
        bits.append("https")
    tm = count_team_members(soup)
    if tm >= 3:
        bits.append(f"team~{tm}")
    return f"Signals: q={q:.2f}; " + ", ".join(bits)

# ---------- geo ----------
def geocode_city(city: str, country: str) -> Tuple[float, float, float, float]:
    throttle("nominatim", 1.1)
    r = SESS.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": f"{city}, {country}", "format":"json", "limit": 1},
        headers={"Referer":"https://nominatim.org"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"Nominatim couldn't find {city}, {country}")
    south, north, west, east = map(float, data[0]["boundingbox"])
    return south, west, north, east

# ---------- Overpass ----------
_OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
)

def _is_bad_overpass_remark(remark: str) -> bool:
    r = (remark or "").lower()
    # Typical failure remarks that should cause retry/mirror switch
    bad = [
        "runtime error",
        "timed out",
        "timeout",
        "query timed out",
        "out of memory",
        "too many requests",
        "rate limit",
    ]
    return any(b in r for b in bad)

def _overpass_run(q: str, timeout_s: int, purpose: str) -> Optional[dict]:
    """
    Critical behavior:
      - If Overpass returns a runtime-error/timeout remark AND empty elements,
        treat it as failure and try other mirrors.
    """
    max_attempts_per_endpoint = 2 if IS_CI else 3

    for url in _OVERPASS_ENDPOINTS:
        for attempt in range(1, max_attempts_per_endpoint + 1):
            try:
                throttle("overpass", 2.0)
                r = SESS.post(url, data={"data": q}, timeout=timeout_s + 20)

                if r.status_code != 200:
                    snippet = (r.text or "")[:180].replace("\n", " ")
                    print(f"[overpass] HTTP {r.status_code} ({purpose}) via {url} attempt={attempt} snippet='{snippet}'", flush=True)
                    if r.status_code in (429, 500, 502, 503, 504):
                        time.sleep(2.0 * attempt)
                        continue
                    break

                try:
                    js = r.json()
                except Exception:
                    snippet = (r.text or "")[:220].replace("\n", " ")
                    print(f"[overpass] JSON decode failed ({purpose}) via {url} attempt={attempt} snippet='{snippet}'", flush=True)
                    time.sleep(1.25 * attempt)
                    continue

                remark = (js.get("remark") if isinstance(js, dict) else None) or ""
                elems = (js.get("elements", []) if isinstance(js, dict) else []) or []

                if remark:
                    rem = str(remark)[:220].replace("\n", " ")
                    print(f"[overpass] remark ({purpose}) via {url}: '{rem}'", flush=True)

                # BIG FIX:
                # If Overpass says it timed out (or similar) and it returned no data,
                # DO NOT accept it; try a different mirror.
                if elems == [] and remark and _is_bad_overpass_remark(remark):
                    time.sleep(1.0 * attempt)
                    continue

                return js

            except Exception as e:
                print(f"[overpass] error ({purpose}) via {url} attempt={attempt}: {e}", flush=True)
                time.sleep(1.5 * attempt)
                continue

    return None

# ---------- OSM tag filters ----------
OSM_VALUE_VARIANTS = [
    "estate_agent", "estate agent",
    "real_estate", "real estate",
    "real_estate_agent", "real estate agent",
    "property_management", "property management",
    "property_agent", "property agent",
    "letting", "letting_agent", "letting agent",
]

BROAD_NAME_KEYWORDS = [
    "immo", "immobil", "immobilien", "immobilier", "immobili",
    "estate", "real estate", "realestate", "property", "properties",
    "realtor", "broker", "agency", "agence", "makler", "vermittlung",
    "housing", "homes", "wohnungen", "logements", "biens",
]

def _regex_union(words: List[str]) -> str:
    cleaned = []
    for w in words:
        w = (w or "").strip()
        if not w:
            continue
        w = re.escape(w)
        # allow flexible whitespace for multiword tokens
        w = w.replace(r"\ ", r"\s+")
        cleaned.append(w)
    return "(" + "|".join(cleaned) + ")" if cleaned else "(immo)"

def _values_regex(values: List[str]) -> str:
    # anchored regex for OSM tag values, tolerant to whitespace when values contain spaces
    return "^" + _regex_union(values) + "$"

def _build_radii(base_radius_m: int) -> List[int]:
    radii = [max(500, int(base_radius_m))]
    if OSM_ADAPTIVE_RADIUS:
        r = radii[0]
        for _ in range(max(0, OSM_RADIUS_STEPS - 1)):
            r = min(OSM_MAX_RADIUS_M, r * 2)
            if r != radii[-1]:
                radii.append(r)
    return radii

def _parse_overpass_elements(js: dict) -> List[dict]:
    rows = []
    for el in (js.get("elements", []) if isinstance(js, dict) else []):
        tags = el.get("tags", {}) or {}
        name = (tags.get("name") or "").strip()
        if not name:
            continue

        website  = tags.get("website") or tags.get("contact:website") or tags.get("url")
        email    = tags.get("email") or tags.get("contact:email")
        wikidata = tags.get("wikidata")

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        rows.append({
            "business_name": name,
            "website": normalize_url(website) if website else None,
            "email": email,
            "wikidata": wikidata,
            "lat": lat2,
            "lon": lon2,
        })

    # de-dup (name + domain)
    dedup = {}
    for r0 in rows:
        key = (r0["business_name"].lower(), etld1_from_url(r0["website"] or ""))
        if key not in dedup:
            dedup[key] = r0

    out = list(dedup.values())
    random.shuffle(out)
    return out[:max(1, OSM_MAX_CANDIDATES)]

def _query_around_strict(lat: float, lon: float, r_m: int) -> str:
    v_rx = _values_regex(OSM_VALUE_VARIANTS)
    # Use (if: ...) to keep selector count tiny; much cheaper than 150+ clauses.
    # Must include ["name"] to avoid tons of unnamed POIs.
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT_S}];
(
  nwr(around:{r_m},{lat},{lon})["office"~"{v_rx}"]["name"](if:t["website"]||t["contact:website"]||t["url"]||t["email"]||t["contact:email"]);
  nwr(around:{r_m},{lat},{lon})["shop"~"{v_rx}"]["name"](if:t["website"]||t["contact:website"]||t["url"]||t["email"]||t["contact:email"]);
  nwr(around:{r_m},{lat},{lon})["amenity"~"{v_rx}"]["name"](if:t["website"]||t["contact:website"]||t["url"]||t["email"]||t["contact:email"]);
);
out tags center {OVERPASS_OUT_LIMIT};
"""

def _query_around_relaxed(lat: float, lon: float, r_m: int) -> str:
    v_rx = _values_regex(OSM_VALUE_VARIANTS)
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT_S}];
(
  nwr(around:{r_m},{lat},{lon})["office"~"{v_rx}"]["name"];
  nwr(around:{r_m},{lat},{lon})["shop"~"{v_rx}"]["name"];
  nwr(around:{r_m},{lat},{lon})["amenity"~"{v_rx}"]["name"];
);
out tags center {OVERPASS_OUT_LIMIT};
"""

def _query_around_broad(lat: float, lon: float, r_m: int) -> str:
    rx = _regex_union(BROAD_NAME_KEYWORDS)
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT_S}];
(
  nwr(around:{r_m},{lat},{lon})["office"]["name"~"{rx}",i];
  nwr(around:{r_m},{lat},{lon})["shop"]["name"~"{rx}",i];
  nwr(around:{r_m},{lat},{lon})["amenity"]["name"~"{rx}",i];
);
out tags center {OVERPASS_OUT_LIMIT};
"""

def _query_area_relaxed(city: str, country: str) -> str:
    v_rx = _values_regex(OSM_VALUE_VARIANTS)
    place = (f"{city}, {country}").replace('"', '')
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT_S}];
{{{{geocodeArea:{place}}}}}->.a;
(
  nwr(area.a)["office"~"{v_rx}"]["name"];
  nwr(area.a)["shop"~"{v_rx}"]["name"];
  nwr(area.a)["amenity"~"{v_rx}"]["name"];
);
out tags center {OVERPASS_OUT_LIMIT};
"""

def _query_area_broad(city: str, country: str) -> str:
    rx = _regex_union(BROAD_NAME_KEYWORDS)
    place = (f"{city}, {country}").replace('"', '')
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT_S}];
{{{{geocodeArea:{place}}}}}->.a;
(
  nwr(area.a)["office"]["name"~"{rx}",i];
  nwr(area.a)["shop"]["name"~"{rx}",i];
  nwr(area.a)["amenity"]["name"~"{rx}",i];
);
out tags center {OVERPASS_OUT_LIMIT};
"""

def overpass_estate_agents(city: str, country: str, lat: float, lon: float, base_radius_m: int) -> List[dict]:
    radii = _build_radii(base_radius_m)

    # 1) strict around (only if enabled)
    if OSM_REQUIRE_DIRECT_CONTACT:
        for r_m in radii:
            js = _overpass_run(_query_around_strict(lat, lon, r_m), OVERPASS_TIMEOUT_S, purpose=f"strict-around r={r_m}")
            if not js:
                continue
            out = _parse_overpass_elements(js)
            if len(out) >= OSM_STRICT_MIN_CANDIDATES:
                dbg(f"[overpass] strict-around ok r={r_m} -> {len(out)}")
                return out
        print("[overpass] strict-around insufficient; falling back to relaxed.", flush=True)

    # 2) relaxed around
    for r_m in radii:
        js = _overpass_run(_query_around_relaxed(lat, lon, r_m), OVERPASS_TIMEOUT_S, purpose=f"relaxed-around r={r_m}")
        if not js:
            continue
        out = _parse_overpass_elements(js)
        if out:
            dbg(f"[overpass] relaxed-around ok r={r_m} -> {len(out)}")
            return out

    # 3) broad around (still cheap)
    print("[overpass] relaxed-around yielded zero; trying broad-around.", flush=True)
    for r_m in radii:
        js = _overpass_run(_query_around_broad(lat, lon, r_m), OVERPASS_TIMEOUT_S, purpose=f"broad-around r={r_m}")
        if not js:
            continue
        out = _parse_overpass_elements(js)
        if out:
            dbg(f"[overpass] broad-around ok r={r_m} -> {len(out)}")
            return out

    # 4) area fallback (often succeeds for mega cities)
    print("[overpass] around queries failed/empty; trying geocodeArea relaxed.", flush=True)
    js = _overpass_run(_query_area_relaxed(city, country), OVERPASS_TIMEOUT_S, purpose="area-relaxed")
    if js:
        out = _parse_overpass_elements(js)
        if out:
            dbg(f"[overpass] area-relaxed ok -> {len(out)}")
            return out

    print("[overpass] geocodeArea relaxed empty; trying geocodeArea broad.", flush=True)
    js = _overpass_run(_query_area_broad(city, country), OVERPASS_TIMEOUT_S, purpose="area-broad")
    if js:
        out = _parse_overpass_elements(js)
        if out:
            dbg(f"[overpass] area-broad ok -> {len(out)}")
            return out

    return []

# ---------- website resolution helpers ----------
LEGAL_SUFFIXES = [
    "ag","gmbh","sa","sarl","sàrl","llc","ltd","limited","inc","corp","s.p.a","spa","bv","nv",
    "kg","ohg","ug","gbr","kft","sro","s.r.o","oy","ab","as","aps"
]

def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[\u2019'`\".,:;()\-_/\\]+", " ", s)
    parts = [p for p in s.split() if p and p not in LEGAL_SUFFIXES]
    return " ".join(parts)

def _escape_overpass_regex(s: str) -> str:
    return re.escape(s or "")

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 999999.0
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

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
[out:json][timeout:{OVERPASS_LOOKUP_TIMEOUT_S}];
(
  node(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  way(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  relation(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
);
out tags center {min(OVERPASS_OUT_LIMIT, 200)};
"""
    js = _overpass_run(q, OVERPASS_LOOKUP_TIMEOUT_S, purpose="name-lookup")
    if not js:
        return None

    best = None
    best_score = -1e9

    for el in js.get("elements", []):
        tags = el.get("tags", {}) or {}
        nm = (tags.get("name") or "").strip()
        w = tags.get("website") or tags.get("contact:website") or tags.get("url")
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
            params={"q": q, "format": "jsonv2", "limit": limit, "extratags": 1},
            headers={"Referer": "https://nominatim.org"},
            timeout=30,
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

def fsq_find_website(name: str, lat: float, lon: float) -> Optional[str]:
    if not FOURSQUARE_API_KEY:
        return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept": "application/json"}
    try:
        throttle("foursquare", 0.6)
        params = {"query": name, "ll": f"{lat},{lon}", "limit": 1, "radius": 50000}
        r = SESS.get("https://api.foursquare.com/v3/places/search", headers=headers, params=params, timeout=20)
        if r.status_code != 200:
            return None
        results = (r.json().get("results") or [])
        if not results:
            return None
        first = results[0]
        website = first.get("website")
        if website:
            return normalize_url(website)
        fsq_id = first.get("fsq_id")
        if not fsq_id:
            return None
        throttle("foursquare", 0.6)
        d = SESS.get(
            f"https://api.foursquare.com/v3/places/{fsq_id}",
            headers=headers,
            params={"fields": "website"},
            timeout=20,
        )
        if d.status_code == 200:
            w = d.json().get("website")
            if w:
                return normalize_url(w)
    except Exception:
        return None
    return None

def resolve_website(
    biz_name: str,
    city: str,
    country: str,
    lat: float,
    lon: float,
    direct: Optional[str],
    wikidata_qid: Optional[str] = None
) -> Optional[str]:
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

    w = fsq_find_website(biz_name, lat, lon)
    if w:
        STATS["website_fsq"] += 1
        return w

    return None

# ---------- official sources (optional, unchanged) ----------
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
    r = SESS.post(
        "https://api.insee.fr/token",
        data={"grant_type": "client_credentials"},
        auth=(SIRENE_KEY, SIRENE_SECRET),
        timeout=30,
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
    queries = ["real estate", "realtor OR brokerage", "immobilier", "immobilien"]
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
            time.sleep(3 * (attempt + 1))
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
                        timeout=30,
                    )
                    if r.status_code != 200:
                        throttle("zefix", 0.4)
                        r = SESS.get(
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
TARGET_LABELS = ["Company", "First", "Email", "Hook", "Variant", "Website"]
LABEL_RE = {lab: re.compile(rf"(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$") for lab in TARGET_LABELS}

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
    return bool(re.search(r"(?mi)^\s*Company\s*:\s*$", d))

def find_empty_template_cards(list_id, max_needed=1):
    r = SESS.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
        timeout=30,
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
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN,
                "idList": list_id, "idCardSource": template_card_id, "name": name},
        timeout=30,
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

# ---------- dedupe + CSV ----------
def load_seen() -> set:
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

def save_seen(seen: set):
    try:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            for d in sorted(seen):
                f.write(d + "\n")
    except Exception:
        pass

def append_csv(leads: List[dict]):
    if not leads:
        return
    fname = os.getenv("LEADS_CSV", f"leads_{date.today().isoformat()}.csv")
    file_exists = pathlib.Path(fname).exists()
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp", "city", "country", "company", "email", "website", "q"])
        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        for L in leads:
            w.writerow([
                ts,
                L.get("City", ""),
                L.get("Country", ""),
                L["Company"],
                L["Email"],
                L["Website"],
                f'{L.get("q", 0):.2f}',
            ])

# ---------- main ----------
def main():
    if not IS_CI and (not NOMINATIM_EMAIL or "example.com" in NOMINATIM_EMAIL):
        raise SystemExit("NOMINATIM_EMAIL is missing or placeholder. Set it to a real email (local runs).")

    missing = [n for n in ["TRELLO_KEY", "TRELLO_TOKEN", "TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    leads: List[dict] = []
    seen = load_seen()

    for (city, country) in iter_cities():
        print(f"\n=== CITY START: {city}, {country} ===", flush=True)
        t_city = time.time()

        # geocode
        try:
            t_geo = time.time()
            south, west, north, east = geocode_city(city, country)
            lat = (south + north) / 2.0
            lon = (west + east) / 2.0
            print(f"[{city}, {country}] geocode OK -> {lat:.5f},{lon:.5f} (took {time.time()-t_geo:.1f}s)", flush=True)
        except Exception as e:
            print(f"[{city}, {country}] geocode FAILED: {e}", flush=True)
            continue

        # official sources (optional)
        off = []
        if USE_OFFICIAL_SOURCES:
            t_off = time.time()
            off = official_sources(city, country, lat, lon)
            STATS["off_candidates"] += len(off)
            print(f"[{city}, {country}] official candidates: {len(off)} (took {time.time()-t_off:.1f}s)", flush=True)
        else:
            print(f"[{city}, {country}] official candidates: 0 (disabled)", flush=True)

        leads_before_city = len(leads)

        # ---- Process official candidates ----
        for i, biz in enumerate(off, start=1):
            if len(leads) >= DAILY_LIMIT:
                break

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
            if site_dom and site_dom in seen:
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
            email = ""

            q = quality_score(website, home.text, soup_home, email)
            if q < QUALITY_MIN:
                STATS["skip_quality"] += 1
                continue

            leads.append({
                "City": city,
                "Country": country,
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

        # ---- OSM fallback ----
        if len(leads) < DAILY_LIMIT:
            t_osm = time.time()
            print(f"[{city}] OSM search starting...", flush=True)

            cands = overpass_estate_agents(city, country, lat, lon, OSM_RADIUS_M)

            STATS["osm_candidates"] += len(cands)
            print(f"[{city}] OSM candidates: {len(cands)} (took {time.time()-t_osm:.1f}s)", flush=True)

            leads_before_osm = len(leads)

            for biz in cands:
                if len(leads) >= DAILY_LIMIT:
                    break

                lat0 = biz.get("lat") or lat
                lon0 = biz.get("lon") or lon

                website = normalize_url(biz.get("website"))

                if not website and OSM_ALLOW_NAME_FALLBACK:
                    website = resolve_website(
                        biz_name=biz["business_name"],
                        city=city,
                        country=country,
                        lat=lat0,
                        lon=lon0,
                        direct=biz.get("website"),
                        wikidata_qid=biz.get("wikidata"),
                    )

                if not website:
                    STATS["skip_no_website"] += 1
                    continue

                site_dom = etld1_from_url(website)
                if site_dom and site_dom in seen:
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
                email = ""

                q = quality_score(website, home.text, soup_home, email)
                if q < QUALITY_MIN:
                    STATS["skip_quality"] += 1
                    continue

                leads.append({
                    "City": city,
                    "Country": country,
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

    if PRECLONE and leads and TRELLO_TEMPLATE_CARD_ID:
        ensure_min_blank_templates(TRELLO_LIST_ID, TRELLO_TEMPLATE_CARD_ID, min(DAILY_LIMIT, len(leads)))

    if leads:
        append_csv(leads)

    def push_one_lead(lead: dict, seen: set) -> bool:
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

        site_dom = etld1_from_url(lead.get("Website") or "")
        if site_dom:
            seen_domains(site_dom)
            seen.add(site_dom)

        if changed:
            print(f"PUSHED ✅ q={lead.get('q',0):.2f} — {lead['Company']} — {lead['Website']}", flush=True)
            if ADD_SIGNALS_NOTE:
                append_note(card_id, lead.get("signals", ""))
        else:
            print(f"UNCHANGED ℹ️ — {lead['Company']}", flush=True)

        return True

    pushed = 0
    for lead in (sorted(leads, key=lambda x: x.get("q", 0), reverse=True) if leads else []):
        if pushed >= DAILY_LIMIT:
            break
        ok = push_one_lead(lead, seen)
        if ok:
            pushed += 1
            time.sleep(max(0, PUSH_INTERVAL_S) + max(0, BUTLER_GRACE_S))

    save_seen(seen)

    print(f"SEEN_FILE path: {os.path.abspath(SEEN_FILE)} — total domains in set: {len(seen)}", flush=True)
    print(f"Done. Leads pushed: {pushed}/{min(len(leads), DAILY_LIMIT)}", flush=True)

if __name__ == "__main__":
    main()
