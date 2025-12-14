# realestate_to_trello.py
# Fills Trello template cards with ONLY:
#   - Company (card title + header field)
#   - Website (header field)
#
# It preserves other fields already on the card:
#   First, Email, Hook, Variant
#
# Reliability fixes (GitHub Actions friendly):
# - Nominatim rate-limit safe: single global throttle key + 429 backoff + email= param
# - Website check: don't discard leads on 403/429 (common on GH Actions); only hard-fail on 404/410
# - DNS fallback: if HTTP fails, accept if domain resolves
# - Robots OFF by default (won't block leads); enable with CHECK_ROBOTS=1
# - tldextract offline: avoid downloading suffix list in CI

import os, re, json, time, random, csv, pathlib, math, socket
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
DAILY_LIMIT      = env_int("DAILY_LIMIT", 15)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 20)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.8)
SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")

# Batch rotation
BATCH_FILE = os.getenv("BATCH_FILE", "batch_state.txt")
BATCH_SLOTS = [
    "a friday",
    "m monday",
    "a monday",
    "m tuesday",
    "a tuesday",
    "m wednesday",
    "a wednesday",
    "m thursday",
    "a thursday",
    "m friday",
]

def load_batch_index() -> int:
    try:
        with open(BATCH_FILE, "r", encoding="utf-8") as f:
            idx = int((f.read() or "").strip())
            if 0 <= idx < len(BATCH_SLOTS):
                return idx
    except Exception:
        pass
    return 0

def save_batch_index(idx: int) -> None:
    try:
        os.makedirs(os.path.dirname(BATCH_FILE) or ".", exist_ok=True)
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            f.write(str(idx))
    except Exception:
        pass

BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 15)
DEBUG            = env_on("DEBUG", False)

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

# Robots toggle (OFF by default)
CHECK_ROBOTS = env_on("CHECK_ROBOTS", False)

# OSM candidates source toggles
OVERPASS_ENABLED             = env_on("OVERPASS_ENABLED", 1)
NOMINATIM_POI_ENABLED        = env_on("NOMINATIM_POI_ENABLED", 1)
OVERPASS_NAME_LOOKUP_ENABLED = env_on("OVERPASS_NAME_LOOKUP_ENABLED", 0)

# Overpass tuning
OVERPASS_TIMEOUT_S      = env_int("OVERPASS_TIMEOUT_S", 45)           # HTTP timeout (client)
OVERPASS_QUERY_TIMEOUT  = env_int("OVERPASS_QUERY_TIMEOUT", 25)       # [timeout:..] inside query
OVERPASS_RETRIES        = env_int("OVERPASS_RETRIES", 2)              # per endpoint
OVERPASS_MIN_INTERVAL_S = env_float("OVERPASS_MIN_INTERVAL_S", 2.0)

# Nominatim POI fallback tuning
NOMINATIM_LIMIT = env_int("NOMINATIM_LIMIT", 60)
NOMINATIM_POI_QUERIES_PER_CITY = env_int("NOMINATIM_POI_QUERIES_PER_CITY", 3)

# Nominatim + UA
NOMINATIM_EMAIL = (os.getenv("NOMINATIM_EMAIL") or "you@example.com").strip()
UA_NOMINATIM    = os.getenv("USER_AGENT", f"MatlyLeads/1.0 (+{NOMINATIM_EMAIL})")

# Website fetch UA (browser-like to reduce mass-403 on CI)
WEB_USER_AGENT = os.getenv(
    "WEB_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Trello
TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # used only if PRECLONE=1

# Discovery (optional)
FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")

# Official sources (optional)
USE_COMPANIES_HOUSE = env_on("USE_COMPANIES_HOUSE", False); CH_API_KEY = os.getenv("CH_API_KEY")
USE_SIRENE          = env_on("USE_SIRENE", False); SIRENE_KEY = os.getenv("SIRENE_KEY"); SIRENE_SECRET = os.getenv("SIRENE_SECRET")
USE_OPENCORP        = env_on("USE_OPENCORP", False); OPENCORP_API_KEY = os.getenv("OPENCORP_API_KEY")
USE_ZEFIX           = env_on("USE_ZEFIX", False)

STATS = {
    "off_candidates": 0, "osm_candidates": 0,
    "skip_no_website": 0, "skip_dupe_domain": 0, "skip_robots": 0, "skip_fetch": 0,
    "cand_overpass": 0, "cand_nominatim_poi": 0,
    "website_direct": 0, "website_overpass_name": 0, "website_nominatim": 0, "website_fsq": 0, "website_wikidata": 0,
    "fetch_ok": 0, "fetch_soft_ok": 0, "fetch_hard_fail": 0, "fetch_dns_ok": 0,
}

def dbg(msg):
    if DEBUG:
        print(msg, flush=True)

# ---------- throttling ----------
_LAST_CALL = {}
def throttle(key: str, min_interval_s: float):
    now = time.monotonic()
    last = _LAST_CALL.get(key, 0.0)
    wait = (min_interval_s - (now - last))
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[key] = time.monotonic()

def _sleep():
    time.sleep(REQUEST_DELAY_S)

# ---------- HTTP ----------
SESS = requests.Session()
SESS.headers.update({
    "User-Agent": UA_NOMINATIM,
    "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"
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

# ---------- tldextract (offline / no suffix-list download in CI) ----------
_TLD_EXTRACT = tldextract.TLDExtract(
    cache_dir=os.path.expanduser("~/.cache/tldextract"),
    suffix_list_urls=None,
)

# ---------- geo / city ----------
COUNTRY_WHITELIST = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
CITY_MODE     = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY    = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS     = env_int("CITY_HOPS", 8)
OSM_RADIUS_M  = env_int("OSM_RADIUS_M", 2500)

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

    if u.lower().startswith("mailto:"):
        return None

    if "@" in u and "://" not in u:
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", u):
            return None

    p = urlparse(u)

    if not p.scheme:
        u = "https://" + u.strip("/")

    p2 = urlparse(u)

    if p2.scheme not in ("http", "https"):
        return None

    if not p2.netloc:
        return None

    if p2.username or p2.password or "@" in p2.netloc:
        return None

    return u

def etld1_from_url(u: str) -> str:
    try:
        ex = _TLD_EXTRACT(u or "")
        if ex.domain:
            return f"{ex.domain}.{ex.suffix}" if ex.suffix else ex.domain
    except Exception:
        pass
    return ""

def _dns_resolves(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False

def fetch_site_ok(url: str) -> bool:
    """
    Website existence check.
    - Hard fail only on 404/410
    - Treat 2xx/3xx as OK
    - Treat 401/403/405/406/429 as SOFT OK (datacenter/bot blocks)
    - If HTTP fails, accept if DNS resolves
    """
    try:
        r = SESS.get(
            url,
            timeout=20,
            headers={
                "User-Agent": WEB_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            allow_redirects=True,
        )
        code = int(r.status_code)

        if code in (404, 410):
            STATS["fetch_hard_fail"] += 1
            return False

        if 200 <= code < 400:
            STATS["fetch_ok"] += 1
            return True

        if code in (401, 403, 405, 406, 429):
            STATS["fetch_soft_ok"] += 1
            return True

        # For everything else (500 etc), fall back to DNS:
        if _dns_resolves(url):
            STATS["fetch_dns_ok"] += 1
            return True

        STATS["fetch_hard_fail"] += 1
        return False

    except Exception:
        if _dns_resolves(url):
            STATS["fetch_dns_ok"] += 1
            return True
        STATS["fetch_hard_fail"] += 1
        return False

# ---------- robots (cached per-base) ----------
@lru_cache(maxsize=2048)
def _robots_parser_for_base(base: str) -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    try:
        resp = SESS.get(
            urljoin(base, "/robots.txt"),
            timeout=10,
            headers={"User-Agent": WEB_USER_AGENT}
        )
        if resp.status_code != 200:
            rp.parse([])  # allow all
            return rp
        rp.parse(resp.text.splitlines())
        return rp
    except Exception:
        rp.parse([])  # allow all
        return rp

def allowed_by_robots(base_url: str, path: str = "/") -> bool:
    if not CHECK_ROBOTS:
        return True
    try:
        p = urlparse(base_url)
        base = f"{p.scheme}://{p.netloc}"
        path0 = path or "/"
        if not path0.startswith("/"):
            path0 = "/" + path0
        rp = _robots_parser_for_base(base)
        return rp.can_fetch(WEB_USER_AGENT, urljoin(base, path0))
    except Exception:
        return True

# ---------- geo ----------
def geocode_city(city, country) -> Tuple[float,float,float,float]:
    for attempt in range(5):
        throttle("nominatim", 1.3)
        r = SESS.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{city}, {country}",
                "format":"json",
                "limit":1,
                "email": NOMINATIM_EMAIL,
            },
            headers={"Referer":"https://nominatim.org"},
            timeout=30
        )

        if r.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue

        r.raise_for_status()
        data = r.json()
        if not data:
            raise RuntimeError(f"Nominatim couldn't find {city}, {country}")
        south, north, west, east = map(float, data[0]["boundingbox"])
        return south, west, north, east

    raise RuntimeError(f"Nominatim rate-limited geocode for {city}, {country}")

# ---------- OSM candidates ----------
OSM_FILTERS = [
    ("office","estate_agent"),
    ("office","real_estate"),
    ("office","property_management"),
    ("shop","estate_agent"),
    ("shop","real_estate"),
]
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

def _overpass_post(query: str) -> Optional[dict]:
    body = (query or "").encode("utf-8")
    for base_url in OVERPASS_ENDPOINTS:
        for attempt in range(1, OVERPASS_RETRIES + 1):
            try:
                throttle("overpass", OVERPASS_MIN_INTERVAL_S)
                r = SESS.post(base_url, data=body, timeout=OVERPASS_TIMEOUT_S)
                if r.status_code == 200:
                    return r.json()
                dbg(f"[overpass] HTTP {r.status_code} via {base_url} attempt={attempt}")
            except Exception as e:
                dbg(f"[overpass] error via {base_url} attempt={attempt}: {e}")
                continue
    return None

def overpass_estate_agents(lat: float, lon: float, radius_m: int) -> List[dict]:
    if not OVERPASS_ENABLED:
        return []
    parts = []
    for k, v in OSM_FILTERS:
        for t in ("node", "way", "relation"):
            parts.append(f'{t}(around:{radius_m},{lat},{lon})["{k}"="{v}"];')
    q = f"""[out:json][timeout:{OVERPASS_QUERY_TIMEOUT}];({ ' '.join(parts) });out tags center;"""

    js = _overpass_post(q)
    if not js:
        return []

    rows = []
    for el in js.get("elements", []):
        tags = el.get("tags", {}) or {}
        name = (tags.get("name") or "").strip()
        if not name:
            continue
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        wikidata = tags.get("wikidata")

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        rows.append({
            "business_name": name,
            "website": normalize_url(website) if website else None,
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
    STATS["cand_overpass"] += len(out)
    return out

def _viewbox_param(south: float, west: float, north: float, east: float) -> str:
    return f"{west},{north},{east},{south}"

def _guess_name_from_nominatim(item: dict) -> str:
    nd = item.get("namedetails") or {}
    nm = (nd.get("name") or item.get("name") or "").strip()
    if nm:
        return nm
    dn = (item.get("display_name") or "").strip()
    if dn and "," in dn:
        return dn.split(",", 1)[0].strip()
    return dn

def _nominatim_poi_queries_for(country: str) -> List[str]:
    c = (country or "").lower().strip()
    base = ["real estate agency", "estate agent", "property management"]
    if c in ("france",):
        base = ["agence immobiliere", "immobilier", "gestion immobiliere"]
    elif c in ("germany", "austria", "switzerland"):
        base = ["immobilienmakler", "immobilien", "hausverwaltung"]
    elif c in ("italy",):
        base = ["agenzia immobiliare", "immobiliare", "gestione immobiliare"]
    elif c in ("spain",):
        base = ["inmobiliaria", "agencia inmobiliaria", "administracion de fincas"]
    elif c in ("portugal",):
        base = ["imobiliaria", "agencia imobiliaria", "gestao imobiliaria"]
    elif c in ("netherlands",):
        base = ["makelaar", "vastgoed", "property management"]
    elif c in ("denmark",):
        base = ["ejendomsmægler", "bolig", "property management"]
    elif c in ("norway",):
        base = ["eiendomsmegler", "eiendom", "property management"]
    return base

def nominatim_poi_candidates(city: str, country: str, south: float, west: float, north: float, east: float) -> List[dict]:
    if not NOMINATIM_POI_ENABLED:
        return []
    vb = _viewbox_param(south, west, north, east)
    queries = _nominatim_poi_queries_for(country)
    random.shuffle(queries)
    queries = queries[:max(1, NOMINATIM_POI_QUERIES_PER_CITY)]

    out: List[dict] = []
    seen_key = set()

    for qstr in queries:
        items = []
        for attempt in range(5):
            try:
                throttle("nominatim", 1.3)
                r = SESS.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q": f"{qstr} {city} {country}",
                        "format": "jsonv2",
                        "limit": NOMINATIM_LIMIT,
                        "viewbox": vb,
                        "bounded": 1,
                        "dedupe": 1,
                        "extratags": 1,
                        "namedetails": 1,
                        "email": NOMINATIM_EMAIL,
                    },
                    headers={"Referer":"https://nominatim.org"},
                    timeout=30,
                )

                if r.status_code == 429:
                    time.sleep(2 * (attempt + 1))
                    continue

                if r.status_code != 200:
                    items = []
                    break

                items = r.json() or []
                break
            except Exception as e:
                dbg(f"[nominatim_poi] error: {e}")
                time.sleep(1.0 + attempt)
                continue

        for it in items:
            nm = _guess_name_from_nominatim(it).strip()
            if not nm:
                continue

            klass = (it.get("class") or "").lower()
            typ = (it.get("type") or "").lower()
            if klass and klass not in ("office", "shop", "amenity", "tourism"):
                continue
            if typ in ("house", "residential", "road", "yes", "city", "town", "village", "suburb", "neighbourhood"):
                continue

            xt = it.get("extratags") or {}
            website = xt.get("website") or xt.get("contact:website") or xt.get("url")

            try:
                lat2 = float(it.get("lat")) if it.get("lat") is not None else None
                lon2 = float(it.get("lon")) if it.get("lon") is not None else None
            except Exception:
                lat2, lon2 = None, None

            website = normalize_url(website) if website else None
            key = (nm.lower(), etld1_from_url(website or "") or f"{lat2},{lon2}")
            if key in seen_key:
                continue
            seen_key.add(key)

            out.append({
                "business_name": nm,
                "website": website,
                "wikidata": xt.get("wikidata"),
                "lat": lat2,
                "lon": lon2,
            })

    random.shuffle(out)
    STATS["cand_nominatim_poi"] += len(out)
    return out

def get_osm_candidates(city: str, country: str, lat: float, lon: float, south: float, west: float, north: float, east: float) -> Tuple[List[dict], str]:
    if OVERPASS_ENABLED:
        for rad in (OSM_RADIUS_M, max(800, OSM_RADIUS_M // 2), max(500, OSM_RADIUS_M // 3)):
            cands = overpass_estate_agents(lat, lon, rad)
            if cands:
                return cands, "overpass"
    if NOMINATIM_POI_ENABLED:
        cands = nominatim_poi_candidates(city, country, south, west, north, east)
        if cands:
            return cands, "nominatim_poi"
    return [], "none"

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
    return re.sub(r'([.^$*+?{}$begin:math:display$$end:math:display$\\|()])', r'\\\1', s)

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
    if not (OVERPASS_ENABLED and OVERPASS_NAME_LOOKUP_ENABLED):
        return None
    if not name or lat is None or lon is None:
        return None
    n = _norm_name(name)
    if not n:
        return None

    tokens = [t for t in n.split() if len(t) >= 3] or n.split()
    tokens = tokens[:5]
    pattern = ".*".join(_escape_overpass_regex(t) for t in tokens)

    q = f"""
[out:json][timeout:{OVERPASS_QUERY_TIMEOUT}];
(
  node(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  way(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  relation(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
);
out tags center;
"""
    js = _overpass_post(q)
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

@lru_cache(maxsize=4096)
def nominatim_lookup_website(name: str, city: str, country: str, limit: int = 8) -> Optional[str]:
    if not name:
        return None

    q = f"{name}, {city}, {country}".strip(", ")

    for attempt in range(5):
        try:
            throttle("nominatim", 1.3)
            r = SESS.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": q,
                    "format":"jsonv2",
                    "limit": limit,
                    "extratags": 1,
                    "namedetails": 1,
                    "email": NOMINATIM_EMAIL,
                },
                headers={"Referer":"https://nominatim.org"},
                timeout=30
            )

            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue

            if r.status_code != 200:
                return None

            items = r.json() or []
            for it in items:
                xt = it.get("extratags") or {}
                w = xt.get("website") or xt.get("contact:website") or xt.get("url")
                w = normalize_url(w)
                if w:
                    return w
            return None
        except Exception:
            time.sleep(1.0 + attempt)
            continue

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

    w = nominatim_lookup_website(biz_name, city, country, limit=8)
    if w:
        STATS["website_nominatim"] += 1
        return w

    w = overpass_lookup_website_by_name(biz_name, lat, lon, radius_m=20000)
    if w:
        STATS["website_overpass_name"] += 1
        return w

    w = normalize_url(fsq_find_website(biz_name, lat, lon))
    if w:
        STATS["website_fsq"] += 1
        return w

    return None

# ---------- official / registry (optional) ----------
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
        out = [{"business_name": (it.get("company_name") or "").strip(), "website": None, "wikidata": None} for it in items]
        out = [x for x in out if x["business_name"]]
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
                out.append({"business_name": nm, "website": None, "wikidata": None})
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
                out.append({"business_name": nm, "website": None, "wikidata": None})
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
                            out.append({"business_name": nm, "website": None, "wikidata": None})
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

    header_lines = []
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
            header_lines.append(line)
            seen_labels.add(m_lab)

            val = (LABEL_RE[m_lab].match(line).group(1) or "").strip()
            if not val and (i + 1) < len(lines):
                nxt = lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    header_lines.append(nxt)
                    i += 1

            i += 1
            continue

        if line.strip() == "":
            header_lines.append(line)
            i += 1
            if "Website" in seen_labels:
                break
            continue

        if started:
            break

        i += 1

    rest_lines = lines[i:]
    return header_lines, rest_lines

def normalize_header_block(desc: str, company: str, website: str, batch: Optional[str] = None) -> str:
    desc = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    header_lines, rest_lines = _split_header_rest(desc)

    preserved = {"First": "", "Email": "", "Hook": "", "Variant": ""}

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
            if lab in preserved and preserved[lab] == "":
                preserved[lab] = val
            break
        i += 1

    def hard(line: str) -> str:
        return (line or "").rstrip() + "  "

    new_header = [
        hard(f"Company: {company or ''}"),
        hard(f"First: {preserved['First']}"),
        hard(f"Email: {preserved['Email']}"),
        hard(f"Hook: {preserved['Hook']}"),
        hard(f"Variant: {preserved['Variant']}"),
        hard(f"Website: {website or ''}"),
        "",
    ]

    if batch:
        has_batch = any((line or "").strip() == batch for line in rest_lines)
        if not has_batch:
            if rest_lines and rest_lines[-1].strip() != "":
                rest_lines.append("")
            rest_lines.append(batch)

    out = ("\n".join(new_header + rest_lines)).rstrip("\n") + "\n\n@lead\n"
    return out

def update_card_header(card_id: str, company: str, website: str,
                       new_name: Optional[str] = None,
                       batch: Optional[str] = None) -> bool:
    cur = trello_get_card(card_id)
    desc_old = cur["desc"]
    name_old = cur["name"]

    desc_new = normalize_header_block(desc_old, company, website, batch=batch)

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

def is_template_blank(desc: str) -> bool:
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    company = extract_label_value(d, "Company").strip()
    website = extract_label_value(d, "Website").strip()
    if company == "" and website == "":
        return True
    if re.search(r"(?mi)^\s*Company\s*:\s*$", d) and re.search(r"(?mi)^\s*Website\s*:\s*$", d):
        return True
    return False

def find_empty_template_cards(list_id: str, max_needed: int = 1) -> List[str]:
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

def clone_template_into_list(template_card_id: str, list_id: str, name: str="Lead (auto)"):
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

def ensure_min_blank_templates(list_id: str, template_id: str, need: int):
    if need <= 0 or not template_id:
        return
    empties = find_empty_template_cards(list_id, max_needed=need)
    missing = max(0, need - len(empties))
    for i in range(missing):
        clone_template_into_list(template_id, list_id, name=f"Lead (auto) {int(time.time())%100000}-{i+1}")
        time.sleep(1.0)

# ---------- seen + CSV ----------
def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(l.strip().lower() for l in f if l.strip())
    except Exception:
        return set()

def seen_domain_write(domain: str):
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

def append_csv(leads, city, country):
    if not leads:
        return
    fname = os.getenv("LEADS_CSV", f"leads_{date.today().isoformat()}.csv")
    file_exists = pathlib.Path(fname).exists()
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp","city","country","company","website"])
        ts = datetime.utcnow().isoformat(timespec="seconds")+"Z"
        for L in leads:
            w.writerow([ts, city, country, L["Company"], L["Website"]])

# ---------- main ----------
def main():
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    if not OVERPASS_ENABLED and not NOMINATIM_POI_ENABLED:
        raise SystemExit("Both OVERPASS_ENABLED and NOMINATIM_POI_ENABLED are disabled; no way to fetch candidates.")

    leads: List[dict] = []
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

        # --- official sources (optional) ---
        t_off = time.time()
        off = official_sources(city, country, lat, lon)
        STATS["off_candidates"] += len(off)
        print(f"[{city}, {country}] official candidates: {len(off)} (took {time.time()-t_off:.1f}s)", flush=True)

        leads_before_city = len(leads)

        # Process official candidates
        for biz in off:
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

            if not fetch_site_ok(website):
                STATS["skip_fetch"] += 1
                continue

            leads.append({"Company": biz["business_name"], "Website": website})

            if site_dom:
                seen_domain_write(site_dom)
                seen.add(site_dom)

            _sleep()

        print(f"[{city}] official done: +{len(leads)-leads_before_city} leads", flush=True)

        # --- OSM / POI fallback ---
        if len(leads) < DAILY_LIMIT:
            t_osm = time.time()
            print(f"[{city}] OSM search starting...", flush=True)

            cands, via = get_osm_candidates(city, country, lat, lon, south, west, north, east)
            STATS["osm_candidates"] += len(cands)
            print(f"[{city}] OSM candidates: {len(cands)} (took {time.time()-t_osm:.1f}s) via {via}", flush=True)

            leads_before_osm = len(leads)

            for biz in cands:
                if len(leads) >= DAILY_LIMIT:
                    break

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

                if not fetch_site_ok(website):
                    STATS["skip_fetch"] += 1
                    continue

                leads.append({"Company": biz["business_name"], "Website": website})

                if site_dom:
                    seen_domain_write(site_dom)
                    seen.add(site_dom)

                _sleep()

            print(f"[{city}] OSM done: +{len(leads)-leads_before_osm} leads", flush=True)

        print(f"=== CITY END: {city} in {time.time()-t_city:.1f}s | total leads={len(leads)}/{DAILY_LIMIT} ===", flush=True)

        if len(leads) >= DAILY_LIMIT:
            break

    if leads:
        leads = leads[:DAILY_LIMIT]

    # Ensure blank templates exist (optional)
    need = min(DAILY_LIMIT, len(leads))
    if PRECLONE and need > 0 and TRELLO_TEMPLATE_CARD_ID:
        ensure_min_blank_templates(TRELLO_LIST_ID, TRELLO_TEMPLATE_CARD_ID, need)

    # Save CSV (optional)
    if leads and last_city and last_country:
        append_csv(leads, last_city, last_country)

    # Push to Trello
    def push_one_lead(lead: dict, seen: set, batch_label: Optional[str] = None) -> bool:
        empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
        if not empties:
            print("No empty template card available; skipping push.", flush=True)
            return False

        card_id = empties[0]
        changed = update_card_header(
            card_id=card_id,
            company=lead["Company"],
            website=lead["Website"],
            new_name=lead["Company"],
            batch=batch_label,
        )

        dom = etld1_from_url(lead.get("Website") or "")
        if dom:
            seen_domain_write(dom)
            seen.add(dom)

        if changed:
            print(f"PUSHED ✅ — {lead['Company']} — {lead['Website']}", flush=True)
        else:
            print(f"UNCHANGED ℹ️ — {lead['Company']}", flush=True)
        return True

    batch_idx = load_batch_index()
    batch_label = BATCH_SLOTS[batch_idx]
    next_batch_idx = (batch_idx + 1) % len(BATCH_SLOTS)

    pushed = 0
    for lead in leads:
        if pushed >= DAILY_LIMIT:
            break
        ok = push_one_lead(lead, seen, batch_label=batch_label)
        if ok:
            pushed += 1
            time.sleep(max(0, PUSH_INTERVAL_S) + max(0, BUTLER_GRACE_S))

    if pushed > 0:
        save_batch_index(next_batch_idx)

    # Always print stats
    print("Stats:", json.dumps(STATS, indent=2), flush=True)

    save_seen(seen)

    print(f"SEEN_FILE path: {os.path.abspath(SEEN_FILE)} — total domains in set: {len(seen)}", flush=True)
    print(f"Done. Leads pushed: {pushed}/{len(leads)}", flush=True)

if __name__ == "__main__":
    main()
