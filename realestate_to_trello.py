# realestate_to_trello.py
# Fills Trello template cards with Company / Website (paced).
# Header order is enforced and other fields are preserved:
# Company, First, Email, Hook, Variant, Website
#
# EMAIL REMOVED:
# - No email crawling, no fallbacks, Email always blank.
#
# SPEED FIX:
# - Uses a "relaxed" Overpass query (fast) to get candidates.
# - For candidates missing a website tag: does a SMALL, CAPPED Nominatim extratags lookup
#   (no Overpass name-lookup per candidate = no 45 minute runs).
#
# GitHub Actions friendly: no venv instructions.

import os, re, json, time, random, csv, pathlib, math
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Tuple

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

PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 15)
BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 10)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.2)

QUALITY_MIN      = env_float("QUALITY_MIN", 1.2)

SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")

DEBUG            = env_on("DEBUG", False)

CITY_MODE        = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY    = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY       = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS        = env_int("CITY_HOPS", 8)

# Overpass: keep radius reasonable; huge radii time out frequently
OSM_RADIUS_M     = env_int("OSM_RADIUS_M", 8000)
OSM_MAX_CANDS    = env_int("OSM_MAX_CANDS", 250)
OVERPASS_TIMEOUT_S = env_int("OVERPASS_TIMEOUT_S", 60)

# Nominatim: capped lookups for missing website tags
NOMINATIM_WEBSITE_LOOKUPS_PER_CITY = env_int("NOMINATIM_WEBSITE_LOOKUPS_PER_CITY", 12)

NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"EditorLeads/1.0 (+{NOMINATIM_EMAIL})")

# Trello
TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")

PRECLONE        = env_on("PRECLONE", False)

STATS = {
    "osm_candidates": 0,
    "skip_no_website": 0,
    "skip_dupe_domain": 0,
    "skip_robots": 0,
    "skip_fetch": 0,
    "skip_quality": 0,
    "overpass_timeouts": 0,
}

def dbg(msg):
    if DEBUG:
        print(msg, flush=True)

# ---------- HTTP ----------
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.8,de;q=0.6,fr;q=0.6"})

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
def normalize_url(u):
    if not u:
        return None
    u = u.strip()
    if not u or u.startswith("mailto:"):
        return None
    p = urlparse(u)
    if not p.scheme:
        u = "https://" + u.strip().lstrip("/")
    return u

def etld1_from_url(u: str) -> str:
    try:
        ex = tldextract.extract(u or "")
        if ex.domain:
            return f"{ex.domain}.{ex.suffix}" if ex.suffix else ex.domain
    except Exception:
        pass
    return ""

def fetch(url):
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

# ---------- quality ----------
def looks_parked(html_text: str) -> bool:
    hay = (html_text or "").lower()
    red_flags = ["this domain is for sale","coming soon","sedo","godaddy","namecheap","parked domain"]
    return any(p in hay for p in red_flags)

def has_listings_signals(soup: BeautifulSoup) -> bool:
    needles = ["for sale","for rent","to let","buy","sell","rent","listings","properties",
               "our properties","immobili","immobilier","angebote","objekte"]
    txt = soup.get_text(" ").lower()
    if any(n in txt for n in needles):
        return True
    for a in soup.find_all("a", href=True):
        h = (a.get("href","") or "").lower()
        if any(x in h for x in ["/listings","/properties","/property","/immobili","/angebote"]):
            return True
    return False

def has_recent_content(soup: BeautifulSoup, max_days=365) -> bool:
    import datetime as dt
    text = soup.get_text(" ")
    patterns = [r"(20[2-9][0-9])", r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}"]
    now = dt.datetime.utcnow()
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            s = m.group(0)
            try:
                if re.fullmatch(r"20[2-9][0-9]", s):
                    d = dt.datetime(int(s), 12, 31)
                else:
                    parts = s.split()
                    y = int(parts[-1])
                    mon = parts[0][:3].title()
                    month_num = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].index(mon)+1
                    d = dt.datetime(y, month_num, 1)
                if (now - d).days <= max_days:
                    return True
            except Exception:
                continue
    return False

def uses_https(url: str) -> bool:
    return (urlparse(url or "").scheme == "https")

def quality_score(website: str, html_text: str, soup: BeautifulSoup) -> float:
    score = 0.0
    if not looks_parked(html_text): score += 1.0
    if has_listings_signals(soup): score += 1.0
    if has_recent_content(soup, 365): score += 0.7
    if uses_https(website): score += 0.2
    return min(score, 5.0)

def summarize_signals(q, website, soup):
    bits = []
    if has_listings_signals(soup): bits.append("listings")
    if has_recent_content(soup, 365): bits.append("recent-content")
    if uses_https(website): bits.append("https")
    return f"Signals: q={q:.2f}; " + ", ".join(bits)

# ---------- geo ----------
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

# ---------- fast Overpass candidates (no name-lookup loops) ----------
# Keep it simple: fetch agencies/brokerages. Website may or may not be present.
OSM_FILTERS = [
    ("office","estate_agent"),
    ("office","real_estate"),
    ("office","property_management"),
    ("amenity","estate_agent"),
    ("shop","estate_agent"),
    ("shop","real_estate"),
]

OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)

def _overpass_post(query: str) -> Optional[dict]:
    for ep in OVERPASS_ENDPOINTS:
        try:
            throttle("overpass", 2.0)
            r = SESS.post(ep, data={"data": query}, timeout=OVERPASS_TIMEOUT_S)
            if r.status_code != 200:
                dbg(f"[overpass] HTTP {r.status_code} via {ep}: {(r.text or '')[:160]}")
                continue
            js = r.json()
            # Overpass sometimes returns remark only
            if isinstance(js, dict) and js.get("remark"):
                dbg(f"[overpass] remark via {ep}: {js.get('remark')}")
            return js
        except Exception as e:
            dbg(f"[overpass] error via {ep}: {e}")
    return None

def overpass_estate_agents_relaxed(lat: float, lon: float, radius_m: int) -> List[dict]:
    # If Overpass is overloaded, auto-shrink radius and retry.
    radii = [radius_m, max(2500, int(radius_m * 0.6)), max(2000, int(radius_m * 0.4))]
    for r0 in radii:
        parts = []
        for k, v in OSM_FILTERS:
            parts.append(f'nwr(around:{r0},{lat},{lon})["{k}"="{v}"]["name"];')
        q = f"""[out:json][timeout:60];({ ' '.join(parts) });out tags center qt;"""
        js = _overpass_post(q)
        if not js:
            STATS["overpass_timeouts"] += 1
            continue
        els = js.get("elements") or []
        if els:
            dbg(f"[overpass] ok radius={r0} -> {len(els)} elements")
            rows = []
            for el in els:
                tags = el.get("tags", {}) or {}
                name = (tags.get("name") or "").strip()
                if not name:
                    continue
                website = tags.get("website") or tags.get("contact:website") or tags.get("url")
                email = tags.get("email") or tags.get("contact:email")  # ignored, but left for future
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

            # dedupe by (name, domain)
            dedup = {}
            for rr in rows:
                key = (rr["business_name"].lower(), etld1_from_url(rr["website"] or ""))
                if key not in dedup:
                    dedup[key] = rr
            out = list(dedup.values())
            random.shuffle(out)
            return out[:OSM_MAX_CANDS]

        # if empty elements, try smaller radius next
        STATS["overpass_timeouts"] += 1

    return []

# ---------- website resolution (no email) ----------
def nominatim_lookup_website(name: str, city: str, country: str, limit: int = 3) -> Optional[str]:
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

def resolve_website(biz: dict, city: str, country: str, lookups_left: List[int]) -> Optional[str]:
    # 1) direct from OSM tags
    w = normalize_url(biz.get("website"))
    if w:
        return w

    # 2) capped Nominatim extratags lookup (fast-ish, but limited per city)
    if lookups_left[0] > 0:
        lookups_left[0] -= 1
        w2 = nominatim_lookup_website(biz.get("business_name") or "", city, country, limit=3)
        if w2:
            return w2

    return None

# ---------- cities ----------
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
]

def iter_cities():
    pool = CITY_ROTATION[:]
    country_whitelist = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
    if country_whitelist:
        wl = {c.lower() for c in country_whitelist}
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

def normalize_header_block(desc, company, website):
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

    # Email intentionally blank
    new_header = [
        hard(f"Company: {company or ''}"),
        hard(f"First: {preserved['First']}"),
        hard("Email: "),
        hard(f"Hook: {preserved['Hook']}"),
        hard(f"Variant: {preserved['Variant']}"),
        hard(f"Website: {website or ''}"),
        "",
    ]
    return "\n".join(new_header + rest_lines)

def update_card_header(card_id, company, website, new_name=None):
    cur = trello_get_card(card_id)
    desc_old = cur["desc"]
    name_old = cur["name"]

    desc_new = normalize_header_block(desc_old, company, website)

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
    return bool(re.search(r"(?mi)^\s*Company\s*:\s*$", d))

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

# ---------- seen file ----------
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

# ---------- CSV ----------
def append_csv(leads):
    if not leads:
        return
    fname = os.getenv("LEADS_CSV", f"leads_{date.today().isoformat()}.csv")
    file_exists = pathlib.Path(fname).exists()
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp","city","country","company","website","q"])
        ts = datetime.utcnow().isoformat(timespec="seconds")+"Z"
        for L in leads:
            w.writerow([ts, L["City"], L["Country"], L["Company"], L["Website"], f'{L.get("q",0):.2f}'])

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

        print(f"[{city}, {country}] official candidates: 0 (disabled)", flush=True)
        print(f"[{city}] official done: +0 leads", flush=True)

        # OSM
        if len(leads) < DAILY_LIMIT:
            t_osm = time.time()
            print(f"[{city}] OSM search starting...", flush=True)

            cands = overpass_estate_agents_relaxed(lat, lon, OSM_RADIUS_M)
            STATS["osm_candidates"] += len(cands)
            print(f"[{city}] OSM candidates: {len(cands)} (took {time.time()-t_osm:.1f}s)", flush=True)

            lookups_left = [NOMINATIM_WEBSITE_LOOKUPS_PER_CITY]
            leads_before_osm = len(leads)

            for j, biz in enumerate(cands, start=1):
                if len(leads) >= DAILY_LIMIT:
                    break
                if j % 25 == 0:
                    print(f"[{city}] OSM progress: {j}/{len(cands)} | leads={len(leads)}/{DAILY_LIMIT}", flush=True)

                website = resolve_website(biz, city, country, lookups_left)
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
                q = quality_score(website, home.text, soup_home)
                if q < QUALITY_MIN:
                    STATS["skip_quality"] += 1
                    continue

                leads.append({
                    "City": city,
                    "Country": country,
                    "Company": biz["business_name"],
                    "Email": "",  # removed
                    "Website": website,
                    "q": q,
                    "signals": summarize_signals(q, website, soup_home),
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
        append_csv(leads,)

    def push_one_lead(lead: dict) -> bool:
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
        )

        # persist domain always
        cur = trello_get_card(card_id)
        website_on_card = extract_label_value(cur["desc"], "Website") or (lead.get("Website") or "")
        website_on_card = normalize_url(website_on_card) or ""
        site_dom = etld1_from_url(website_on_card)
        if site_dom:
            seen_domains(site_dom)
            seen.add(site_dom)

        if changed:
            print(f"PUSHED ✅ q={lead.get('q',0):.2f} — {lead['Company']} — {lead['Website']}", flush=True)
        else:
            print(f"UNCHANGED ℹ️ (still recorded domain) — {lead['Company']}", flush=True)
        return True

    pushed = 0
    if leads:
        for lead in leads:
            if pushed >= DAILY_LIMIT:
                break
            if push_one_lead(lead):
                pushed += 1
                time.sleep(max(0, PUSH_INTERVAL_S) + max(0, BUTLER_GRACE_S))

    if DEBUG:
        print("STATS:", json.dumps(STATS, indent=2), flush=True)

    save_seen(seen)

    print(f"SEEN_FILE path: {os.path.abspath(SEEN_FILE)} — total domains in set: {len(seen)}", flush=True)
    print(f"Done. Leads pushed: {pushed}/{min(len(leads), DAILY_LIMIT)}", flush=True)

if __name__ == "__main__":
    main()
