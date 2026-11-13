"""
Fast & Clean IPTV Updater (Video Only - No Radio, Single-Pass Fast Check).
Sources:
  1. AynaOTT (BD & Bangla API)
  2. JagoBD (Web Scraper)
  3. Samsung TV Plus (Official Public API - IN & US)
  4. Pluto TV (Official Public API - Movies/Music/Sports)
  5. Extra Public Repos (BDIX, Free-TV India, IPTV-org Sports/Movies)
  6. GitHub Code Search (Star/Sony/Zee/Bangla/IP - Rate-Limit Safe)
-> Radio/Audio Filter -> Fast Single-Pass Dead Link Check -> Duplicate Remove -> playlist.m3u
"""
import json, os, re, ssl, sys, time, uuid
from collections import Counter
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ------------------------------------------------------------------ settings
OUTPUT = Path("playlist.m3u")
WORKERS = 80                 # ek sathe 80 ta link check hobe (onek fast)
CHECK_TIMEOUT = 6            # sekond (slow/dead link druto bad dibe)
MAX_LINKS_PER_CHANNEL = 1    # proti channel e max koto alive link
MIN_KEEP_RATIO = 0.5         # alive sonkhya ager playlist er 50% er niche hole overwrite korbe na

# GitHub Secrets theke token nibe (code e khali rakhun)
GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")

# GitHub Search Settings (Fast & 422/429 Safe)
USE_GITHUB_SEARCH = True
SEARCH_PAGES = (1,)          # proti query te top 100 file nibe (time bachabe)
SEARCH_DELAY = 15            # proti API request er majhe 15s biroti
GITHUB_SEARCH_QUERIES = [
    "EXTINF bangla extension:m3u",
    "EXTINF bdix extension:m3u8",
    'EXTINF "Star Gold" extension:m3u',
    'EXTINF "Sony Sab" extension:m3u',
    'EXTINF "Zee Cinema" extension:m3u',
    'EXTINF "http://103." extension:m3u',
]

IPTV_BASE = "https://iptv-org.github.io/iptv"

# Desh wise: Bangladesh (shob) & India (shudhu Hindi o Bangla)
INDIA_LANGS = ("hin", "ben")
COUNTRY_SOURCES = [
    ("Bangladesh", f"{IPTV_BASE}/countries/bd.m3u", None),
    ("India",      f"{IPTV_BASE}/countries/in.m3u", INDIA_LANGS),
]

# True dile: niche dewa GROUP_ORDER er baire onno group er channel playlist e rakhbe na
STRICT_GROUPS = True

# Category + Bhasha
COMBOS = [
    ("Kolkata Movies", "movies", "ben"),
    ("Hindi Movies",   "movies", "hin"),
    ("English Movies", "movies", "eng"),
    ("Hindi Music",    "music",  "hin"),
    ("English Music",  "music",  "eng"),
    ("Bangla Music",   "music",  "ben"),
    ("Sports",         "sports", ("hin", "eng", "ben")),
]

# Playlist e group er order
GROUP_ORDER = [
    "Bangladesh", "Bangla (India)", "Kolkata Movies", "Bangla Music",
    "Hindi Movies", "Hindi Music", "English Movies", "English Music",
    "Sports", "India"
]

BLOCK_URLS = [
    "amg13643-amg13643c1-amgplt0016",
]

NOT_BANGLA_HOSTS = ["amagi.tv"]

# Adult & Radio/Audio content filter
BLOCK_WORDS = ["xxx", "adult", "porn", "18+", "erotic", "sex"]
RADIO_RE = re.compile(
    r"\b(radio|fm|am\s*radio|podcast|audio\s*only|mirchi|foorti|shadhin|bhoot\s*fm|"
    r"dhaka\s*fm|abc\s*radio|peoples\s*radio|today\s*fm|jago\s*fm|air\s*vividh|akashvani|"
    r"red\s*fm|big\s*fm|radio\s*city|radio\s*one|fever\s*fm|my\s*fm|ishq\s*fm)\b",
    re.I
)
AUDIO_EXT_RE = re.compile(r"\.(mp3|aac|ogg|oga|wav|flac|m4a|wma|opus|pls|asx)(\?|$)", re.I)

# ------------------------------------------------------------------ http
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/129.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}


def log(msg):
    print(msg, flush=True)


def http_get(url, headers=None, timeout=15):
    req = urllib.request.Request(url, headers=headers or HEADERS)
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        return r.read().decode("utf-8", errors="ignore")


def github_search_get(api_url, headers, max_retries=2):
    """429 Too Many Requests ashle antoto 70s wait kore abar try korbe"""
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=20, context=SSL_CTX) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 403) and attempt < max_retries:
                raw_wait = int(e.headers.get("Retry-After", 70))
                wait_sec = max(raw_wait, 70)
                log(f"[GitHub search] Rate limit ({e.code}), {wait_sec}s wait kore abar try korchi...")
                time.sleep(wait_sec)
            else:
                raise
    return {}


# ------------------------------------------------------------------ m3u parse
def parse_m3u(text, source):
    out, meta = [], None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            def attr(k):
                m = re.search(rf'{k}="([^"]*)"', line, re.I)
                return m.group(1).strip() if m else ""
            name = line.rsplit(",", 1)[-1].strip() if "," in line else attr("tvg-name")
            meta = {
                "name": name or attr("tvg-name") or "Unknown",
                "tvg_id": attr("tvg-id"),
                "logo": attr("tvg-logo"),
                "group": attr("group-title") or "Others",
                "source": source,
                "radio_tag": bool(re.search(r'radio="true"', line, re.I))
            }
        elif meta and not line.startswith("#"):
            if re.match(r"^https?://", line):
                meta["url"] = line
                out.append(meta)
            meta = None
    return out


# ------------------------------------------------------------------ scrapers
def scrape_aynaott():
    base = "https://web.aynaott.com"
    h = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "X-Tenant-Id": "019dd92f-107c-7056-9e79-e5233f6e51d9",
        "Cache-Control": "no-cache"
    }
    catalog, seen = [], set()
    for page in range(1, 20):
        try:
            data = json.loads(http_get(f"{base}/api/v5/catalog/contents?type=live-tvs&per_page=100&page={page}",
                                       headers=h, timeout=15))
        except Exception as e:
            log(f"[AynaOTT] catalog page {page}: {e}")
            break
        recs = data.get("records", [])
        if not recs:
            break
        for r in recs:
            s = str(r.get("slug", "")).strip()
            if s and s not in seen:
                seen.add(s)
                catalog.append(r)
        if len(recs) < 100:
            break
    log(f"[AynaOTT] catalog: {len(catalog)}")

    def one(item):
        slug, title = item.get("slug", ""), item.get("title", "")
        img = item.get("images", {})
        logo = img.get("thumbnail_url", "") if isinstance(img, dict) else ""
        if logo and not logo.startswith("http"):
            logo = base + logo
        try:
            d = json.loads(http_get(f"{base}/api/v5/browse/live-tvs/{slug}", headers=h, timeout=10))
            rec = d.get("record", d)
            so = rec.get("stream", {})
            url = so.get("url", "") if isinstance(so, dict) else ""
            url = url or rec.get("stream_url") or rec.get("hls") or ""
            if url:
                return {
                    "name": title,
                    "url": url,
                    "logo": logo,
                    "tvg_id": f"aynaott-{slug}",
                    "group": "Bangla TV",
                    "source": "AynaOTT"
                }
        except Exception:
            pass
        return None

    res = []
    with ThreadPoolExecutor(max_workers=25) as ex:
        for f in as_completed([ex.submit(one, i) for i in catalog]):
            r = f.result()
            if r:
                res.append(r)
    log(f"[AynaOTT] stream URL: {len(res)}")
    return res


def format_jagobd_url(raw):
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    try:
        parts = [p for p in urllib.parse.urlparse(raw).path.lstrip("/").split("/") if p]
        if len(parts) >= 2:
            return f"https://app24.jagobd.com.bd/{parts[0]}/{'/'.join(parts[1:])}"
    except Exception:
        pass
    clean = raw.split("|")[0]
    parts = [p for p in clean.split("/") if p]
    for i, p in enumerate(parts):
        if ".m3u8" in p or ".stream" in p:
            if i >= 2:
                return f"https://app24.jagobd.com.bd/{parts[i-1]}/{'/'.join(parts[i:])}"
            break
    return clean


def scrape_jagobd():
    cands = []
    try:
        html = http_get("https://www.jagobd.com/", timeout=15)
        skip = ['/category', '/tag', '/contact', '/about', '/privacy', '/page', '/wp-', '/feed',
                '/disclaimer', 'dmca', 'terms', 'advertise', 'copyright']
        seen = set()
        for url, content in re.findall(
                r'<a\s+[^>]*href=[\'"](https://www\.jagobd\.com/[^\'"#?]+)[\'"][^>]*>(.*?)</a>', html, re.S):
            url = url.rstrip("/")
            if url in seen or any(x in url.lower() for x in skip):
                continue
            seen.add(url)
            img = re.search(r'<img\s+[^>]*src=[\'"]([^\'"]+)[\'"]', content, re.I)
            alt = re.search(r'alt=[\'"]([^\'"]+)[\'"]', content, re.I)
            ttl = re.search(r'title=[\'"]([^\'"]+)[\'"]', content, re.I)
            slug = url.split("/")[-1]
            raw = (alt.group(1) if alt and alt.group(1).strip() else
                   ttl.group(1) if ttl and ttl.group(1).strip() else slug).strip()
            nm = re.sub(r'\s*(?:Live|Online|TV Live|Bangla TV|Live TV|Channel)\s*$', '', raw, flags=re.I).strip()
            nm = re.sub(r'[-_]+', ' ', nm).strip().title()
            nm = re.sub(r'\bTv\b', 'TV', nm)
            cands.append({
                "slug": slug,
                "url": url,
                "name": nm or slug.replace("-", " ").title(),
                "logo": img.group(1) if img else ""
            })
    except Exception as e:
        log(f"[JagoBD] homepage error: {e}")
    log(f"[JagoBD] channel link: {len(cands)}")

    def resolve(c):
        try:
            page = http_get(c["url"], timeout=10)
            embed = None
            for ifr in re.findall(r'<iframe[^>]+src=[\'"]([^\'"]+)[\'"]', page, re.I):
                if "embed.php" in ifr or "jagobd.com" in ifr:
                    embed = ifr
                    break
            stream = None
            if not embed:
                m = re.findall(r'[\'"](https?://[^\'"]+?\.m3u8[^\'"]*)[\'"]', page)
                stream = m[0] if m else None
            else:
                if embed.startswith("//"):
                    embed = "https:" + embed
                eh = http_get(embed, headers={"User-Agent": UA, "Referer": c["url"]}, timeout=10)
                for _fn, arr in re.findall(
                        r'function\s+(\w+)\(\)\s*\{\s*return\(\[([^\]]+)\]\.join\([\'"][\'"]\)', eh):
                    val = "".join(x.strip().strip("'\"") for x in arr.split(",")).replace(r"\/", "/")
                    if ".m3u8" in val:
                        stream = val
                        break
                if not stream:
                    d = re.findall(r'[\'"](https?://[^\'"]+?\.m3u8[^\'"]*)[\'"]', eh)
                    stream = d[0] if d else None
            if stream:
                return {
                    "name": c["name"],
                    "url": format_jagobd_url(stream),
                    "logo": c["logo"],
                    "tvg_id": f"jagobd-{c['slug']}",
                    "group": "Bangla TV",
                    "source": "JagoBD"
                }
        except Exception:
            pass
        return None

    res = []
    with ThreadPoolExecutor(max_workers=20) as ex:
        for f in as_completed([ex.submit(resolve, c) for c in cands]):
            r = f.result()
            if r:
                res.append(r)
    log(f"[JagoBD] stream URL: {len(res)}")
    return res


def scrape_samsung_tv():
    """Samsung TV Plus Public API (India & US)"""
    res = []
    endpoints = [
        ("IN", "https://i.mjh.nz/SamsungTVPlus/in.m3u8"),
        ("US", "https://i.mjh.nz/SamsungTVPlus/us.m3u8"),
    ]
    for region, url in endpoints:
        try:
            txt = http_get(url, timeout=15)
            chs = parse_m3u(txt, "SamsungTV")
            for c in chs:
                c["region"] = region
                c["auto_group"] = True
            res.extend(chs)
            log(f"[SamsungTV] {region}: {len(chs)} channel")
        except Exception as e:
            log(f"[SamsungTV] {region} error: {e}")
    return res


def scrape_pluto_tv():
    """Pluto TV Official Public API (Movies, Music, Sports)"""
    res = []
    try:
        sid = str(uuid.uuid4())
        did = str(uuid.uuid4())
        api = (f"https://api.pluto.tv/v2/channels?start=2026-01-01T00:00:00Z&stop=2026-01-01T01:00:00Z"
               f"&deviceId={did}&sid={sid}&deviceType=web&deviceMake=Chrome&deviceModel=web&appName=web&appVersion=5.0")
        data = json.loads(http_get(api, timeout=15))
        for ch in data:
            if not ch.get("isStitched"):
                continue
            urls = ch.get("stitched", {}).get("urls", [])
            stream_url = urls[0].get("url", "") if urls else ""
            if not stream_url:
                continue
            cat = ch.get("category", "")
            logo = ch.get("colorLogoPNG", {}).get("path", "") or ch.get("logo", {}).get("path", "")
            res.append({
                "name": ch.get("name", "Unknown").strip(),
                "url": stream_url,
                "logo": logo,
                "tvg_id": f"pluto-{ch.get('slug', '')}",
                "group": cat,
                "source": "PlutoTV",
                "auto_group": True
            })
        log(f"[PlutoTV] stream URL: {len(res)}")
    except Exception as e:
        log(f"[PlutoTV] error: {e}")
    return res


def scrape_extra_repos():
    """Popular Public BDIX, Free-TV & Sports/Movies Repos"""
    res = []
    repos = [
        ("BDIX-Mrgify",      "https://raw.githubusercontent.com/abusaeeidx/Mrgify-BDIX-IPTV/main/playlist.m3u"),
        ("Free-TV-IN",       "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_india.m3u8"),
        ("IPTV-org-Sports",  f"{IPTV_BASE}/categories/sports.m3u"),
        ("IPTV-org-Movies",  f"{IPTV_BASE}/categories/movies.m3u"),
    ]
    for label, url in repos:
        try:
            txt = http_get(url, timeout=20)
            chs = parse_m3u(txt, "GitHub")
            for c in chs:
                c["auto_group"] = True
            res.extend(chs)
            log(f"[{label}] {len(chs)} channel")
        except Exception as e:
            log(f"[{label}] error: {e}")
    return res


def scrape_github():
    cache = {}

    def get(url):
        if url not in cache:
            try:
                cache[url] = parse_m3u(http_get(url, timeout=30), "GitHub")
            except Exception as e:
                log(f"[GitHub] fail {url}: {e}")
                cache[url] = []
            log(f"[GitHub] {url.replace(IPTV_BASE + '/', '')}: {len(cache[url])} channel")
        return cache[url]

    res = []
    # 1) Category x Language
    for group, cat, lang in COMBOS:
        langs = (lang,) if isinstance(lang, str) else lang
        lang_urls = {c["url"].split("|")[0] for lg in langs for c in get(f"{IPTV_BASE}/languages/{lg}.m3u")}
        n = 0
        for c in get(f"{IPTV_BASE}/categories/{cat}.m3u"):
            if c["url"].split("|")[0] in lang_urls:
                res.append({**c, "group": group})
                n += 1
        log(f"[GitHub] {group}: {n} channel")

    # 2) Country sources (Bangladesh & India [hin/ben])
    for group, url, langs in COUNTRY_SOURCES:
        allowed = None
        if langs:
            allowed = {c["url"].split("|")[0] for lg in langs for c in get(f"{IPTV_BASE}/languages/{lg}.m3u")}
        n_all = n_keep = 0
        for c in get(url):
            n_all += 1
            if allowed is not None and c["url"].split("|")[0] not in allowed:
                continue
            n_keep += 1
            res.append({**c, "group": "Sports" if "sport" in c.get("group", "").lower() else group})
        log(f"[GitHub] {group}: {n_keep}/{n_all} channel rakha holo")

    # 3) Fast GitHub Search (422 & 429 safe)
    if USE_GITHUB_SEARCH and GITHUB_TOKEN:
        h = {
            "User-Agent": UA,
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {GITHUB_TOKEN.strip()}"
        }
        raws = []
        req_count = 0
        for idx, kw in enumerate(GITHUB_SEARCH_QUERIES):
            for page in SEARCH_PAGES:
                time.sleep(SEARCH_DELAY if req_count > 0 else 5)
                req_count += 1
                try:
                    api = ("https://api.github.com/search/code?q=" +
                           urllib.parse.quote(kw) + f"&per_page=100&page={page}")
                    data = github_search_get(api, headers=h)
                    items = data.get("items", [])
                    log(f"[GitHub search] query {idx+1} ('{kw}') page {page}: {len(items)} ta file")
                    for it in items:
                        raws.append(it["html_url"].replace("https://github.com/",
                                    "https://raw.githubusercontent.com/").replace("/blob/", "/"))
                    if len(items) < 100:
                        break
                except Exception as e:
                    log(f"[GitHub search] query {idx+1} page {page} fail: {e}")
                    break

        unique_raws = list(dict.fromkeys(raws))
        log(f"[GitHub search] mot {len(unique_raws)} ta unique file download kora hocche...")

        def fetch_raw_file(raw_url):
            try:
                txt = http_get(raw_url, timeout=10)
                if "#EXTINF" in txt:
                    parsed = parse_m3u(txt, "GitHub")
                    for c in parsed:
                        c["auto_group"] = True
                    return parsed
            except Exception:
                pass
            return []

        found = 0
        with ThreadPoolExecutor(max_workers=35) as ex:
            for f in as_completed([ex.submit(fetch_raw_file, r) for r in unique_raws]):
                chs = f.result()
                res.extend(chs)
                found += len(chs)
        log(f"[GitHub search] mot {len(unique_raws)} file theke {found} channel paoa gese")
    elif USE_GITHUB_SEARCH:
        log("[GitHub search] GITHUB_TOKEN boshano hoy ni, keyword search skip kora holo")
    return res


# ------------------------------------------------------------------ stream check (Video Only - Single Pass)
def check_stream(raw_url, timeout=CHECK_TIMEOUT):
    """(alive, latency_sec, reason) - Shudhu video stream allow korbe, radio/audio bad dibe"""
    if not raw_url:
        return False, 0, "empty url"
    base, url = dict(HEADERS), raw_url
    if "|" in raw_url:
        url, extra = raw_url.split("|", 1)
        url = url.strip()
        for part in extra.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                base[k.strip()] = v.strip()

    if AUDIO_EXT_RE.search(url):
        return False, 0, "radio/audio file"

    if "jagobd.com" in url and "Referer" not in base:
        base["Referer"] = "https://www.jagobd.com/"
    elif "aynaott.com" in url and "Referer" not in base:
        base["Referer"] = "https://web.aynaott.com/"

    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=base)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            ctype = r.headers.get("Content-Type", "").lower()
            icy_name = r.headers.get("icy-name") or r.headers.get("icy-genre")
            if icy_name or ctype.startswith("audio/"):
                return False, 0, "radio/audio stream"

            raw = r.read(2048)
            low = raw.decode("utf-8", errors="ignore").strip().lower()
            lat = time.time() - t0

            if r.status not in (200, 206):
                return False, 0, f"HTTP {r.status}"
            if ("<html" in low or "<body" in low) and "#extm3u" not in low:
                return False, 0, "html page (block/login)"
            if "#extm3u" in low and AUDIO_EXT_RE.search(low) and ".ts" not in low and "#ext-x-stream-inf" not in low:
                return False, 0, "audio-only m3u8"
            if any(t in low for t in ("#extm3u", "#ext-x-stream-inf", "#extinf",
                                        "#ext-x-targetduration", "#ext-x-media-sequence")):
                return True, lat, "ok"
            if any(v in ctype for v in ("video/", "mpegurl", "octet-stream")):
                return True, lat, "ok"
            if raw and raw[0] == 0x47:
                return True, lat, "ok"
            if (".m3u8" in url or ".ts" in url) and low and not low.startswith("<"):
                return True, lat, "ok"
            return False, 0, "not a video stream"
    except urllib.error.HTTPError as e:
        return False, 0, f"HTTP {e.code}"
    except urllib.error.URLError as e:
        msg = str(e.reason)
        return False, 0, "timeout" if "timed out" in msg.lower() else f"connect: {msg[:35]}"
    except TimeoutError:
        return False, 0, "timeout"
    except Exception as e:
        return False, 0, f"error: {type(e).__name__}"


def check_all(channels):
    total, done = len(channels), 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut = {ex.submit(check_stream, c["url"]): c for c in channels}
        for f in as_completed(fut):
            c = fut[f]
            try:
                ok, lat, reason = f.result()
            except Exception as e:
                ok, lat, reason = False, 0, f"error: {type(e).__name__}"
            c["alive"], c["latency"], c["reason"] = ok, lat, reason
            done += 1
            if done % 500 == 0 or done == total:
                log(f"checked {done}/{total} | alive {sum(1 for x in channels if x.get('alive'))}")

    dead = Counter(c["reason"] for c in channels if not c.get("alive") and c.get("reason"))
    log("Dead er karon: " + ", ".join(f"{k}: {v}" for k, v in dead.most_common(8)))


# ------------------------------------------------------------------ grouping & dedupe
NOISE = {"hd", "fhd", "sd", "uhd", "4k", "live", "official", "hevc", "h265", "h264"}
SOURCE_RANK = {"AynaOTT": 0, "JagoBD": 1, "SamsungTV": 2, "PlutoTV": 3, "GitHub": 4}


def norm(name):
    n = name.lower()
    n = re.sub(r"[\(\[].*?[\)\]]", " ", n)
    n = re.sub(r"\b\d{3,4}p\b", " ", n)
    n = n.replace("television", "tv")
    n = re.sub(r"[^a-z0-9\u0980-\u09ff]+", " ", n)
    return "".join(t for t in n.split() if t not in NOISE)


def is_blocked_or_radio(c):
    if c.get("radio_tag"):
        return True
    t = f'{c["name"]} {c.get("group", "")}'.lower()
    if any(re.search(rf"\b{re.escape(w)}\b", t) for w in BLOCK_WORDS):
        return True
    if RADIO_RE.search(t) or AUDIO_EXT_RE.search(c.get("url", "")):
        return True
    return False


BANGLA_RE = re.compile(r"bangla|bengali|bengal|bangladesh|\bbd\b|jalsha|\baath\b|ananda|kolkata|calcutta|ghanta|ebela|sangeet bangla")
HINDI_RE = re.compile(
    r"\bhindi\b|bollywood|star plus|star bharat|star utsav|star gold|sony sab|\bsab tv\b|sony tv|sony entertainment|"
    r"sony pal|sony max|sony pix|sony wah|sony yay|sony bbc|\bcolors\b|colors tv|colors rishtey|colors cineplex|"
    r"zee tv|zee anmol|zee cinema|zee classic|zee action|zee bollywood|zee news|zee hindustan|zee zing|zee zest|zee cafe|"
    r"&tv|\band tv\b|and pictures|&pictures|and priv|&priv|dangal|ishara|shemaroo|big magic|bindass|aaj tak|abp news|ndtv|"
    r"india tv|news18 india|republic bharat|tv9 bharat|dd national|dd news|dd bharati|sanskar|aastha|sadhna|bhojpuri|b4u|"
    r"mtv india|9xm|9x jalwa|zing|utv|filamchi|goldmines|manoranjan|cineplex|news nation|india news|good news today|"
    r"bharat 24|\btez\b|zoom tv|mastiii|movie plus|enter10|bflix")

OTHER_LANG_RE = re.compile(
    r"tamil|telugu|malayalam|kannada|marathi|punjabi|gujarati|odia|oriya|assamese|konkani|"
    r"star vijay|star maa|star pravah|star suvarna|star kirano|zee tamil|zee telugu|zee keralam|zee kannada|"
    r"zee marathi|zee punjabi|zee sarthak|zee talkies|zee yuva|zee thirai|zee cinemalu|zee picchar|"
    r"sony marathi|sun tv|vijay|asianet|surya|kairali|maa tv|etv telugu|gemini|raj tv|jaya tv|polimer|"
    r"arabic|russian|french|german|spanish|turkish|italian|portuguese|polish|greek|dutch|swedish|ukrainian|romanian|"
    r"farsi|persian|hebrew|japanese|chinese|mandarin|korean|thai|vietnamese|indonesian|tagalog|filipino|"
    r"al jazeera|al arabiya|\bnhk\b|\bcctv\b|\bkbs\b|\bmbc\b|telemundo|univision")

HINDI_REMAP = {"Bangladesh": "India", "Kolkata Movies": "Hindi Movies", "Bangla Music": "Hindi Music"}


def bd_group(name):
    return "Bangla Music" if re.search(r"\b(music|gaan|song|sangeet)\b", name.lower()) else "Bangladesh"


KOLKATA_RE = re.compile(r"\b(jalsha movies|zee bangla cinema|colors bangla cinema|bangla cinema|"
                        r"bangla movies?|bengali movies?|bengali cinema)\b")

HINDI_MOVIE_CH_RE = re.compile(
    r"\b(star gold|sony max|sony wah|zee cinema|zee classic|zee action|zee bollywood|zee anmol cinema|"
    r"colors cineplex|and pictures|&pictures|utv movies|utv action|b4u movies|b4u kadak|"
    r"filamchi|goldmines|manoranjan|enter10|bflix|movie plus)\b"
)
ENGLISH_MOVIE_CH_RE = re.compile(
    r"\b(star movies|hbo|sony pix|zee cafe|and priv|&priv|movies now|mnx|mn\+|romedy now|axn|wb|warner bros|"
    r"paramount|amc|cinemax|showtime|tcm|hallmark|syfy|mgm|universal movies)\b"
)

NETWORK_GROUP_RE = re.compile(
    r"\b(star plus|star bharat|star utsav|star jalsha|star sports|"
    r"sony entertainment|sony tv|set hd|set india|sony sab|sab tv|sony pal|sony aath|sony yay|sony bbc|sony ten|sony sports|"
    r"zee tv|zee anmol|zee bangla|zee news|zee hindustan|zee zing|zee zest|zee business|zee salaam|&tv|and tv)\b"
)

IP_URL_RE = re.compile(r"^https?://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
POPULAR_BD_IN_RE = re.compile(
    r"\b(btv|somoy|jamuna|ekattor|channel 24|dbc|independent|atn|news 24|channel i|ntv|rtv|"
    r"banglavision|maasranga|deepto|boishakhi|desh tv|nagorik|gtv|gazi|asian tv|sa tv|mohona|"
    r"my tv|bijoy|ananda|nexus|t sports|duronto|gaan bangla|star jalsha|zee bangla|colors bangla|"
    r"sony aath|jalsha movies|sun bangla|akas aath|sangeet bangla)\b"
)


def classify_network_channel(lname):
    if KOLKATA_RE.search(lname):
        return "Kolkata Movies"
    if any(w in lname for w in ("star jalsha", "zee bangla", "sony aath")):
        return "Bangla (India)"
    if HINDI_MOVIE_CH_RE.search(lname):
        return "Hindi Movies"
    if ENGLISH_MOVIE_CH_RE.search(lname):
        return "English Movies"
    if any(w in lname for w in ("star sports", "sony ten", "sony sports")):
        return "Sports"
    if "zing" in lname:
        return "Hindi Music"
    return "India"


def keyword_group(c):
    name, group, url = c["name"], c.get("group", ""), c.get("url", "")
    lname = name.lower()
    t = f"{name} {group}".lower()

    if KOLKATA_RE.search(t):
        return "Kolkata Movies"
    if HINDI_MOVIE_CH_RE.search(t):
        return "Hindi Movies"
    if ENGLISH_MOVIE_CH_RE.search(t):
        return "English Movies"
    if NETWORK_GROUP_RE.search(lname):
        return classify_network_channel(lname)

    music = re.search(r"\b(music|song|songs|gaan|mtv|vevo|hits|zing)\b", t)
    movie = re.search(r"\b(movie|movies|cinema|film|films|flix|bollywood|hollywood|action|thriller|romance)\b", t)
    sport = re.search(r"\b(sport|sports|cricket|football|soccer|ten sports|star sports|t sports|willow|ptv sports|sony ten|sony sports)\b", t)
    bn = re.search(r"\b(bangla|bengali|bangladesh|\bbd\b)\b", t) or POPULAR_BD_IN_RE.search(t)
    hi = re.search(r"\b(hindi|bollywood|bhojpuri)\b", t) or c.get("region") == "IN"
    en = re.search(r"\b(english|hollywood)\b", t) or c.get("region") == "US" or c["source"] == "PlutoTV"

    if music:
        if bn:
            return "Bangla Music"
        if hi:
            return "Hindi Music"
        if en:
            return "English Music"
    if movie:
        if hi:
            return "Hindi Movies"
        if en:
            return "English Movies"
    if sport:
        return "Sports"
    if bn:
        return "Bangladesh"
    if hi and c.get("region") != "IN":
        return "India"
    if IP_URL_RE.match(url) and re.search(r"\b(bd|bangla|india|kolkata|live)\b", t):
        return "Bangladesh"
    return None


def dedupe_by_name(chs, limit):
    specific = {g for g, _c, _l in COMBOS}

    def rank(c):
        g = c["group"]
        return (0 if g in specific else 1 if g in GROUP_ORDER else 2,
                SOURCE_RANK.get(c["source"], 9), c.get("latency", 99))

    kept, count = set(), {}
    for c in sorted(chs, key=rank):
        key = norm(c["name"]) or c["name"].lower()
        if count.get(key, 0) < limit:
            count[key] = count.get(key, 0) + 1
            kept.add(id(c))
    return [c for c in chs if id(c) in kept]


# ------------------------------------------------------------------ output
def write_playlist(channels):
    def q(s):
        return str(s).replace('"', "'").replace("\n", " ")
    lines = ["#EXTM3U"]
    for c in channels:
        lines.append(f'#EXTINF:-1 tvg-id="{q(c.get("tvg_id", ""))}" tvg-name="{q(c["name"])}" '
                     f'tvg-logo="{q(c.get("logo", ""))}" group-title="{q(c["group"])}",{c["name"]}')
        lines.append(c["url"])
    OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    start_time = time.time()
    pool = []
    scrapers = (
        ("AynaOTT", scrape_aynaott),
        ("JagoBD", scrape_jagobd),
        ("SamsungTV", scrape_samsung_tv),
        ("PlutoTV", scrape_pluto_tv),
        ("ExtraRepos", scrape_extra_repos),
        ("GitHub", scrape_github),
    )
    for name, fn in scrapers:
        try:
            pool.extend(fn())
        except Exception as e:
            log(f"[{name}] error: {e}")

    seen, items = set(), []
    for c in pool:
        raw = c["url"].split("|")[0].strip()
        if not raw or raw in seen or is_blocked_or_radio(c):
            continue
        name, lname = c["name"], c["name"].lower()
        if OTHER_LANG_RE.search(lname):
            continue

        if c["source"] in ("AynaOTT", "JagoBD"):
            c["group"] = bd_group(name)
        elif c.get("auto_group"):
            kw = keyword_group(c)
            if kw:
                c["group"] = kw
            else:
                continue

        if KOLKATA_RE.search(lname):
            c["group"] = "Kolkata Movies"
        elif HINDI_MOVIE_CH_RE.search(lname):
            c["group"] = "Hindi Movies"
        elif ENGLISH_MOVIE_CH_RE.search(lname):
            c["group"] = "English Movies"
        elif NETWORK_GROUP_RE.search(lname):
            c["group"] = classify_network_channel(lname)

        if any(b in c["url"] for b in BLOCK_URLS):
            continue
        if (c["source"] == "GitHub" and c["group"] in HINDI_REMAP
                and any(h in c["url"].lower() for h in NOT_BANGLA_HOSTS)):
            continue
        if c["group"] in HINDI_REMAP and HINDI_RE.search(lname) and not BANGLA_RE.search(lname):
            c["group"] = HINDI_REMAP[c["group"]]
        if STRICT_GROUPS and c["group"] not in GROUP_ORDER:
            continue
        seen.add(raw)
        items.append(c)
    log(f"Total unique video channel: {len(items)}")

    check_all(items)
    alive = [c for c in items if c.get("alive")]
    log(f"Alive: {len(alive)} / {len(items)}")

    prev = OUTPUT.read_text(encoding="utf-8", errors="ignore").count("#EXTINF") if OUTPUT.exists() else 0
    if prev >= 20 and len(alive) < prev * MIN_KEEP_RATIO:
        log(f"[SAFETY] Alive ({len(alive)}) ager playlist ({prev}) er theke onek kom. Overwrite kora holo na.")
        return 1

    if MAX_LINKS_PER_CHANNEL:
        before = len(alive)
        alive = dedupe_by_name(alive, MAX_LINKS_PER_CHANNEL)
        log(f"Duplicate naam bad: {before} -> {len(alive)}")

    alive.sort(key=lambda c: (GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99,
                              SOURCE_RANK.get(c["source"], 9),
                              c["name"].lower()))

    cnt = Counter(c["group"] for c in alive)
    log("Group wise channel: " + ", ".join(f"{g}: {n}" for g, n in cnt.most_common()))
    write_playlist(alive)
    elapsed = int(time.time() - start_time)
    log(f"{OUTPUT} e {len(alive)} ta alive video channel save hoise (Total time: {elapsed // 60}m {elapsed % 60}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
