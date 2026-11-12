"""
Standalone IPTV updater (kono GUI / iptv_core lagbe na).
Source: AynaOTT + JagoBD + GitHub (BD, IN [hin/ben], Movies/Music/Sports + Keyword Search) -> dead check -> shudhu alive rakhbe.
PRIORITY list er channel gula playlist er sobar upore thakbe (oi order e).
"""
import difflib, json, os, re, ssl, sys, time
from collections import Counter
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ------------------------------------------------------------------ settings
OUTPUT = Path("playlist.m3u")
WORKERS = 60                 # ek sathe koto link check hobe
CHECK_TIMEOUT = 8            # sekond
MAX_LINKS_PER_PRIORITY = 1   # top 39 channel er proti channel e max koto alive link (0 = shob)
MAX_LINKS_PER_CHANNEL = 1    # baki channel er proti naam e max koto link (1 = duplicate nei)
PRIORITY_KEEP_DEAD = True    # True: top channel er link GitHub e verify hoy ni (timeout/403, BD/BDIX server) tao 1 ta link rakhbe
KEEP_REASONS = ("timeout", "HTTP 401", "HTTP 403", "HTTP 451", "html page (block/login)")
REPORT = Path("priority_report.txt")
MIN_KEEP_RATIO = 0.5         # alive sonkhya ager playlist er 50% er niche hole overwrite korbe na

# Ekhane tomar GitHub Personal Access Token boshabe (jemon: "ghp_xxxxxxxxxxxx")
GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")

# GitHub e ei keyword gula diye .m3u / .m3u8 file search korbe
USE_GITHUB_SEARCH = True
GITHUB_SEARCH_QUERIES = [
    "EXTINF bangla m3u",
    "EXTINF bdix m3u8",
    "EXTINF kolkata bangla m3u",
    "EXTINF hindi movies m3u8",
]

IPTV_BASE = "https://iptv-org.github.io/iptv"

# Desh wise: Bangladesh (shob) & India (shudhu Hindi o Bangla)
INDIA_LANGS = ("hin", "ben")
COUNTRY_SOURCES = [
    ("Bangladesh", f"{IPTV_BASE}/countries/bd.m3u", None),
    ("India",      f"{IPTV_BASE}/countries/in.m3u", INDIA_LANGS),
]

# True dile: upore dewa group chhara onno group er channel (top 39 / manual chhara) playlist e rakhbe na
STRICT_GROUPS = True

# Category + Bhasha: duita lister common channel (group name, category, language code)
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
    "Kolkata Movies", "Hindi Movies", "English Movies", "Hindi Music", "English Music",
    "Bangla Music", "Sports",
    "Bangladesh", "Bangla (India)", "India"
]

# Tumi nijer jana link ekhane dao: (naam, link, verify)
# verify=False -> check korbe na, shobshomoy playlist e rakhbe (BDIX / token block er jonno best)
MANUAL_CHANNELS = [
    ("News 24", "https://tvsen6.aynaott.com/cdgr3tw6WoG7JyRnLbi0/index.m3u8", False),
    ("Channel 9", "https://app24.jagobd.com.bd/c3VydmVyX8RpbEU9Mi8xNy8yMDIxIDU6NDQ6MjcgUE0mdXNlcmlkPTI3MzgmdG9rZW49Y2hhbm5lbDk=/channel9-8.stream/playlist.m3u8", False),
    ("Nagorik TV", "https://app24.jagobd.com.bd/c3VydmVyX8RpbEU9Mi8xNy8yMDIxIDU6NDQ6MjcgUE0mdXNlcmlkPTI3MzgmdG9rZW49bmFnb3Jpay10dg==/nagoriktv-8.stream/playlist.m3u8", False),
]

# Ei link gula kokhono nibe na
BLOCK_URLS = [
    "amg13643-amg13643c1-amgplt0016",
]

NOT_BANGLA_HOSTS = ["amagi.tv"]

# Sobar upore ei channel gula (ei order e) thakbe
PRIORITY = [
    ("BTV", "BD"), ("BTV World", "BD"), ("BTV Chattogram", "BD"), ("Somoy TV", "BD"),
    ("Jamuna Television", "BD"), ("Ekattor TV", "BD"), ("Channel 24", "BD"), ("DBC News", "BD"),
    ("Independent Television", "BD"), ("ATN News", "BD"), ("News 24", "BD"),
    ("Channel i", "BD"), ("NTV", "BD"), ("RTV", "BD"), ("ATN Bangla", "BD"), ("Banglavision", "BD"),
    ("Maasranga Television", "BD"), ("Deepto TV", "BD"), ("Boishakhi TV", "BD"), ("Desh TV", "BD"),
    ("Nagorik TV", "BD"), ("GTV", "BD"), ("Asian TV", "BD"), ("SA TV", "BD"), ("Channel 9", "BD"),
    ("Mohona TV", "BD"), ("My TV", "BD"), ("Bijoy TV", "BD"), ("Ananda TV", "BD"),
    ("Global Television", "BD"), ("Nexus Television", "BD"),
    ("T Sports", "BD"), ("Duronto TV", "BD"), ("Gaan Bangla", "BD"),
    ("Star Jalsha", "IN"), ("Zee Bangla", "IN"), ("Colors Bangla", "IN"), ("Sony Aath", "IN"),
    ("Jalsha Movies", "IN"),
]

# Source e onno naame thakle
PRIORITY_ALIASES = {
    "BTV": ["BTV National", "BTV Dhaka"],
    "BTV Chattogram": ["BTV CTG", "BTV Chittagong"],
    "Somoy TV": ["Somoy", "Shomoy TV", "Samoy TV", "Somoy News"],
    "Nagorik TV": ["Nagorik", "Nagorik Television", "Nagarik TV", "Nagarik"],
    "Maasranga Television": ["Maasranga", "Maasranga TV"],
    "Boishakhi TV": ["Boishakhi", "Baishakhi TV", "Boishakhi Television"],
    "Deepto TV": ["Deepto", "Dipto TV", "Deepto Television"],
    "Ekattor TV": ["Ekattor", "Ekattor 71", "71 TV"],
    "DBC News": ["DBC", "DBC News TV"],
    "Channel 9": ["Channel Nine", "Channel9", "Channel 9 BD"],
    "Mohona TV": ["Mohona", "Mohona Television"],
    "Bijoy TV": ["Bijoy"],
    "Desh TV": ["Desh", "Desh Television"],
    "Duronto TV": ["Duronto"],
    "RTV": ["Rtv Bangla", "RTV Live"],
    "GTV": ["Gazi TV", "Gazi Television"],
    "Gaan Bangla": ["Gaan Bangla TV", "GB TV"],
}

GROUP_LABEL = {"BD": "Bangladesh", "IN": "Bangla (India)"}

# Adult content filter
BLOCK_WORDS = ["xxx", "adult", "porn", "18+", "erotic", "sex"]

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
                "source": source
            }
        elif meta and not line.startswith("#"):
            if re.match(r"^https?://", line):
                meta["url"] = line
                out.append(meta)
            meta = None
    return out


# ------------------------------------------------------------------ scrapers
def scrape_github():
    cache = {}

    def get(url):
        if url not in cache:
            try:
                cache[url] = parse_m3u(http_get(url, timeout=40), "GitHub")
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

    # 3) GitHub keyword code search
    if USE_GITHUB_SEARCH and GITHUB_TOKEN:
        h = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {GITHUB_TOKEN.strip()}"
        }
        raws = []
        for kw in GITHUB_SEARCH_QUERIES:
            for page in (1, 2):
                try:
                    api = ("https://api.github.com/search/code?q=" +
                           urllib.parse.quote(kw) + f"&per_page=30&page={page}")
                    data = json.loads(http_get(api, headers=h, timeout=20))
                    for it in data.get("items", []):
                        raws.append(it["html_url"].replace("https://github.com/",
                                    "https://raw.githubusercontent.com/").replace("/blob/", "/"))
                    time.sleep(6)  # GitHub search API rate-limit safety
                except Exception as e:
                    log(f"[GitHub search] '{kw}' page {page}: {e}")
                    break
        found = 0
        for raw in dict.fromkeys(raws):
            try:
                txt = http_get(raw, timeout=15)
                if "#EXTINF" in txt:
                    for c in parse_m3u(txt, "GitHub"):
                        c["search"] = True
                        res.append(c)
                        found += 1
            except Exception:
                pass
        log(f"[GitHub search] {len(dict.fromkeys(raws))} file theke {found} channel paoa gese")
    elif USE_GITHUB_SEARCH:
        log("[GitHub search] GITHUB_TOKEN boshano hoy ni, keyword search skip kora holo")
    return res


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
            d = json.loads(http_get(f"{base}/api/v5/browse/live-tvs/{slug}", headers=h, timeout=12))
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
    with ThreadPoolExecutor(max_workers=20) as ex:
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
            page = http_get(c["url"], timeout=12)
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
                eh = http_get(embed, headers={"User-Agent": UA, "Referer": c["url"]}, timeout=12)
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
    with ThreadPoolExecutor(max_workers=15) as ex:
        for f in as_completed([ex.submit(resolve, c) for c in cands]):
            r = f.result()
            if r:
                res.append(r)
    log(f"[JagoBD] stream URL: {len(res)}")
    return res


# ------------------------------------------------------------------ stream check
ALT_UAS = (UA, "VLC/3.0.18 LibVLC/3.0.18", "Lavf/60.3.100", "ExoPlayerLib/2.18.7")


def check_stream(raw_url, timeout=CHECK_TIMEOUT, attempts=2, uas=(UA,)):
    """(alive, latency_sec, reason)"""
    if not raw_url:
        return False, 0, "empty url"
    base, url, custom_ua = dict(HEADERS), raw_url, False
    if "|" in raw_url:
        url, extra = raw_url.split("|", 1)
        url = url.strip()
        for part in extra.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                base[k.strip()] = v.strip()
                custom_ua = custom_ua or k.strip().lower() == "user-agent"
    if "jagobd.com" in url and "Referer" not in base:
        base["Referer"] = "https://www.jagobd.com/"
    elif "aynaott.com" in url and "Referer" not in base:
        base["Referer"] = "https://web.aynaott.com/"

    reason = "unknown"
    for ua in ((None,) if custom_ua else uas):
        headers = dict(base)
        if ua:
            headers["User-Agent"] = ua
        for _ in range(attempts):
            t0 = time.time()
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                    ctype = r.headers.get("Content-Type", "").lower()
                    raw = r.read(2048)
                    low = raw.decode("utf-8", errors="ignore").strip().lower()
                    lat = time.time() - t0
                    if r.status not in (200, 206):
                        reason = f"HTTP {r.status}"
                    elif ("<html" in low or "<body" in low) and "#extm3u" not in low:
                        reason = "html page (block/login)"
                    elif any(t in low for t in ("#extm3u", "#ext-x-stream-inf", "#extinf",
                                                "#ext-x-targetduration", "#ext-x-media-sequence")):
                        return True, lat, "ok"
                    elif any(v in ctype for v in ("video/", "audio/", "mpegurl", "octet-stream")):
                        return True, lat, "ok"
                    elif raw and raw[0] == 0x47:
                        return True, lat, "ok"
                    elif (".m3u8" in url or ".ts" in url) and low and not low.startswith("<"):
                        return True, lat, "ok"
                    else:
                        reason = "not a stream"
                    break
            except urllib.error.HTTPError as e:
                reason = f"HTTP {e.code}"
                if e.code in (404, 410):
                    return False, 0, reason
                break
            except urllib.error.URLError as e:
                msg = str(e.reason)
                reason = "timeout" if "timed out" in msg.lower() else f"connect: {msg[:40]}"
            except TimeoutError:
                reason = "timeout"
            except Exception as e:
                reason = f"error: {type(e).__name__}"
            time.sleep(0.5)
    return False, 0, reason


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


def recheck(chs):
    if not chs:
        return 0
    revived = 0
    with ThreadPoolExecutor(max_workers=20) as ex:
        fut = {ex.submit(check_stream, c["url"], 20, 1, ALT_UAS): c for c in chs}
        for f in as_completed(fut):
            c = fut[f]
            try:
                ok, lat, reason = f.result()
            except Exception as e:
                ok, lat, reason = False, 0, f"error: {type(e).__name__}"
            c["reason"] = reason
            if ok:
                c["alive"], c["latency"] = True, lat
                revived += 1
    return revived


# ------------------------------------------------------------------ priority matching
NOISE = {"hd", "fhd", "sd", "uhd", "4k", "live", "official", "hevc", "h265", "h264"}


def norm(name):
    n = name.lower()
    n = re.sub(r"[\(\[].*?[\)\]]", " ", n)
    n = re.sub(r"\b\d{3,4}p\b", " ", n)
    n = n.replace("television", "tv")
    n = re.sub(r"[^a-z0-9\u0980-\u09ff]+", " ", n)
    return "".join(t for t in n.split() if t not in NOISE)


PRIORITY_MAP = {norm(n): (i, n, c) for i, (n, c) in enumerate(PRIORITY)}
ALIAS_TO_CANON = {norm(a): norm(canon) for canon, alts in PRIORITY_ALIASES.items() for a in alts}
SOURCE_RANK = {"Manual": -1, "AynaOTT": 0, "JagoBD": 1, "GitHub": 2}

SUFFIXES = ("bangladesh", "bangla", "tv", "bd")


def pkey(name):
    k = norm(name)
    for _ in range(3):
        if k in PRIORITY_MAP:
            return k
        if k in ALIAS_TO_CANON:
            return ALIAS_TO_CANON[k]
        for suf in SUFFIXES:
            if k.endswith(suf) and len(k) > len(suf):
                k = k[:-len(suf)]
                break
        else:
            return None
    if k in PRIORITY_MAP:
        return k
    return ALIAS_TO_CANON.get(k)


def is_blocked(c):
    t = f'{c["name"]} {c.get("group", "")}'.lower()
    return any(re.search(rf"\b{re.escape(w)}\b", t) for w in BLOCK_WORDS)


# ---- bhasha / group sanitize ----
BANGLA_RE = re.compile(r"bangla|bengali|bengal|bangladesh|\bbd\b|jalsha|\baath\b|ananda|kolkata|calcutta|ghanta|ebela|sangeet bangla")
HINDI_RE = re.compile(
    r"\bhindi\b|bollywood|star plus|star bharat|star utsav|star gold|sony sab|\bsab tv\b|sony tv|sony entertainment|"
    r"sony pal|sony max|sony pix|sony wah|\bcolors\b|colors tv|colors rishtey|colors cineplex|zee tv|zee anmol|zee cinema|"
    r"zee classic|zee action|zee bollywood|zee news|zee hindustan|&tv|\band tv\b|and pictures|&pictures|dangal|ishara|shemaroo|"
    r"big magic|bindass|aaj tak|abp news|ndtv|india tv|news18 india|republic bharat|tv9 bharat|dd national|dd news|dd bharati|"
    r"sanskar|aastha|sadhna|bhojpuri|b4u|mtv india|9xm|9x jalwa|zing|utv|filamchi|goldmines|manoranjan|cineplex|"
    r"news nation|india news|good news today|bharat 24|\btez\b|zoom tv|mastiii|movie plus|enter10|bflix")
OTHER_LANG_RE = re.compile(
    r"tamil|telugu|malayalam|kannada|marathi|punjabi|gujarati|odia|oriya|assamese|konkani|"
    r"sun tv|vijay|asianet|surya|kairali|maa tv|etv telugu|gemini|raj tv|jaya tv|polimer|"
    r"arabic|russian|french|german|spanish|turkish|italian|portuguese|polish|greek|dutch|swedish|ukrainian|romanian|"
    r"farsi|persian|hebrew|japanese|chinese|mandarin|korean|thai|vietnamese|indonesian|tagalog|filipino|"
    r"al jazeera|al arabiya|\bnhk\b|\bcctv\b|\bkbs\b|\bmbc\b|telemundo|univision")

HINDI_REMAP = {"Bangladesh": "India", "Kolkata Movies": "Hindi Movies", "Bangla Music": "Hindi Music"}


def bd_group(name):
    return "Bangla Music" if re.search(r"\b(music|gaan|song|sangeet)\b", name.lower()) else "Bangladesh"


KOLKATA_RE = re.compile(r"\b(jalsha movies|zee bangla cinema|colors bangla cinema|bangla cinema|"
                        r"bangla movies?|bengali movies?|bengali cinema)\b")


def keyword_group(name, group=""):
    t = f"{name} {group}".lower()
    if KOLKATA_RE.search(t):
        return "Kolkata Movies"
    music = re.search(r"\b(music|song|songs|gaan|mtv)\b", t)
    movie = re.search(r"\b(movie|movies|cinema|film|films|flix|hbo|bollywood|hollywood)\b", t)
    sport = re.search(r"\b(sport|sports|cricket|football)\b", t)
    bn = re.search(r"\b(bangla|bengali)\b", t)
    hi = re.search(r"\b(hindi|bollywood)\b", t)
    en = re.search(r"\b(english|hollywood|hbo)\b", t)
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
    if hi:
        return "India"
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


def short(u, n=60):
    u = u.split("|")[0]
    return u if len(u) <= n else u[:n] + "..."


def priority_report(items):
    lines, problems = [], []
    for idx, (name, _cc) in enumerate(PRIORITY):
        key = norm(name)
        exact = [c for c in items if pkey(c["name"]) == key]
        ok = [c for c in exact if c.get("alive")]
        if ok:
            b = min(ok, key=lambda c: (SOURCE_RANK.get(c["source"], 9), c.get("latency", 99)))
            lines.append(f"OK        {name}: {len(ok)}/{len(exact)} link alive (best: {b['source']})")
        elif exact:
            det = " | ".join(f"{c['source']}: {c.get('reason', '?')} [{short(c['url'])}]" for c in exact[:5])
            msg = f"DEAD      {name}: {len(exact)} link paoa gese, shob dead -> {det}"
            lines.append(msg)
            problems.append(msg)
        else:
            sim = []
            for c in items:
                k2 = norm(c["name"])
                if not k2:
                    continue
                if (len(key) >= 3 and len(k2) >= 3 and (key in k2 or k2 in key)) or \
                        difflib.SequenceMatcher(None, key, k2).ratio() >= 0.8:
                    sim.append(c)
            det = " | ".join(f"'{c['name']}' [{c['source']}, {'alive' if c.get('alive') else c.get('reason', '?')}]"
                             for c in sim[:5]) or "kisu nai"
            msg = f"NOT FOUND {name}: kono source e ei naam e nai. Kachakachi naam: {det}"
            lines.append(msg)
            problems.append(msg)
    try:
        REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception:
        pass
    return problems


# ------------------------------------------------------------------ output
def write_playlist(top, rest):
    def q(s):
        return str(s).replace('"', "'").replace("\n", " ")
    lines = ["#EXTM3U"]
    for c in top + rest:
        lines.append(f'#EXTINF:-1 tvg-id="{q(c.get("tvg_id", ""))}" tvg-name="{q(c["name"])}" '
                     f'tvg-logo="{q(c.get("logo", ""))}" group-title="{q(c["group"])}",{c["name"]}')
        lines.append(c["url"])
    OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    pool = []
    for name, fn in (("AynaOTT", scrape_aynaott), ("JagoBD", scrape_jagobd), ("GitHub", scrape_github)):
        try:
            pool.extend(fn())
        except Exception as e:
            log(f"[{name}] error: {e}")

    manual = [{
        "name": n, "url": u, "group": "Others", "logo": "", "tvg_id": "", "source": "Manual",
        "manual": True, "verify": v
    } for n, u, v in MANUAL_CHANNELS]
    if manual:
        log(f"[Manual] {len(manual)} ta nijer dewa link")
    pool = manual + pool

    seen, items = set(), []
    for c in pool:
        raw = c["url"].split("|")[0].strip()
        if not raw or raw in seen or is_blocked(c):
            continue
        name, lname = c["name"], c["name"].lower()
        is_top = pkey(name) is not None
        if c["source"] in ("AynaOTT", "JagoBD"):
            c["group"] = bd_group(name)
        elif c.get("search"):
            kw = keyword_group(name, c.get("group", ""))
            if kw:
                c["group"] = kw
            elif not is_top:
                continue
        if KOLKATA_RE.search(lname):
            c["group"] = "Kolkata Movies"
        if any(b in c["url"] for b in BLOCK_URLS) and not c.get("manual"):
            continue
        if (c["source"] == "GitHub" and (is_top or c["group"] in HINDI_REMAP)
                and any(h in c["url"].lower() for h in NOT_BANGLA_HOSTS)):
            continue
        if not is_top and not c.get("manual"):
            if OTHER_LANG_RE.search(lname):
                continue
            if c["group"] in HINDI_REMAP and HINDI_RE.search(lname) and not BANGLA_RE.search(lname):
                c["group"] = HINDI_REMAP[c["group"]]
            if STRICT_GROUPS and c["group"] not in GROUP_ORDER:
                continue
        seen.add(raw)
        items.append(c)
    log(f"Total unique channel: {len(items)}")

    for c in items:
        if c.get("manual") and not c.get("verify"):
            c["alive"], c["latency"], c["reason"] = True, 0, "manual (not verified)"
    check_all([c for c in items if not (c.get("manual") and not c.get("verify"))])
    prio_dead = [c for c in items if not c.get("alive") and pkey(c["name"]) is not None]
    log(f"Top channel er {len(prio_dead)} ta dead link abar dhoirjo niye check kora hocche...")
    log(f"Abar check e {recheck(prio_dead)} ta chalu hoise")
    alive = [c for c in items if c.get("alive")]
    log(f"Alive: {len(alive)} / {len(items)}")

    problems = priority_report(items)
    for line in problems:
        log(line)

    prev = OUTPUT.read_text(encoding="utf-8", errors="ignore").count("#EXTINF") if OUTPUT.exists() else 0
    if prev >= 20 and len(alive) < prev * MIN_KEEP_RATIO:
        log(f"[SAFETY] Alive ({len(alive)}) ager playlist ({prev}) er theke onek kom. Overwrite kora holo na.")
        return 1

    # Priority bucket
    buckets, rest = {}, []
    for c in alive:
        hit = PRIORITY_MAP.get(pkey(c["name"]) or "")
        if hit:
            idx, canon, cc = hit
            c["name"], c["group"] = canon, GROUP_LABEL[cc]
            buckets.setdefault(idx, []).append(c)
        else:
            rest.append(c)

    if PRIORITY_KEEP_DEAD:
        for idx, (name, cc) in enumerate(PRIORITY):
            if idx in buckets:
                continue
            key = norm(name)
            cands = [c for c in items if not c.get("alive") and pkey(c["name"]) == key
                     and c.get("reason") in KEEP_REASONS]
            if cands:
                def keep_rank(c):
                    flat = re.sub(r"[^a-z0-9]", "", c["url"].split("|")[0].lower())
                    return (0 if key in flat else 1, SOURCE_RANK.get(c["source"], 9))
                best = min(cands, key=keep_rank)
                best["name"], best["group"] = name, GROUP_LABEL[cc]
                buckets[idx] = [best]
                log(f"[KEEP] {name}: verify hoy ni ({best.get('reason')}), 1 ta link rakha holo: {short(best['url'])}")

    top = []
    for idx in sorted(buckets):
        lst = sorted(buckets[idx], key=lambda c: (SOURCE_RANK.get(c["source"], 9), c.get("latency", 99)))
        top.extend(lst[:MAX_LINKS_PER_PRIORITY] if MAX_LINKS_PER_PRIORITY else lst)
    if MAX_LINKS_PER_CHANNEL:
        before = len(rest)
        rest = dedupe_by_name(rest, MAX_LINKS_PER_CHANNEL)
        log(f"Duplicate naam bad: {before} -> {len(rest)}")

    rest.sort(key=lambda c: GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99)

    missing = [n for i, (n, _c) in enumerate(PRIORITY) if i not in buckets]
    log(f"Priority: {len(buckets)}/{len(PRIORITY)} channel alive paoa gese")
    if missing:
        log("Priority te alive paoa jay ni: " + ", ".join(missing))

    cnt = Counter(c["group"] for c in top + rest)
    log("Group wise channel: " + ", ".join(f"{g}: {n}" for g, n in cnt.most_common()))
    write_playlist(top, rest)
    log(f"{OUTPUT} e {len(top) + len(rest)} ta channel save hoise ({len(top)} ta top e)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
