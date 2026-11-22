"""
All-in-One Direct (No-Proxy) Live TV IPTV Updater (BD + Indian + English Only)
Features:
- Keeps ONLY Bangladeshi, Indian (all regional languages), and English channels (drops International/foreign junk)
- Strictly separates Bangladeshi ('Bangladesh') from Indian Bangla ('Bangla (India)' & 'Kolkata Movies')
- Blocks expired/soon-to-expire tokens, VOD/movies, radio, and gibberish/Amagi ID names
- Deep HLS segment verification (requires >= 5KB real video chunk)
- Live token scrapers for AynaOTT & JagoBD + 'discovered_sources.json' + IPTV-org
- Generates TWO playlists:
  1. 'playlist.m3u': All verified alive BD, Indian & English streams (with backups)
  2. 'playlist_clean.m3u': Duplicate-free, lowest ping (fastest response) 1 link per channel
"""
import json, os, re, socket, ssl, sys, time
from collections import Counter
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

socket.setdefaulttimeout(8)

# ------------------------------------------------------------------ settings
OUTPUT = Path("playlist.m3u")
CLEAN_OUTPUT = Path("playlist_clean.m3u")
DISCOVERED_FILE = Path("discovered_sources.json")
WORKERS = 150
CHECK_TIMEOUT = 7
MAX_LINKS_PER_CHANNEL = None
MIN_KEEP_RATIO = 0.1

GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")
IPTV_BASE = "https://iptv-org.github.io/iptv"

COUNTRY_SOURCES = [
    ("Bangladesh", f"{IPTV_BASE}/countries/bd.m3u"),
    ("India",      f"{IPTV_BASE}/countries/in.m3u"),
]

COMBOS = [
    ("Kolkata Movies",        "movies",        "ben"),
    ("Hindi Movies",          "movies",        "hin"),
    ("English Movies",        "movies",        "eng"),
    ("English Entertainment", "entertainment", "eng"),
    ("English Entertainment", "series",        "eng"),
    ("English Entertainment", "documentary",   "eng"),
    ("Hindi Music",           "music",         "hin"),
    ("English Music",         "music",         "eng"),
    ("Bangla Music",          "music",         "ben"),
    ("Sports",                "sports",        None),
]

# শুধুমাত্র অনুমোদিত বাংলাদেশি, ভারতীয় এবং ইংরেজি গ্রুপসমূহ (এর বাইরের সব চ্যানেল বাদ যাবে)
ALLOWED_GROUPS = {
    "Bangladesh", "Bangla (India)", "Kolkata Movies", "Bangla Music",
    "Hindi Movies", "Hindi Music", "India",
    "English Movies", "English Entertainment", "English Music",
    "Sports", "Tamil", "Telugu", "Malayalam", "Kannada",
    "Marathi", "Punjabi", "Gujarati", "Odia", "Assamese"
}

GROUP_ORDER = [
    "Bangladesh", "Bangla (India)", "Kolkata Movies", "Bangla Music",
    "Hindi Movies", "Hindi Music", "India",
    "English Movies", "English Entertainment", "English Music",
    "Sports", "Tamil", "Telugu", "Malayalam", "Kannada",
    "Marathi", "Punjabi", "Gujarati", "Odia", "Assamese"
]

BLOCK_URLS = [
    "amg13643-amg13643c1-amgplt0016",
    "amgplt",
]

NOT_BANGLA_HOSTS = ["amagi.tv"]

EXPIRED_TOKEN_URL_RE = re.compile(
    r"(\?|&)(token|hdnts|wmsAuthSign|signature|auth|expires|policy|key-pair-id)=",
    re.I
)

BLOCK_WORDS = ["xxx", "adult", "porn", "18+", "erotic", "sex"]
RADIO_RE = re.compile(
    r"\b(radio|fm|am\s*radio|podcast|audio\s*only|mirchi|foorti|shadhin|bhoot\s*fm|"
    r"dhaka\s*fm|abc\s*radio|peoples\s*radio|today\s*fm|jago\s*fm|air\s*vividh|akashvani|"
    r"red\s*fm|big\s*fm|radio\s*city|radio\s*one|fever\s*fm|my\s*fm|ishq\s*fm)\b",
    re.I
)
AUDIO_EXT_RE = re.compile(r"\.(mp3|aac|ogg|oga|wav|flac|m4a|wma|opus|pls|asx)(\?|$)", re.I)

VOD_EXT_RE = re.compile(r"\.(mp4|mkv|avi|mov|wmv|flv|webm|m4v|mpg|mpeg|3gp|vob)(\?|$)", re.I)
VOD_PATH_RE = re.compile(r"/(vod|movies?|films?|series|episodes?|season|natok|drama)/", re.I)
VOD_NAME_RE = re.compile(
    r"(\(\s*(19|20)\d{2}\s*\)|\[\s*(19|20)\d{2}\s*\]|\b(19|20)\d{2}\b.*\b(720p|1080p|480p|hdrip|web-?dl|bluray|dvdrip)\b|"
    r"\b(dual\s*audio|hindi\s*dubbed|bengali\s*dubbed|web-?dl|hdrip|bluray|brrip|dvdrip|camrip|hdts|uncut|"
    r"full\s*movie|full\s*natok|episode\s*\d+|s\d{1,2}\s*e\d{1,2}|season\s*\d+)\b)",
    re.I
)

KEY_URI_RE = re.compile(r'#EXT-X-KEY:[^\n]*URI="([^"]+)"', re.I)
M3U8_AUDIO_LANG_RE = re.compile(r'#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*LANGUAGE="([^"]+)"', re.I)
XTREAM_NUM_RE = re.compile(r"^(https?://[^/]+(?:/live)?/[^/]+/[^/]+/\d+)(?:\.ts)?$", re.I)

ISO_LANG_MAP = {
    "ben": "ben", "bn": "ben", "bengali": "ben", "bangla": "ben",
    "hin": "hin", "hi": "hin", "hindi": "hin",
    "eng": "eng", "en": "eng", "english": "eng",
    "tam": "tam", "ta": "tam", "tamil": "tam",
    "tel": "tel", "te": "tel", "telugu": "tel",
    "mal": "mal", "ml": "mal", "malayalam": "mal",
    "kan": "kan", "kn": "kan", "kannada": "kan",
    "mar": "mar", "mr": "mar", "marathi": "mar",
    "pan": "pan", "pa": "pan", "punjabi": "pan",
    "guj": "guj", "gu": "guj", "gujarati": "guj",
    "ori": "ori", "or": "ori", "odia": "ori", "oriya": "ori",
    "asm": "asm", "as": "asm", "assamese": "asm",
    "urd": "urd", "ur": "urd", "urdu": "urd",
    "ara": "ara", "ar": "ara", "arabic": "ara",
}

# ------------------------------------------------------------------ http
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/129.0.0.0 Safari/537.36")
PAY_UAS = (
    UA,
    "Dalvik/2.1.0 (Linux; Android 11) ExoPlayerLib/2.18.7",
)
HEADERS = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}


def log(msg):
    print(msg, flush=True)


def http_get(url, headers=None, timeout=6, max_bytes=1_500_000):
    req = urllib.request.Request(url, headers=headers or HEADERS)
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        return r.read(max_bytes).decode("utf-8", errors="ignore")


# ------------------------------------------------------------------ URL converter & m3u parse
def convert_to_hls_url(url):
    if not url:
        return url
    if "output=ts" in url.lower():
        return re.sub(r"output=ts", "output=m3u8", url, flags=re.I)
    if "output=mpegts" in url.lower():
        return re.sub(r"output=mpegts", "output=m3u8", url, flags=re.I)
    base_part = url.split("?")[0]
    m = XTREAM_NUM_RE.match(base_part)
    if m:
        qs = ("?" + url.split("?", 1)[1]) if "?" in url else ""
        return f"{m.group(1)}.m3u8{qs}"
    return url


def normalize_lang_code(raw_lang):
    if not raw_lang:
        return ""
    for token in re.split(r"[,;/\s-]+", raw_lang.strip().lower()):
        if token in ISO_LANG_MAP:
            return ISO_LANG_MAP[token]
    return raw_lang.strip().lower()[:3]


def parse_m3u(text, source):
    out, meta = [], None
    vlc_ua, vlc_ref = "", ""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            def attr(k):
                m = re.search(rf'{k}="([^"]*)"', line, re.I)
                return m.group(1).strip() if m else ""
            name = line.rsplit(",", 1)[-1].strip() if "," in line else attr("tvg-name")
            raw_lang = attr("tvg-language") or attr("language")
            meta = {
                "name": name or attr("tvg-name") or "Unknown",
                "tvg_id": attr("tvg-id"),
                "logo": attr("tvg-logo"),
                "group": attr("group-title") or "Others",
                "lang": normalize_lang_code(raw_lang),
                "source": source,
                "radio_tag": bool(re.search(r'radio="true"', line, re.I))
            }
            vlc_ua, vlc_ref = "", ""
        elif line.startswith("#EXTVLCOPT:"):
            opt = line.split(":", 1)[1].strip()
            if opt.lower().startswith("http-user-agent="):
                vlc_ua = opt.split("=", 1)[1].strip()
            elif opt.lower().startswith("http-referrer="):
                vlc_ref = opt.split("=", 1)[1].strip()
        elif meta and not line.startswith("#"):
            if re.match(r"^https?://", line):
                if "|" in line:
                    u_part, h_part = line.split("|", 1)
                    line = f"{convert_to_hls_url(u_part.strip())}|{h_part}"
                else:
                    line = convert_to_hls_url(line)
                    if vlc_ua or vlc_ref:
                        parts = []
                        if vlc_ua:
                            parts.append(f"User-Agent={vlc_ua}")
                        if vlc_ref:
                            parts.append(f"Referer={vlc_ref}")
                        line = f"{line}|{'&'.join(parts)}"
                meta["url"] = line
                out.append(meta)
            meta = None
            vlc_ua, vlc_ref = "", ""
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
                                       headers=h, timeout=12))
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
            d = json.loads(http_get(f"{base}/api/v5/browse/live-tvs/{slug}", headers=h, timeout=8))
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
                    "group": "Others",
                    "lang": "",
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
        html = http_get("https://www.jagobd.com/", timeout=12)
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
            page = http_get(c["url"], timeout=8)
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
                eh = http_get(embed, headers={"User-Agent": UA, "Referer": c["url"]}, timeout=8)
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
                    "group": "Others",
                    "lang": "",
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


def scrape_discovered_sources():
    if not DISCOVERED_FILE.exists():
        log("[DiscoveredSources] discovered_sources.json paoa jay ni, skip kora holo")
        return []

    try:
        data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"[DiscoveredSources] JSON read error: {e}")
        return []

    upstream_urls = data.get("all_upstream_sources", [])
    bot_playlists = data.get("all_bot_playlists", [])
    log(f"[DiscoveredSources] {len(upstream_urls)} ti upstream source o {len(bot_playlists)} ti bot feed check kora hocche...")

    res = []

    def extract_name_from_url(url):
        try:
            path = urllib.parse.urlparse(url).path
            parts = [p for p in path.split("/") if p]
            skip_words = {
                "index.m3u8", "playlist.m3u8", "master.m3u8", "live.m3u8",
                "chunklist.m3u8", "800.m3u8", "720p.m3u8", "1080p.m3u8",
                "480p.m3u8", "hls", "live", "stream", "streams", "tv",
                "preview", "tracks-v1a1", "mono.m3u8"
            }
            for p in reversed(parts):
                low = p.lower()
                if low in skip_words or low.isdigit():
                    continue
                clean = re.sub(r"\.m3u8?$", "", p, flags=re.I)
                clean = re.sub(r"([a-z])([A-Z])", r"\1 \2", clean)
                clean = re.sub(r"[-_]+", " ", clean).strip()
                if len(clean) >= 3 and not clean.isdigit():
                    return clean.title()
        except Exception:
            pass
        return None

    def fetch_upstream(u):
        out = []
        try:
            txt = http_get(u, timeout=5, max_bytes=1_000_000)
            if "#EXTINF" in txt:
                chs = parse_m3u(txt, "GitHub")
                if len(chs) <= 3000:
                    return chs
                return []
            links = re.findall(r'[\'"](https?://[^\'"]+?\.m3u8[^\'"]*)[\'"]', txt)
            for lk in dict.fromkeys(links):
                ch_name = extract_name_from_url(lk)
                if ch_name:
                    out.append({
                        "name": ch_name,
                        "url": convert_to_hls_url(lk),
                        "logo": "",
                        "tvg_id": "",
                        "group": "Others",
                        "lang": "",
                        "source": "GitHub"
                    })
        except Exception:
            pass
        return out

    def fetch_bot_pl(u):
        try:
            txt = http_get(u, timeout=6, max_bytes=1_500_000)
            if "#EXTINF" in txt:
                chs = parse_m3u(txt, "GitHub")
                if len(chs) <= 3000:
                    return chs
        except Exception:
            pass
        return []

    total_sources = len(upstream_urls) + len(bot_playlists)
    done_sources = 0
    with ThreadPoolExecutor(max_workers=120) as ex:
        futs = [ex.submit(fetch_upstream, u) for u in upstream_urls] + \
               [ex.submit(fetch_bot_pl, p) for p in bot_playlists]
        for f in as_completed(futs):
            res.extend(f.result())
            done_sources += 1
            if done_sources % 300 == 0 or done_sources == total_sources:
                log(f"[DiscoveredSources] progress: {done_sources}/{total_sources} source checked | {len(res)} channel paoa gese")

    log(f"[DiscoveredSources] mot {len(res)} ti candidate channel paoa gese")
    return res


def scrape_adaptive_repos():
    res = []
    adaptive_repos = [
        ("amazeyourself", "adaptive-streams", "main"),
    ]
    h = {"User-Agent": UA, "Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN.strip()}"

    for owner, repo, branch in adaptive_repos:
        try:
            tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
            data = json.loads(http_get(tree_url, headers=h, timeout=12))
            count = 0
            for item in data.get("tree", []):
                path = item.get("path", "")
                if item.get("type") == "blob" and path.endswith(".m3u8"):
                    fname = path.rsplit("/", 1)[-1].replace(".m3u8", "")
                    ch_name = re.sub(r"([a-z])([A-Z])", r"\1 \2", fname).replace("_", " ").replace("-", " ").strip()
                    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{urllib.parse.quote(path)}"
                    res.append({
                        "name": ch_name,
                        "url": raw_url,
                        "logo": "",
                        "tvg_id": f"{repo}-{fname.lower()}",
                        "group": "Others",
                        "lang": "",
                        "source": "GitHub"
                    })
                    count += 1
            log(f"[{owner}/{repo}] {count} ta adaptive stream file paoa gese")
        except Exception as e:
            log(f"[{owner}/{repo}] error: {e}")
    return res


def scrape_iptv_org_base():
    cache = {}

    def get(url):
        if url not in cache:
            try:
                cache[url] = parse_m3u(http_get(url, timeout=15, max_bytes=2_000_000), "GitHub")
            except Exception as e:
                log(f"[IPTV-org] fail {url}: {e}")
                cache[url] = []
        return cache[url]

    url_lang_map = {}
    for lg in ("ben", "hin", "eng", "tam", "tel", "mal", "kan", "mar", "pan", "guj", "ori", "asm"):
        for c in get(f"{IPTV_BASE}/languages/{lg}.m3u"):
            url_lang_map[c["url"].split("|")[0]] = lg

    res = []
    for group, cat, lang in COMBOS:
        langs = (lang,) if isinstance(lang, str) else (lang if lang else None)
        lang_urls = {c["url"].split("|")[0] for lg in langs for c in get(f"{IPTV_BASE}/languages/{lg}.m3u")} if langs else None
        for c in get(f"{IPTV_BASE}/categories/{cat}.m3u"):
            clean_u = c["url"].split("|")[0]
            if lang_urls is None or clean_u in lang_urls:
                detected_lg = url_lang_map.get(clean_u, c.get("lang", ""))
                res.append({**c, "group": group, "lang": detected_lg})

    for group, url in COUNTRY_SOURCES:
        for c in get(url):
            clean_u = c["url"].split("|")[0]
            detected_lg = url_lang_map.get(clean_u, "ben" if group == "Bangladesh" else c.get("lang", ""))
            res.append({**c, "group": "Sports" if "sport" in c.get("group", "").lower() else group, "lang": detected_lg})

    log(f"[IPTV-org] mot {len(res)} official channel load hoyeche")
    return res


# ------------------------------------------------------------------ stream check & M3U8 audio lang detection
def detect_m3u8_audio_lang(m3u8_text):
    langs = M3U8_AUDIO_LANG_RE.findall(m3u8_text)
    if not langs:
        return ""
    normed = [normalize_lang_code(lg) for lg in langs if lg.strip()]
    for pref in ("ben", "hin", "eng", "tam", "tel", "mal", "kan", "mar", "pan", "guj", "ori", "asm"):
        if pref in normed:
            return pref
    return normed[0] if normed else ""


def check_hls_key(m3u8_text, base_url, headers, timeout=4):
    low = m3u8_text.lower()
    if "#ext-x-key" not in low or "method=none" in low:
        return True
    m = KEY_URI_RE.search(m3u8_text)
    if not m:
        return False
    key_url = urllib.parse.urljoin(base_url, m.group(1).strip())
    try:
        req = urllib.request.Request(key_url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return r.status in (200, 206) and len(r.read(64)) == 16
    except Exception:
        return False


def verify_deep_hls(m3u8_text, base_url, headers, timeout=5):
    if not check_hls_key(m3u8_text, base_url, headers, timeout=4):
        return False

    lines = [ln.strip() for ln in m3u8_text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return False

    for candidate in lines[:3]:
        target_url = urllib.parse.urljoin(base_url, candidate)
        if VOD_EXT_RE.search(target_url):
            continue
        try:
            req = urllib.request.Request(target_url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                if r.status not in (200, 206):
                    continue
                chunk = r.read(8192)
                low = chunk.decode("utf-8", errors="ignore").lower()
                if "#ext-x-endlist" in low:
                    return False
                if any(drm in low for drm in ("sample-aes", "widevine", "playready", "skd://")):
                    continue
                if "#extm3u" in low:
                    sub_text = chunk.decode("utf-8", errors="ignore")
                    if not check_hls_key(sub_text, target_url, headers, timeout=4):
                        continue
                    sub_lines = [ln.strip() for ln in sub_text.splitlines()
                                 if ln.strip() and not ln.strip().startswith("#")]
                    if not sub_lines:
                        continue
                    seg_url = urllib.parse.urljoin(target_url, sub_lines[0])
                    req2 = urllib.request.Request(seg_url, headers=headers)
                    with urllib.request.urlopen(req2, timeout=timeout, context=SSL_CTX) as r2:
                        seg_data = r2.read(16384)
                        if r2.status in (200, 206) and len(seg_data) >= 5000 and not seg_data[:20].lstrip().startswith(b"<"):
                            return True
                elif len(chunk) >= 5000 and not chunk[:20].lstrip().startswith(b"<"):
                    return True
        except Exception:
            continue
    return False


def check_stream(raw_url, timeout=CHECK_TIMEOUT):
    if not raw_url:
        return False, 0, "empty url", UA, ""
    base, url, custom_ua = dict(HEADERS), raw_url, False
    if "|" in raw_url:
        url, extra = raw_url.split("|", 1)
        url = url.strip()
        for part in extra.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                base[k.strip()] = v.strip()
                if k.strip().lower() == "user-agent":
                    custom_ua = True

    exp_m = re.search(r"(?:exp|expires)=(\d{10})", url, re.I)
    if exp_m and int(exp_m.group(1)) < int(time.time()) + 7200:
        return False, 0, "token expired or expiring soon", UA, ""

    if AUDIO_EXT_RE.search(url):
        return False, 0, "radio/audio file", UA, ""
    if VOD_EXT_RE.search(url) or VOD_PATH_RE.search(url):
        return False, 0, "VOD/movie file", UA, ""

    if "jagobd.com" in url and "Referer" not in base:
        base["Referer"] = "https://www.jagobd.com/"
    elif "aynaott.com" in url and "Referer" not in base:
        base["Referer"] = "https://web.aynaott.com/"

    uas = (base.get("User-Agent", UA),) if custom_ua else PAY_UAS
    reason = "unknown"
    for idx, ua in enumerate(uas):
        headers = dict(base)
        headers["User-Agent"] = ua
        t0 = time.time()
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                final_url = r.geturl() or url
                if VOD_EXT_RE.search(final_url):
                    return False, 0, "VOD redirect", ua, ""

                ctype = r.headers.get("Content-Type", "").lower()
                icy_name = r.headers.get("icy-name") or r.headers.get("icy-genre")
                if icy_name or ctype.startswith("audio/"):
                    return False, 0, "radio/audio stream", ua, ""
                if any(v in ctype for v in ("video/mp4", "video/x-matroska", "video/avi", "video/webm")):
                    return False, 0, "direct movie file", ua, ""

                raw = r.read(6144)
                text_body = raw.decode("utf-8", errors="ignore")
                low = text_body.strip().lower()
                lat = time.time() - t0

                if r.status not in (200, 206):
                    reason = f"HTTP {r.status}"
                    continue
                if ("<html" in low or "<body" in low) and "#extm3u" not in low:
                    reason = "html page (block/login)"
                    continue

                if "#ext-x-endlist" in low:
                    return False, 0, "VOD m3u8 (ENDLIST)", ua, ""

                if any(drm in low for drm in ("sample-aes", "widevine", "playready", "skd://",
                                              "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed")):
                    return False, 0, "DRM encrypted", ua, ""

                if "#extm3u" in low and AUDIO_EXT_RE.search(low) and ".ts" not in low and "#ext-x-stream-inf" not in low:
                    return False, 0, "audio-only m3u8", ua, ""

                if "#extm3u" in low:
                    if any(tag in low for tag in ("#ext-x-stream-inf", "#extinf", "#ext-x-targetduration")):
                        if verify_deep_hls(text_body, final_url, headers, timeout=5):
                            stream_lang = detect_m3u8_audio_lang(text_body)
                            return True, lat, "ok", ua, stream_lang
                        return False, 0, "dead/corrupt HLS segment", ua, ""
                    reason = "empty/invalid m3u8"
                    continue

                if raw and raw[0] == 0x47:
                    return False, 0, "raw ts stream (not m3u8)", ua, ""
                if ".ts" in final_url.lower():
                    return False, 0, "raw ts stream (not m3u8)", ua, ""
                reason = "not an m3u8 live stream"
        except urllib.error.HTTPError as e:
            reason = f"HTTP {e.code}"
            if e.code in (404, 410):
                return False, 0, reason, ua, ""
        except urllib.error.URLError as e:
            msg = str(e.reason)
            reason = "timeout" if "timed out" in msg.lower() else f"connect: {msg[:35]}"
            if idx == 0 and reason != "timeout":
                break
        except TimeoutError:
            reason = "timeout"
        except Exception as e:
            reason = f"error: {type(e).__name__}"
            break
    return False, 0, reason, UA, ""


def check_all(channels):
    total, done = len(channels), 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut = {ex.submit(check_stream, c["url"]): c for c in channels}
        for f in as_completed(fut):
            c = fut[f]
            try:
                ok, lat, reason, matched_ua, stream_lang = f.result()
            except Exception as e:
                ok, lat, reason, matched_ua, stream_lang = False, 0, f"error: {type(e).__name__}", UA, ""
            c["alive"], c["latency"], c["reason"], c["matched_ua"] = ok, lat, reason, matched_ua
            if stream_lang:
                c["stream_lang"] = stream_lang
            done += 1
            if done % 500 == 0 or done == total:
                log(f"checked {done}/{total} | alive {sum(1 for x in channels if x.get('alive'))}")

    dead = Counter(c["reason"] for c in channels if not c.get("alive") and c.get("reason"))
    log("Dead/Invalid er karon: " + ", ".join(f"{k}: {v}" for k, v in dead.most_common(8)))


# ------------------------------------------------------------------ original name filter, language grouping & dedupe
NOISE = {"hd", "fhd", "sd", "uhd", "4k", "live", "official", "hevc", "h265", "h264"}
SOURCE_RANK = {"AynaOTT": 0, "JagoBD": 1, "GitHub": 2}

VALID_SHORT_NAMES = {
    "bbc", "cnn", "ndtv", "mtv", "btv", "rtv", "ntv", "gtv", "atn", "dbc", "satv",
    "hbo", "axn", "tlc", "fx", "fxx", "wb", "cbs", "nbc", "abc", "itv", "ptv", "ary",
    "geo", "dd", "wion", "cnbc", "espn", "tnt", "tbs", "amc", "vtv", "otv", "ktv",
    "mh1", "9xm", "e24", "b4u", "tv9", "tv5", "sky", "fox", "pbs", "hln", "cw", "id",
    "osn", "mbc", "dw", "nhk", "cgtn", "rt", "trt", "tcm", "mgm", "mnx", "vh1", "zing"
}

GARBAGE_EXACT_RE = re.compile(
    r"^(unknown|untitled|no\s*name|none|null|test|testing|demo|sample|offline|error|"
    r"live|stream|live\s*stream|tv|channel|ch|server|link|backup|main|default|"
    r"index|playlist|master|chunklist|mono|tracks|video|audio|hd|fhd|sd|uhd|4k|"
    r"720p|1080p|480p|360p|hls|m3u8|ts|mp4)(\s*[-_#]?\s*\d+)?$",
    re.I
)


def is_garbage_channel_name(name):
    if not name:
        return True
    clean = re.sub(r"[\(\[].*?[\)\]]", " ", name).strip()
    clean = re.sub(r"\s+", " ", clean)
    if not clean:
        return True

    alnum_only = re.sub(r"[^a-zA-Z0-9\u0980-\u09ff]", "", clean)
    if not alnum_only or alnum_only.isdigit():
        return True
    if len(alnum_only) < 2:
        return True

    low_clean = clean.lower()
    if low_clean in VALID_SHORT_NAMES or alnum_only.lower() in VALID_SHORT_NAMES:
        return False

    if GARBAGE_EXACT_RE.match(low_clean):
        return True

    if re.search(r"\b(amg\d+[a-z0-9]*|amgplt\d+|tracks[-_]?v\d|chunklist|playlist|master|index|mono|hls|stream\d+|ch[-_]?\d{3,}|tv[-_]?\d{3,})\b", low_clean):
        return True
    if re.fullmatch(r"[0-9a-f]{8,}", alnum_only.lower()):
        return True

    digits = sum(c.isdigit() for c in alnum_only)
    letters = len(alnum_only) - digits
    if letters == 0 or (digits >= 4 and digits > letters):
        return True

    has_bengali = bool(re.search(r"[\u0980-\u09ff]", clean))
    if not has_bengali:
        words = [w for w in re.findall(r"[a-zA-Z]+", clean) if len(w) >= 3]
        if words and all(not re.search(r"[aeiouyAEIOUY]", w) for w in words):
            if any(len(w) >= 5 for w in words) or (len(words) == 1 and words[0].lower() not in VALID_SHORT_NAMES):
                return True

    return False


def norm(name):
    n = name.lower()
    n = re.sub(r"[\(\[].*?[\)\]]", " ", n)
    n = re.sub(r"\b\d{3,4}p\b", " ", n)
    n = n.replace("television", "tv")
    n = re.sub(r"[^a-z0-9\u0980-\u09ff]+", " ", n)
    return "".join(t for t in n.split() if t not in NOISE)


def is_blocked_or_non_tv(c):
    if c.get("radio_tag"):
        return True
    name = c["name"]
    if is_garbage_channel_name(name):
        return True

    group = c.get("group", "")
    url = c.get("url", "")
    t = f"{name} {group}".lower()

    if c.get("source") == "GitHub" and EXPIRED_TOKEN_URL_RE.search(url):
        return True

    if any(re.search(rf"\b{re.escape(w)}\b", t) for w in BLOCK_WORDS):
        return True
    if RADIO_RE.search(t) or AUDIO_EXT_RE.search(url):
        return True
    if VOD_EXT_RE.search(url) or VOD_PATH_RE.search(url):
        return True
    if VOD_NAME_RE.search(name):
        return True
    if re.search(r"\b(vod|24/7\s*movies?|box\s*office)\b", group.lower()):
        return True
    return False


BANGLA_RE = re.compile(r"\b(bangla|bengali|bengal|bangladesh|bangladeshi|jalsha|aath|ananda|kolkata|calcutta|ghanta|ebela|sangeet bangla)\b", re.I)
HINDI_RE = re.compile(
    r"\b(hindi|bollywood|star plus|star bharat|star utsav|star gold|sony sab|sab tv|sony tv|sony entertainment|"
    r"sony pal|sony max|sony pix|sony wah|sony yay|sony bbc|colors|colors tv|colors rishtey|colors cineplex|"
    r"zee tv|zee anmol|zee cinema|zee classic|zee action|zee bollywood|zee news|zee hindustan|zee zing|zee zest|zee cafe|"
    r"zee business|zee salaam|&tv|and tv|and pictures|&pictures|and priv|&priv|dangal|ishara|shemaroo|big magic|bindass|"
    r"aaj tak|abp news|abp ganga|ndtv|india tv|news18|republic bharat|republic tv|tv9|dd national|dd news|dd bharati|"
    r"dd kisan|dd retro|dd india|sanskar|aastha|sadhna|bhojpuri|b4u|mtv india|9xm|9x jalwa|9x tashan|zing|utv|"
    r"filamchi|goldmines|manoranjan|cineplex|news nation|india news|good news today|bharat 24|tez|zoom tv|mastiii|"
    r"movie plus|enter10|bflix|e24|times now|mirror now|et now|wion|india today|cnbc awaaz|cnbc tv18|zee bihar|"
    r"zee rajasthan|zee mp|zee up|news24 india|sudarshan|jansatta|shubh tv|dabangg|dhamaal|maha movie|"
    r"wow cinema|abzy|pitara|popkorn|housefull|dil se)\b", re.I
)

# শুধুমাত্র ভারতীয় আঞ্চলিক ভাষা (Urdu/Arabic বাদ দেওয়া হয়েছে)
REGIONAL_LANG_PATTERNS = [
    ("Tamil",     re.compile(r"\b(tamil|star vijay|vijay tv|sun tv|kalaignar|jaya tv|raj tv|polimer|puthiya thalaimurai|thanthi|zee tamil|colors tamil|ktv|adithya|isaiaruvi|sun music|sun news|sri sankara)\b", re.I)),
    ("Telugu",    re.compile(r"\b(telugu|star maa|maa tv|maa movies|maa music|maa gold|etv telugu|etv plus|etv cinema|gemini|zee telugu|ntv telugu|tv9 telugu|sakshi|abn|v6 news|10tv|hmtv|t news)\b", re.I)),
    ("Malayalam", re.compile(r"\b(malayalam|asianet|surya tv|surya movies|surya music|mazhavil|manorama|kairali|flowers tv|zee keralam|amrita|media one|twentyfour|mathrubhumi|reporter tv|janam tv)\b", re.I)),
    ("Kannada",   re.compile(r"\b(kannada|star suvarna|udaya|zee kannada|colors kannada|tv9 kannada|public tv|suvarna news|power tv|kasthuri|raj news kannada)\b", re.I)),
    ("Marathi",   re.compile(r"\b(marathi|star pravah|zee marathi|zee talkies|zee yuva|colors marathi|sony marathi|abp majha|lokshahi|saam tv|fakt marathi|tv9 marathi|news18 lokmat)\b", re.I)),
    ("Punjabi",   re.compile(r"\b(punjabi|ptc punjabi|ptc chak de|ptc news|zee punjabi|pitaara|mh one|chardikla|brit asia|akaal|sangat)\b", re.I)),
    ("Gujarati",  re.compile(r"\b(gujarati|colors gujarati|tv9 gujarati|abp asmita|sandesh news|vtv gujarati|gstv|news18 gujarati)\b", re.I)),
    ("Odia",      re.compile(r"\b(odia|oriya|tarang|zee sarthak|star kirano|otv|kanak news|kalinga tv|prameya|mbc tv odia|alankar)\b", re.I)),
    ("Assamese",  re.compile(r"\b(assamese|rengoni|rang tv|jonack|prag news|dy 365|pratidin time|news live assam|ramdhenu)\b", re.I)),
]

LANG_CODE_TO_GROUP = {
    "tam": "Tamil",
    "tel": "Telugu",
    "mal": "Malayalam",
    "kan": "Kannada",
    "mar": "Marathi",
    "pan": "Punjabi",
    "guj": "Gujarati",
    "ori": "Odia",
    "asm": "Assamese",
}

KOLKATA_RE = re.compile(r"\b(jalsha movies|zee bangla cinema|colors bangla cinema|bangla cinema|"
                        r"bangla movies?|bengali movies?|bengali cinema)\b", re.I)

HINDI_MOVIE_CH_RE = re.compile(
    r"\b(star gold|sony max|sony wah|zee cinema|zee classic|zee action|zee bollywood|zee anmol cinema|"
    r"colors cineplex|and pictures|&pictures|utv movies|utv action|b4u movies|b4u kadak|"
    r"filamchi|goldmines|manoranjan|enter10|bflix|movie plus|maha movie|wow cinema|abzy movies|housefull movies)\b", re.I
)
ENGLISH_MOVIE_CH_RE = re.compile(
    r"\b(star movies|hbo|sony pix|zee cafe|and priv|&priv|movies now|mnx|mn\+|romedy now|axn|wb|warner bros|"
    r"paramount|amc|cinemax|showtime|tcm|hallmark|syfy|mgm|universal movies|sony movies|film4|sky cinema|"
    r"rakuten|plex movies|cinevault|miramax|lionsgate|starz|encore|epix)\b", re.I
)
ENGLISH_ENT_CH_RE = re.compile(
    r"\b(star world|colors infinity|comedy central|fox life|fox crime|fox news|fox business|fx|fxx|tlc|"
    r"discovery|national geographic|nat geo|animal planet|history|travel xp|bbc|cnn|cnbc|bloomberg|"
    r"al jazeera|dw|france 24|france24|nhk|cgtn|trt world|euronews|arirang|kbs world|cna|channel news asia|"
    r"cbs|nbc|abc|fox|itv|channel 4|channel 5|e4|more4|voa|voice of america|"
    r"sky one|sky witness|sky atlantic|sky news|sky arts|bravo|lifetime|hgtv|food network|mtv|vh1|"
    r"nickelodeon|nick jr|nicktoons|cartoon network|boomerang|disney|pbs|a&e|tnt|tbs|usa network|cw|"
    r"msnbc|hln|trutv|investigation discovery|id channel|science channel|smithsonian|vice|dave|gold uk|"
    r"alibi|drama uk|yesterday uk|eden uk|quest uk|really uk|pick tv|challenge tv)\b", re.I
)

PAY_ENT_CH_RE = re.compile(
    r"\b(star plus|star bharat|star utsav|star jalsha|colors|rishtey|sony entertainment|sony tv|set hd|set india|"
    r"sony sab|sab tv|sony pal|sony aath|sony yay|sony bbc|zee tv|zee anmol|zee bangla|zee news|zee hindustan|"
    r"zee zing|zee zest|zee business|zee salaam|&tv|and tv|dangal|ishara|shemaroo|big magic|bindass|"
    r"epic tv|disney|hungama|nick|nickelodeon|sonic|cartoon network|pogo|marvel hq)\b", re.I
)
PAY_SPORTS_RE = re.compile(
    r"\b(star sports|sony ten|sony sports|ten sports|t sports|willow|ptv sports|eurosport|"
    r"bein sports|sky sports|bt sport|tnt sports|fox sports|espn|dazn|super sport|dd sports|sports 18|sports18|"
    r"geo super|a sports|premier sports|arena sport|sport tv|nova sport|canal\+ sport)\b", re.I
)

# শুধুমাত্র প্রকৃত বাংলাদেশি চ্যানেলগুলোর তালিকা
BD_ONLY_RE = re.compile(
    r"\b(btv|bangladesh television|sangsad|somoy|jamuna|ekattor|channel 24|dbc|independent|"
    r"atn bangla|atn news|atn islamic|atn music|news 24|news24|channel i|channel 9|channel s|"
    r"ntv|rtv|banglavision|maasranga|deepto|boishakhi|desh tv|nagorik|gtv|gazi tv|asian tv|"
    r"sa tv|satv|mohona|my tv|mytv|bijoy|ananda tv|nexus|t sports|duronto|gaan bangla|"
    r"ekhon|global tv|green tv|bangla tv|bangla vision|spice tv|muktijoddho|diganta|islamic tv|"
    r"madani channel bangla|peace tv bangla|somoy news|jamuna tv|ekattor tv|independent tv|"
    r"bangladesh|bangladeshi|\bbd\b)\b", re.I
)

# ভারতীয় বাংলা চ্যানেলসমূহ
INDIA_BANGLA_RE = re.compile(
    r"\b(star jalsha|jalsha|zee bangla|sony aath|colors bangla|sun bangla|akas aath|akash aath|"
    r"sangeet bangla|dhoom music|rupashi bangla|dd bangla|abp ananda|star ananda|news18 bangla|etv bangla|"
    r"zee 24 ghanta|24 ghanta|republic bangla|tv9 bangla|calcutta news|kolkata tv|r plus|rplus|"
    r"cn tv|calcutta television|tara muzik|tara newz|bangla time|onkar news|high news|sadhu bangla|"
    r"khabor 365|bangla jagao|nkh bangla|ctvn|akd plus|bflix bangla|khoj khobor|newstime bangla|"
    r"prime news bangla|amar bangla|bangla plus|bangla express|kolkata|calcutta|west bengal)\b", re.I
)


def classify_by_language_and_name(c):
    """Bangladeshi, Indian o English channel alada korbe; onno sob channel 'DROP' return korbe"""
    name = c["name"]
    group = c.get("group", "")
    lname = name.lower()
    lgroup = group.lower()
    t = f"{lname} {lgroup}"

    detected_lang = c.get("stream_lang") or c.get("lang") or ""
    music = bool(re.search(r"\b(music|song|songs|gaan|mtv|vevo|hits|zing|sangeet|muzik|dhoom|9xm|9x)\b", lname))
    movie_ch = bool(re.search(r"\b(movies?\s*(tv|channel|hd|plus|now|action|gold|club)?|cinema\s*(tv|hd)?|flix\s*tv)\b", lname))

    # 1. Sports always first
    if PAY_SPORTS_RE.search(t) or re.search(r"\b(sport|sports|cricket|football|soccer)\b", lname):
        return "Sports"

    # 2. Specific Bangla (India) & Kolkata Movies
    if KOLKATA_RE.search(lname):
        return "Kolkata Movies"
    if (INDIA_BANGLA_RE.search(lname) or "kolkata" in lname or "calcutta" in lname
            or (detected_lang == "ben" and re.search(r"\b(india|indian|west bengal|kolkata)\b", lgroup))):
        return "Bangla Music" if music else "Bangla (India)"

    # 3. Specific Movie & English Entertainment channels (AynaOTT/JagoBD theke eleo English e jabe)
    if HINDI_MOVIE_CH_RE.search(lname):
        return "Hindi Movies"
    if ENGLISH_MOVIE_CH_RE.search(lname):
        return "English Movies"
    if ENGLISH_ENT_CH_RE.search(lname) and detected_lang != "ben":
        return "English Music" if music else "English Entertainment"

    # 4. Indian Regional Languages
    if detected_lang in LANG_CODE_TO_GROUP:
        return LANG_CODE_TO_GROUP[detected_lang]

    for reg_group, reg_re in REGIONAL_LANG_PATTERNS:
        if reg_re.search(t):
            return reg_group

    # 5. Strictly English Language streams or English/US/UK group/country tags
    if (detected_lang == "eng"
            or re.search(r"\b(english|usa|uk|united states|united kingdom|canada|australia|hollywood)\b", lgroup)
            or re.search(r"(\(|\[|\b)(us|usa|uk|ca|au)(\)|\]|\b)", lname)) and not BD_ONLY_RE.search(lname) and not BANGLA_RE.search(lname):
        if music:
            return "English Music"
        if movie_ch:
            return "English Movies"
        return "English Entertainment"

    # 6. Strictly Bangladeshi Channels -> 'Bangladesh', other non-BD Bangla -> 'Bangla (India)'
    if BD_ONLY_RE.search(lname) or re.search(r"\bbangladesh\b", lgroup):
        if music:
            return "Bangla Music"
        return "Bangladesh"

    if detected_lang == "ben" or BANGLA_RE.search(lname):
        if HINDI_RE.search(lname) and not BANGLA_RE.search(lname) and detected_lang != "ben":
            return "Hindi Movies" if movie_ch else ("Hindi Music" if music else "India")
        if music:
            return "Bangla Music"
        return "Bangla (India)"

    # 7. Hindi / India
    if (detected_lang == "hin"
            or HINDI_RE.search(lname)
            or PAY_ENT_CH_RE.search(lname)
            or re.search(r"\b(india|indian|hindi|bollywood)\b", lgroup)
            or re.search(r"(\(|\[)(in|ind|india)(\)|\])", lname)):
        if "zing" in lname or music:
            return "Hindi Music"
        if movie_ch:
            return "Hindi Movies"
        return "India"

    # 8. English keyword fallback
    if re.search(r"\b(english|hollywood|usa|uk|british|american)\b", t):
        if music:
            return "English Music"
        if movie_ch:
            return "English Movies"
        return "English Entertainment"

    if group in ALLOWED_GROUPS:
        return group

    # International ba onno kono bhashar channel bad deya hobe
    return "DROP"


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
def write_playlist(channels, target_path=OUTPUT):
    def q(s):
        return str(s).replace('"', "'").replace("\n", " ")
    lines = ["#EXTM3U"]
    for c in channels:
        raw_url = c["url"]
        ref = ""
        ua = c.get("matched_ua") or UA

        if "|" in raw_url:
            clean_url, extra = raw_url.split("|", 1)
            clean_url = clean_url.strip()
            for part in extra.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k.strip().lower() == "referer":
                        ref = v.strip()
                    elif k.strip().lower() == "user-agent":
                        ua = v.strip()
        else:
            clean_url = raw_url.strip()

        if "jagobd.com" in clean_url and not ref:
            ref = "https://www.jagobd.com/"
        elif "aynaott.com" in clean_url and not ref:
            ref = "https://web.aynaott.com/"

        lang_attr = f' tvg-language="{q(c.get("stream_lang") or c.get("lang") or "")}"' if (c.get("stream_lang") or c.get("lang")) else ""
        lines.append(f'#EXTINF:-1 tvg-id="{q(c.get("tvg_id", ""))}" tvg-name="{q(c["name"])}" '
                     f'tvg-logo="{q(c.get("logo", ""))}"{lang_attr} group-title="{q(c["group"])}",{c["name"]}')
        if ua:
            lines.append(f"#EXTVLCOPT:http-user-agent={ua}")
        if ref:
            lines.append(f"#EXTVLCOPT:http-referrer={ref}")
        lines.append(clean_url)
    target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    start_time = time.time()
    pool = []
    scrapers = (
        ("AynaOTT", scrape_aynaott),
        ("JagoBD", scrape_jagobd),
        ("DiscoveredSources", scrape_discovered_sources),
        ("AdaptiveRepos", scrape_adaptive_repos),
        ("IPTV-org", scrape_iptv_org_base),
    )
    for name, fn in scrapers:
        try:
            pool.extend(fn())
        except Exception as e:
            log(f"[{name}] error: {e}")

    seen, items = set(), []
    for c in pool:
        raw = c["url"].split("|")[0].strip()
        if not raw or raw in seen or is_blocked_or_non_tv(c):
            continue
        if any(b in c["url"] for b in BLOCK_URLS):
            continue

        # Stream check er agei pre-filter kore International/foreign channel bad dile check aro 2x fast hobe
        pre_grp = classify_by_language_and_name(c)
        if pre_grp == "DROP" and c["source"] not in ("AynaOTT", "JagoBD"):
            continue

        seen.add(raw)
        items.append(c)
    log(f"Total unique BD/Indian/English candidate: {len(items)}")

    check_all(items)
    alive_raw = [c for c in items if c.get("alive")]

    alive = []
    for c in alive_raw:
        grp = classify_by_language_and_name(c)
        if grp not in ALLOWED_GROUPS:
            continue
        if (c["source"] == "GitHub" and grp in ("Bangladesh", "Kolkata Movies", "Bangla Music")
                and any(h in c["url"].lower() for h in NOT_BANGLA_HOSTS)):
            continue
        c["group"] = grp
        alive.append(c)

    log(f"Alive (BD + Indian + English only): {len(alive)} / {len(items)}")

    prev = OUTPUT.read_text(encoding="utf-8", errors="ignore").count("#EXTINF") if OUTPUT.exists() else 0
    if prev >= 20 and len(alive) < prev * MIN_KEEP_RATIO:
        log(f"[SAFETY] Alive ({len(alive)}) ager playlist ({prev}) er theke onek kom. Overwrite kora holo na.")
        return 1

    if MAX_LINKS_PER_CHANNEL:
        before = len(alive)
        alive = dedupe_by_name(alive, MAX_LINKS_PER_CHANNEL)
        log(f"Duplicate naam bad: {before} -> {len(alive)}")

    alive.sort(key=lambda c: (GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99,
                              c["name"].lower(),
                              SOURCE_RANK.get(c["source"], 9),
                              c.get("latency", 99)))

    cnt = Counter(c["group"] for c in alive)
    log("Language & Category wise channel: " + ", ".join(f"{g}: {n}" for g, n in cnt.most_common()))

    # 1. Save full playlist (with backup links)
    write_playlist(alive, OUTPUT)

    # 2. Save duplicate-free & lowest ping (fastest) playlist
    best_by_channel = {}
    for c in alive:
        key = norm(c["name"]) or c["name"].lower()
        if key not in best_by_channel or c.get("latency", 99) < best_by_channel[key].get("latency", 99):
            best_by_channel[key] = c

    clean_alive = sorted(
        best_by_channel.values(),
        key=lambda c: (GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99,
                       c["name"].lower())
    )
    write_playlist(clean_alive, CLEAN_OUTPUT)

    elapsed = int(time.time() - start_time)
    log(f"{OUTPUT} e {len(alive)} ta channel ebong {CLEAN_OUTPUT} e {len(clean_alive)} ta duplicate-free fastest ping channel save hoise (Total time: {elapsed // 60}m {elapsed % 60}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
