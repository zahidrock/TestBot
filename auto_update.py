"""
All-in-One Direct (No-Proxy) Global Live TV IPTV Updater (Logo-Free + Pause & Resume Edition)
Features:
- Checkpoint / Auto-Pause & Resume: saves progress every 2,500 checks to 'check_checkpoint.json'
  and cleanly pauses before workflow timeout, resuming from the exact remaining channels on next run
- Recursive sub-playlist discovery for deeper active channel harvesting
- Logo-free lightweight M3U output for faster parsing and cleaner player UI
- Unlimited high-speed backup links per channel (MAX_LINKS_PER_CHANNEL = None), sorted fastest-first
- Strict Speed & Throughput Gate: drops slow/buffering streams (manifest latency > 2.2s or chunk throughput < 25 KB/s)
- Official Channel Name Enrichment from 'iptv-org/api/channels.json' + AynaOTT Catalog
- Dedicated 'Kids & Animation' & 'Informative' groups alongside BD, Bangla/Hindi India, English & Sports
- Active Pool vs. Sleeping Pool sync ('source_health.json' & 'sleeping_sources.json')
"""
import json, os, re, socket, ssl, sys, threading, time
from collections import Counter, defaultdict
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

socket.setdefaulttimeout(5)

# ------------------------------------------------------------------ settings
OUTPUT = Path("playlist.m3u")
CUSTOM_CHANNELS_FILE = Path("custom_channels.txt")
CUSTOM_OUTPUT = Path("playlist_custom.m3u")
SOURCE_REPORT = Path("source_report.txt")
DISCOVERED_FILE = Path("discovered_sources.json")
HEALTH_FILE = Path("source_health.json")
SLEEPING_FILE = Path("sleeping_sources.json")
CHECKPOINT_FILE = Path("check_checkpoint.json")

WORKERS = 220
CHECK_TIMEOUT = 3.0
MAX_MANIFEST_LATENCY = 2.2
MIN_CHUNK_KBPS = 25.0
MAX_LINKS_PER_CHANNEL = None
MIN_KEEP_RATIO = 0.1

# Auto-pause cleanly before GitHub Actions workflow timeout (default 140 mins = 8400s)
MAX_RUNTIME_SECONDS = int(os.getenv("MAX_RUNTIME_SECONDS", "8400"))
CHECKPOINT_MAX_AGE_SECONDS = 86400  # 24 hours

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
IPTV_BASE = "https://iptv-org.github.io/iptv"

COUNTRY_SOURCES = [
    ("Bangladesh",            f"{IPTV_BASE}/countries/bd.m3u"),
    ("India",                 f"{IPTV_BASE}/countries/in.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/us.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/uk.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/ca.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/au.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/nz.m3u"),
    ("English Entertainment", f"{IPTV_BASE}/countries/sg.m3u"),
]

DIRECT_GLOBAL_FEEDS = [
    ("FAST-TV", "https://i.mjh.nz/SamsungTVPlus/all.m3u8"),
    ("FAST-TV", "https://i.mjh.nz/PlutoTV/all.m3u8"),
    ("FAST-TV", "https://i.mjh.nz/Plex/all.m3u8"),
    ("FAST-TV", "https://i.mjh.nz/Roku/all.m3u8"),
    ("FAST-TV", "https://i.mjh.nz/Stirr/all.m3u8"),
    ("FAST-TV", "https://www.apsattv.com/lg.m3u"),
    ("FAST-TV", "https://www.apsattv.com/vizio.m3u"),
    ("FAST-TV", "https://www.apsattv.com/xumo.m3u"),
    ("FAST-TV", "https://www.apsattv.com/distrotv.m3u"),
    ("FAST-TV", "https://www.apsattv.com/localnow.m3u"),
    ("FAST-TV", "https://www.apsattv.com/tubi.m3u"),
    ("FAST-TV", "https://www.apsattv.com/redbox.m3u"),
    ("OpenDir", "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8"),
]

COMBOS = [
    ("Kolkata Movies",        "movies",        "ben"),
    ("Hindi Movies",          "movies",        "hin"),
    ("English Movies",        "movies",        "eng"),
    ("English Entertainment", "entertainment", "eng"),
    ("English Entertainment", "series",        "eng"),
    ("Informative",           "documentary",   None),
    ("Informative",           "science",       None),
    ("Informative",           "education",     None),
    ("Kids & Animation",      "kids",          None),
    ("Kids & Animation",      "animation",     None),
    ("Hindi Music",           "music",         "hin"),
    ("English Music",         "music",         "eng"),
    ("Bangla Music",          "music",         "ben"),
    ("Sports",                "sports",        None),
]

ALLOWED_GROUPS = {
    "Bangladesh", "Bangla (India)", "Kolkata Movies", "Bangla Music",
    "Hindi Movies", "Hindi Music", "India",
    "Informative", "Kids & Animation",
    "English Movies", "English Entertainment", "English Music",
    "Sports"
}

GROUP_ORDER = [
    "Bangladesh", "Bangla (India)", "Kolkata Movies", "Bangla Music",
    "Hindi Movies", "Hindi Music", "India",
    "Informative", "Kids & Animation",
    "English Movies", "English Entertainment", "English Music",
    "Sports"
]

BLOCK_URLS = [
    "amg13643-amg13643c1-amgplt0016",
    "amgplt",
    "bitcdn-kronehit.bitmovin.com",
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
    "ben": "ben", "bn": "ben", "bengali": "ben", "bangla": "ben", "ban": "ben",
    "hin": "hin", "hi": "hin", "hindi": "hin",
    "eng": "eng", "en": "eng", "english": "eng",
    "tam": "tam", "ta": "tam", "tamil": "tam",
    "tel": "tel", "te": "tel", "telugu": "tel",
    "mal": "mal", "ml": "mal", "malayalam": "mal",
    "kan": "kan", "kn": "kan", "kannada": "kan",
    "mar": "mar", "mr": "mar", "marathi": "mar",
    "pan": "pan", "pa": "pan", "punjabi": "pan", "pun": "pan",
    "guj": "guj", "gu": "guj", "gujarati": "guj",
    "ori": "ori", "or": "ori", "odia": "ori", "oriya": "ori",
    "asm": "asm", "as": "asm", "assamese": "asm",
    "kok": "kok", "konkani": "kok",
    "urd": "urd", "ur": "urd", "urdu": "urd",
    "ara": "ara", "ar": "ara", "arabic": "ara",
    "bho": "bho", "bhojpuri": "bho",
    "nep": "nep", "ne": "nep", "nepali": "nep",
    "spa": "spa", "es": "spa", "spanish": "spa",
    "ces": "ces", "cs": "ces", "czech": "ces",
    "chi": "chi", "zh": "chi", "chinese": "chi",
    "heb": "heb", "he": "heb", "hebrew": "heb",
    "por": "por", "pt": "por", "portuguese": "por",
    "rus": "rus", "ru": "rus", "russian": "rus",
    "fra": "fra", "fr": "fra", "french": "fra",
    "deu": "deu", "de": "deu", "german": "deu",
    "tha": "tha", "th": "tha", "thai": "tha",
    "pol": "pol", "pl": "pol", "polish": "pol",
}

DROP_LANG_CODES = {
    "tam", "tel", "mal", "kan", "mar", "pan", "guj", "ori", "asm", "bho", "kok",
    "urd", "ara", "nep", "spa", "ces", "chi", "heb", "por", "rus", "fra", "deu",
    "tha", "pol"
}

DROP_SUBSTRING_KEYWORDS = (
    "tamil", "telugu", "telegu", "malayalam", "malyalam", "kannada", "kanda", "marathi",
    "punjabi", "punjab", "gujarati", "gujrati", "odia", "oriya", "assamese", "assam",
    "bhojpuri", "bhojopuri", "bhejpur", "haryanvi", "rajasthani", "konkani", "gurbani", "gurbaani",
    "asianet", "kalaignar", "puthiya", "thanthi", "polimer", "jaya", "rajtv", "rajmusix",
    "rajnews", "suriya", "sunmusic", "sunnews", "ktv", "adithya", "isaiaruvi", "murasu",
    "makkal", "vaanavil", "peppers", "vendhar", "thalaa", "puyugam", "puthuyugam",
    "sathiyam", "nambikkai", "aaseervatham", "arputhar", "madha", "meialai", "thendral",
    "vasanth", "win tv", "captain tv", "imayam", "megatv", "moon tv", "moontv",
    "madhimugam", "mathimugam", "velicham", "valicham", "subintv", "marutam", "vanitha",
    "starmaa", "maatv", "etvtelugu", "etvandhra", "etvtelangana", "etvplus", "etvcinema",
    "etvlife", "etvabhiruchi", "etvbharat", "gemini", "sakshi", "abnandhra", "v6news",
    "10tv", "hmtv", "tnews", "ntvtelugu", "tv5news", "cvrnews", "mahaa", "prime9",
    "studiov", "visssa", "vissatv", "bhagyam", "chhayachitram", "pmc", "mojtv", "mojotv",
    "nijamtv", "subhavaartha", "shubhavartha",
    "mazhavil", "manorama", "mathrubhumi", "kairali", "flowers", "amrita", "twentyfour",
    "reportertv", "janamtv", "kaumudy", "jeevan", "safari", "harvest", "powervision",
    "pulari", "realnews", "swantham", "victers", "kappa", "jaihind", "shalom", "goodness",
    "mediaone", "keralavision", "shekinah",
    "udaya", "suvarna", "kasthuri", "publictv", "siri", "ashwaveega", "namma", "ayush",
    "sadvidya", "sarala", "dheeran", "mnadu",
    "pravah", "lokmat", "abpmajha", "saam", "fakt", "pudhari", "jaimaharashtra",
    "zee24taas", "topnews", "sahyadri", "awaazindia",
    "ptc", "chardikla", "pitaara", "mh1", "mhone", "akaal", "sangat", "5aab", "baallee",
    "balle", "dukhnivaran", "nanaksar", "namdhari", "rozana", "dailypost", "apnapunjab",
    "sada tv", "sadatv", "sanja", "sanjha", "ragni", "saang", "gtc", "livingindia",
    "fatehtv", "primeasia",
    "sandesh", "abpasmita", "vtv", "gstv", "mantavya", "newscapital", "tv13", "zee24kalak",
    "tarang", "sarthak", "kirano", "otv", "kanak", "kalinga", "prameya", "alankar",
    "nandighosha", "argus", "prarthana", "mbctv", "baiscope", "onepaschima",
    "rengoni", "jonack", "prag", "dy365", "pratidin", "ramdhenu", "hornbill",
    "quran", "sunnah", "makkah", "makka", "madina", "medina", "arabic",
    "humtv", "hummasala", "humsitaray", "arydigital", "arynews", "geotv", "geonews",
    "discoverpakistan", "discoverpikstan", "dunyanews", "8xm", "mbcbollywood", "urdu",
    "antena", "prima", "ocko", "paprika", "telestar", "realitatea", "spektrum",
    "stara", "starkefrauen", "handystar", "abai", "republica", "utv.iq", "utvrussian",
    "film+", "passionbollywood", "bollywoodclassic", "nusantara",
    "espanol", "español", "espanhol", "curiosidade", "ultratumba", "caraibes", "caraïbes",
    "schwammkopf", "magisch", "classico", "clássico",
    "afarin", "amou yazid", "pronto socorro", "quebrando o tabu", "tv investigação",
    "naturaleza salvaje", "méxico travel", "thai pbs", "poland", "iran independent",
    "omroep zeeland"
)

DEAD_HOSTS = set()
DEAD_HOST_LOCK = threading.Lock()
HOST_FAIL_COUNTS = Counter()

# ------------------------------------------------------------------ http
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Safari/537.36")
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


# ------------------------------------------------------------------ Official Channel Name Fetcher
def fetch_official_channel_names():
    id_to_name = {}
    try:
        raw = http_get("https://iptv-org.github.io/api/channels.json", timeout=15, max_bytes=15_000_000)
        channels = json.loads(raw)
        for ch in channels:
            cid = (ch.get("id") or "").strip()
            cname = (ch.get("name") or "").strip()
            if not cid or not cname:
                continue
            id_to_name[cid.lower()] = cname
            id_to_name[cid.split("@")[0].lower()] = cname
        log(f"[OfficialDB] iptv-org channels.json theke {len(id_to_name)} ti official channel name load hoyeche.")
    except Exception as e:
        log(f"[OfficialDB] channels.json fetch info: {e}")
    return id_to_name


def looks_like_random_id(token_str):
    s = re.sub(r"\s+", "", token_str or "")
    if len(s) < 9:
        return False
    has_digit = any(c.isdigit() for c in s)
    has_upper = any(c.isupper() for c in s)
    has_lower = any(c.islower() for c in s)
    if has_digit and has_upper and has_lower and len(s) >= 9:
        return True
    orig_no_space = (token_str or "").strip()
    if " " not in orig_no_space and len(s) >= 10:
        vowels = sum(1 for c in s.lower() if c in "aeiou")
        if vowels <= 1:
            return True
    return False


def clean_display_name(raw_name):
    if not raw_name:
        return "Unknown"
    s = raw_name.strip()
    if "#extvlcopt" in s.lower() or "http-user-agent" in s.lower() or "http-referrer" in s.lower():
        return "Unknown"
    s = re.sub(r"^\d+\s*[.\-_|:]\s*", "", s)
    s = re.sub(r"^\d{1,3}\s+(?=[A-Za-z])", "", s)
    s = re.sub(r"^(?:[A-Z]{2,5}\s*\|\s*|【[^】]+】\s*|[A-Z]{2,3}★\s*)", "", s)
    s = re.sub(r"[_#]\d+$", "", s)
    s = re.sub(r"\s*[\(\[](?:\d{3,4}[pi]|geo-?blocked|not\s*24/7|india|bangladesh|jago\s*bd|samsungtvplus|adaptive|multi-?lang|backup\s*\d*|opc\.\d+|nacional|[^a-zA-Z0-9]+)[\)\]]", "", s, flags=re.I)
    s = re.sub(r"[\U0001F1E6-\U0001F1FFⓈⒼ✅]+", "", s)
    s = re.sub(r"\s+", " ", s).strip(" -_|:")
    if looks_like_random_id(s):
        return "Unknown"
    if s.isupper() and len(s) > 4:
        s = s.title()
        for acr in ("Tv", "Hd", "Bbc", "Cnn", "Atn", "Rtv", "Ntv", "Btv", "Gtv", "Dbc", "Satv", "Hbo", "Axn", "Tlc", "Mtv", "Ndtv", "Abp", "Zee", "Pbs", "Nasa", "Espn", "Nfl", "Ufc", "Wwe"):
            s = re.sub(rf"\b{acr}\b", acr.upper(), s)
    return s or "Unknown"


# ------------------------------------------------------------------ URL converter & m3u parse
def clean_stream_url(url):
    if not url:
        return ""
    url = url.replace("\\u0026", "&").replace("\\/", "/")
    url = url.split("#")[0].strip().rstrip("\\").strip()
    if "output=ts" in url.lower():
        url = re.sub(r"output=ts", "output=m3u8", url, flags=re.I)
    if "output=mpegts" in url.lower():
        url = re.sub(r"output=mpegts", "output=m3u8", url, flags=re.I)
    base_part = url.split("?")[0]
    m = XTREAM_NUM_RE.match(base_part)
    if m:
        qs = ("?" + url.split("?", 1)[1]) if "?" in url else ""
        return f"{m.group(1)}.m3u8{qs}"
    return url


def clean_tvg_id(raw_id):
    tid = (raw_id or "").strip()
    if not tid or tid.isdigit() or len(tid) <= 2:
        return ""
    return tid


def normalize_lang_code(raw_lang):
    if not raw_lang:
        return ""
    for token in re.split(r"[,;/\s-]+", raw_lang.strip().lower()):
        if token in ISO_LANG_MAP:
            return ISO_LANG_MAP[token]
    return raw_lang.strip().lower()[:3]


def detect_source_label(url, default_src="Web/Git"):
    low = url.lower()
    if "mjh.nz" in low or "apsattv.com" in low:
        return "FAST-TV"
    if "gitlab.com" in low:
        return "GitLab"
    if "codeberg.org" in low:
        return "Codeberg"
    if "bitbucket.org" in low:
        return "Bitbucket"
    if "huggingface.co" in low:
        return "HuggingFace"
    if any(p in low for p in ("pastebin.com", "rentry.co", "telegra.ph", "dpaste.org", "paste.ee")):
        return "WebPaste"
    if "gist.githubusercontent.com" in low:
        return "GitHub-Gist"
    if "github" in low:
        return "GitHub"
    return default_src


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
            raw_title = line.rsplit(",", 1)[-1].strip() if "," in line else ""
            if raw_title.startswith("#EXT"):
                raw_title = ""
            name = raw_title or attr("tvg-name") or "Unknown"
            raw_lang = attr("tvg-language") or attr("language")
            meta = {
                "name": name,
                "tvg_id": clean_tvg_id(attr("tvg-id")),
                "group": attr("group-title") or "Others",
                "lang": normalize_lang_code(raw_lang),
                "source": source,
                "radio_tag": bool(re.search(r'radio="true"', line, re.I))
            }
            vlc_ua, vlc_ref = "", ""
        elif line.startswith("#EXTVLCOPT:"):
            opt = line.split(":", 1)[1].strip()
            if opt.lower().startswith("http-user-agent="):
                val = opt.split("=", 1)[1].strip()
                val = re.sub(r"^#EXTVLCOPT:http-user-agent=", "", val, flags=re.I)
                vlc_ua = val
            elif opt.lower().startswith("http-referrer="):
                val = opt.split("=", 1)[1].strip()
                val = re.sub(r"^#EXTVLCOPT:http-referrer=", "", val, flags=re.I)
                vlc_ref = val
        elif meta and not line.startswith("#"):
            if re.match(r"^https?://", line):
                if "|" in line:
                    u_part, h_part = line.split("|", 1)
                    line = f"{clean_stream_url(u_part)}|{h_part.split('#')[0].strip()}"
                else:
                    line = clean_stream_url(line)
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
                                       headers=h, timeout=10))
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
        try:
            d = json.loads(http_get(f"{base}/api/v5/browse/live-tvs/{slug}", headers=h, timeout=6))
            rec = d.get("record", d)
            so = rec.get("stream", {})
            url = so.get("url", "") if isinstance(so, dict) else ""
            url = url or rec.get("stream_url") or rec.get("hls") or ""
            if url:
                return {
                    "name": title,
                    "url": clean_stream_url(url),
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
    raw = raw.split("#")[0].strip()
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
        html = http_get("https://www.jagobd.com/", timeout=10)
        skip = ['/category', '/tag', '/contact', '/about', '/privacy', '/page', '/wp-', '/feed',
                '/disclaimer', 'dmca', 'terms', 'advertise', 'copyright']
        seen = set()
        for url, content in re.findall(
                r'<a\s+[^>]*href=[\'"](https://www\.jagobd\.com/[^\' "#?]+)[\'"][^>]*>(.*?)</a>', html, re.S):
            url = url.rstrip("/")
            if url in seen or any(x in url.lower() for x in skip):
                continue
            seen.add(url)
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
            })
    except Exception as e:
        log(f"[JagoBD] homepage error: {e}")
    log(f"[JagoBD] channel link: {len(cands)}")

    def resolve(c):
        try:
            page = http_get(c["url"], timeout=6)
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
                eh = http_get(embed, headers={"User-Agent": UA, "Referer": c["url"]}, timeout=6)
                for _fn, arr in re.findall(
                        r'function\s+(\w+)\(\)\s*\{\s*return\(\[([^\]]+)\]\.join\([\'"] [\'"]\)', eh):
                    val = "".join(x.strip().strip("\'\"") for x in arr.split(",")).replace(r"\/", "/")
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


def scrape_discovered_and_global_feeds(checked_feed_urls):
    upstream_urls = [u for _, u in DIRECT_GLOBAL_FEEDS]
    bot_playlists = []

    if DISCOVERED_FILE.exists():
        try:
            data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
            upstream_urls.extend(data.get("all_upstream_sources", []))
            bot_playlists.extend(data.get("all_bot_playlists", []))
        except Exception as e:
            log(f"[DiscoveredSources] JSON read error: {e}")

    try:
        health_map = json.loads(HEALTH_FILE.read_text(encoding="utf-8")) if HEALTH_FILE.exists() else {}
    except Exception:
        health_map = {}

    always_keep = {u for _, u in DIRECT_GLOBAL_FEEDS}
    upstream_urls = [
        u for u in dict.fromkeys(upstream_urls)
        if u in always_keep or health_map.get(u, {}).get("last_hits", 0) > 0 or health_map.get(u, {}).get("fails", 0) < 2
    ]
    bot_playlists = [
        p for p in dict.fromkeys(bot_playlists)
        if p in always_keep or health_map.get(p, {}).get("last_hits", 0) > 0 or health_map.get(p, {}).get("fails", 0) < 2
    ]

    log(f"[InternetSources] {len(upstream_urls)} ti active upstream/FAST/paste source o {len(bot_playlists)} ti active repo feed check kora hocche...")

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
                if looks_like_random_id(p):
                    continue
                clean = re.sub(r"\.m3u8?$", "", p, flags=re.I)
                clean = re.sub(r"([a-z])([A-Z])", r"\1 \2", clean)
                clean = re.sub(r"[-_]+", " ", clean).strip()
                if len(clean) >= 3 and not clean.isdigit() and not looks_like_random_id(clean):
                    return clean.title()
        except Exception:
            pass
        return None

    def fetch_any_feed(u, depth=0):
        out = []
        src_label = detect_source_label(u)
        host = urllib.parse.urlparse(u).netloc.lower()
        if host in DEAD_HOSTS:
            return []
        try:
            txt = http_get(u, timeout=4, max_bytes=2_000_000)
            if "#EXTINF" in txt:
                chs = parse_m3u(txt, src_label)
                if len(chs) <= 4500:
                    for ch in chs:
                        ch["feed_url"] = u
                    return chs
                return []
            links = re.findall(r'(https?://[^\s\' "<>]+?\.m3u8?(?:\?[^\s\' "<>]*)?)', txt, re.I)
            for lk in list(dict.fromkeys(links))[:40]:
                clean_lk = clean_stream_url(lk)
                if depth == 0 and (clean_lk.lower().endswith(".m3u") or "/raw/" in clean_lk.lower()):
                    sub_chs = fetch_any_feed(clean_lk, depth=1)
                    if sub_chs:
                        out.extend(sub_chs)
                        continue
                ch_name = extract_name_from_url(clean_lk)
                if ch_name:
                    out.append({
                        "name": ch_name,
                        "url": clean_lk,
                        "tvg_id": "",
                        "group": "Others",
                        "lang": "",
                        "source": src_label,
                        "feed_url": u
                    })
        except urllib.error.URLError as e:
            msg = str(e.reason).lower()
            if "name or service not known" in msg or "errno -2" in msg or "errno 111" in msg:
                with DEAD_HOST_LOCK:
                    HOST_FAIL_COUNTS[host] += 1
                    if HOST_FAIL_COUNTS[host] >= 2:
                        DEAD_HOSTS.add(host)
        except Exception:
            pass
        return out

    all_urls = list(dict.fromkeys(upstream_urls + bot_playlists))
    checked_feed_urls.extend(all_urls)
    total_sources = len(all_urls)
    done_sources = 0
    with ThreadPoolExecutor(max_workers=160) as ex:
        futs = [ex.submit(fetch_any_feed, u) for u in all_urls]
        for f in as_completed(futs):
            res.extend(f.result())
            done_sources += 1
            if done_sources % 1000 == 0 or done_sources == total_sources:
                log(f"[InternetSources] progress: {done_sources}/{total_sources} source checked | {len(res)} channel paoa gese")

    log(f"[InternetSources] mot {len(res)} ti candidate channel paoa gese")
    return res


def scrape_adaptive_repos():
    res = []
    adaptive_repos = [
        ("amazeyourself", "adaptive-streams", "main"),
    ]
    h = {"User-Agent": UA, "Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    for owner, repo, branch in adaptive_repos:
        try:
            tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
            data = json.loads(http_get(tree_url, headers=h, timeout=10))
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
                cache[url] = parse_m3u(http_get(url, timeout=12, max_bytes=3_000_000), "IPTV-org")
            except Exception as e:
                log(f"[IPTV-org] fail {url}: {e}")
                cache[url] = []
        return cache[url]

    url_lang_map = {}
    for lg in ("ben", "hin", "eng", "tam", "tel", "mal", "kan", "mar", "pan", "guj", "ori", "asm", "tha", "pol"):
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
            default_lg = "ben" if group == "Bangladesh" else ("eng" if group == "English Entertainment" else c.get("lang", ""))
            detected_lg = url_lang_map.get(clean_u, default_lg)
            res.append({**c, "group": "Sports" if "sport" in c.get("group", "").lower() else group, "lang": detected_lg})

    try:
        streams_api = json.loads(http_get("https://iptv-org.github.io/api/streams.json", timeout=15, max_bytes=15_000_000))
        for st in streams_api:
            ch_id = st.get("channel") or ""
            st_url = st.get("url") or ""
            if st_url and any(ch_id.lower().endswith(ext) for ext in (".bd", ".in", ".us", ".uk", ".ca", ".au", ".nz", ".sg", ".ie")):
                res.append({
                    "name": ch_id.split(".")[0],
                    "url": clean_stream_url(st_url),
                    "tvg_id": clean_tvg_id(ch_id),
                    "group": "Bangladesh" if ch_id.lower().endswith(".bd") else ("India" if ch_id.lower().endswith(".in") else "English Entertainment"),
                    "lang": "ben" if ch_id.lower().endswith(".bd") else ("eng" if not ch_id.lower().endswith(".in") else ""),
                    "source": "IPTV-org"
                })
    except Exception as e:
        log(f"[IPTV-org JSON API] error: {e}")

    log(f"[IPTV-org] mot {len(res)} official channel load hoyeche")
    return res


# ------------------------------------------------------------------ stream check & speed/throughput verification
def detect_m3u8_audio_lang(m3u8_text):
    langs = M3U8_AUDIO_LANG_RE.findall(m3u8_text)
    if not langs:
        return ""
    normed = [normalize_lang_code(lg) for lg in langs if lg.strip()]
    for pref in ("ben", "hin", "eng"):
        if pref in normed:
            return pref
    return normed[0] if normed else ""


def check_hls_key(m3u8_text, base_url, headers, timeout=2.5):
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


def verify_deep_hls_and_speed(m3u8_text, base_url, headers, timeout=3.0):
    if not check_hls_key(m3u8_text, base_url, headers, timeout=2.0):
        return False, 0.0

    lines = [ln.strip() for ln in m3u8_text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return False, 0.0

    for candidate in lines[:2]:
        target_url = urllib.parse.urljoin(base_url, candidate)
        if VOD_EXT_RE.search(target_url):
            continue
        try:
            t_seg = time.time()
            req = urllib.request.Request(target_url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                if r.status not in (200, 206):
                    continue
                chunk = r.read(16384)
                dt = max(time.time() - t_seg, 0.01)
                low = chunk.decode("utf-8", errors="ignore").lower()
                if "#ext-x-endlist" in low:
                    return False, 0.0
                if any(drm in low for drm in ("sample-aes", "widevine", "playready", "skd://")):
                    continue
                if "#extm3u" in low:
                    sub_text = chunk.decode("utf-8", errors="ignore")
                    if not check_hls_key(sub_text, target_url, headers, timeout=2.0):
                        continue
                    sub_lines = [ln.strip() for ln in sub_text.splitlines()
                                 if ln.strip() and not ln.strip().startswith("#")]
                    if not sub_lines:
                        continue
                    seg_url = urllib.parse.urljoin(target_url, sub_lines[0])
                    t_seg2 = time.time()
                    req2 = urllib.request.Request(seg_url, headers=headers)
                    with urllib.request.urlopen(req2, timeout=timeout, context=SSL_CTX) as r2:
                        seg_data = r2.read(32768)
                        dt2 = max(time.time() - t_seg2, 0.01)
                        kbps = (len(seg_data) / 1024.0) / dt2
                        if r2.status in (200, 206) and len(seg_data) >= 6000 and not seg_data[:20].lstrip().startswith(b"<"):
                            if kbps >= MIN_CHUNK_KBPS:
                                return True, kbps
                elif len(chunk) >= 6000 and not chunk[:20].lstrip().startswith(b"<"):
                    kbps = (len(chunk) / 1024.0) / dt
                    if kbps >= MIN_CHUNK_KBPS:
                        return True, kbps
        except Exception:
            continue
    return False, 0.0


def check_stream(raw_url, timeout=CHECK_TIMEOUT):
    if not raw_url:
        return False, 0, "empty url", UA, "", 0.0
    base, url, custom_ua = dict(HEADERS), raw_url, False
    if "|" in raw_url:
        url, extra = raw_url.split("|", 1)
        url = clean_stream_url(url.strip())
        for part in extra.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                base[k.strip()] = v.strip()
                if k.strip().lower() == "user-agent":
                    custom_ua = True
    else:
        url = clean_stream_url(raw_url)

    host = urllib.parse.urlparse(url).netloc.lower()
    if host in DEAD_HOSTS:
        return False, 0, "dead host (cached)", UA, "", 0.0

    exp_m = re.search(r"(?:exp|expires)=(\d{10})", url, re.I)
    if exp_m and int(exp_m.group(1)) < int(time.time()) + 7200:
        return False, 0, "token expired or expiring soon", UA, "", 0.0

    if AUDIO_EXT_RE.search(url):
        return False, 0, "radio/audio file", UA, "", 0.0
    if VOD_EXT_RE.search(url) or VOD_PATH_RE.search(url):
        return False, 0, "VOD/movie file", UA, "", 0.0

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
                    return False, 0, "VOD redirect", ua, "", 0.0

                ctype = r.headers.get("Content-Type", "").lower()
                icy_name = r.headers.get("icy-name") or r.headers.get("icy-genre")
                if icy_name or ctype.startswith("audio/"):
                    return False, 0, "radio/audio stream", ua, "", 0.0
                if any(v in ctype for v in ("video/mp4", "video/x-matroska", "video/avi", "video/webm")):
                    return False, 0, "direct movie file", ua, "", 0.0

                raw = r.read(6144)
                text_body = raw.decode("utf-8", errors="ignore")
                low = text_body.strip().lower()
                lat = time.time() - t0

                if lat > MAX_MANIFEST_LATENCY:
                    return False, lat, "slow manifest (>2.2s)", ua, "", 0.0

                if r.status not in (200, 206):
                    reason = f"HTTP {r.status}"
                    continue
                if ("<html" in low or "<body" in low) and "#extm3u" not in low:
                    reason = "html page (block/login)"
                    continue

                if "#ext-x-endlist" in low:
                    return False, 0, "VOD m3u8 (ENDLIST)", ua, "", 0.0

                if any(drm in low for drm in ("sample-aes", "widevine", "playready", "skd://",
                                              "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed")):
                    return False, 0, "DRM encrypted", ua, "", 0.0

                if "#extm3u" in low and AUDIO_EXT_RE.search(low) and ".ts" not in low and "#ext-x-stream-inf" not in low:
                    return False, 0, "audio-only m3u8", ua, "", 0.0

                if "#extm3u" in low:
                    if any(tag in low for tag in ("#ext-x-stream-inf", "#extinf", "#ext-x-targetduration")):
                        ok_seg, kbps = verify_deep_hls_and_speed(text_body, final_url, headers, timeout=3.0)
                        if ok_seg:
                            stream_lang = detect_m3u8_audio_lang(text_body)
                            return True, lat, "ok", ua, stream_lang, kbps
                        return False, 0, "slow/dead HLS segment", ua, "", 0.0
                    reason = "empty/invalid m3u8"
                    continue

                if raw and raw[0] == 0x47:
                    return False, 0, "raw ts stream (not m3u8)", ua, "", 0.0
                if ".ts" in final_url.lower():
                    return False, 0, "raw ts stream (not m3u8)", ua, "", 0.0
                reason = "not an m3u8 live stream"
        except urllib.error.HTTPError as e:
            reason = f"HTTP {e.code}"
            if e.code in (404, 410, 451):
                return False, 0, reason, ua, "", 0.0
        except urllib.error.URLError as e:
            msg = str(e.reason)
            reason = "timeout" if "timed out" in msg.lower() else f"connect: {msg[:35]}"
            if "name or service not known" in msg.lower() or "errno -2" in msg.lower() or "errno 111" in msg.lower():
                with DEAD_HOST_LOCK:
                    HOST_FAIL_COUNTS[host] += 1
                    if HOST_FAIL_COUNTS[host] >= 2:
                        DEAD_HOSTS.add(host)
                return False, 0, reason, ua, "", 0.0
            if idx == 0 and reason != "timeout":
                break
        except TimeoutError:
            reason = "timeout"
        except Exception as e:
            reason = f"error: {type(e).__name__}"
            break
    return False, 0, reason, UA, "", 0.0


def save_checkpoint(channels, checked_feed_urls, started_at):
    payload = {
        "created_at": started_at,
        "updated_at": time.time(),
        "checked_feed_urls": checked_feed_urls,
        "channels": channels,
    }
    tmp_path = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload), encoding="utf-8")
    tmp_path.replace(CHECKPOINT_FILE)


def load_checkpoint():
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        age = time.time() - float(data.get("updated_at", 0))
        if age > CHECKPOINT_MAX_AGE_SECONDS:
            log(f"[Checkpoint] Purono checkpoint ({int(age // 3600)}h age) expire hoyeche, notun kore shuru hocche.")
            CHECKPOINT_FILE.unlink(missing_ok=True)
            return None
        chs = data.get("channels", [])
        if not chs:
            return None
        return data
    except Exception as e:
        log(f"[Checkpoint] Read error: {e}")
        return None


def check_all_with_resume(channels, checked_feed_urls, run_start_time, original_created_at):
    """Checks remaining unchecked channels in chunks, saves checkpoints, and cleanly pauses before timeout."""
    total = len(channels)
    already_done = sum(1 for c in channels if c.get("checked"))
    remaining = [c for c in channels if not c.get("checked")]

    if already_done > 0:
        alive_so_far = sum(1 for c in channels if c.get("alive"))
        log(f"[Resume] Checkpoint theke resume hocche: {already_done}/{total} agei checked ({alive_so_far} alive), baki {len(remaining)} ti check hobe...")

    BATCH_SIZE = 2500
    done = already_done
    paused = False

    for start_idx in range(0, len(remaining), BATCH_SIZE):
        if time.time() - run_start_time >= MAX_RUNTIME_SECONDS:
            paused = True
            break

        batch = remaining[start_idx:start_idx + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            fut = {ex.submit(check_stream, c["url"]): c for c in batch}
            for f in as_completed(fut):
                c = fut[f]
                try:
                    ok, lat, reason, matched_ua, stream_lang, kbps = f.result()
                except Exception as e:
                    ok, lat, reason, matched_ua, stream_lang, kbps = False, 0, f"error: {type(e).__name__}", UA, "", 0.0
                c["checked"] = True
                c["alive"], c["latency"], c["reason"], c["matched_ua"], c["kbps"] = ok, lat, reason, matched_ua, kbps
                if stream_lang:
                    c["stream_lang"] = stream_lang
                done += 1

        alive_now = sum(1 for x in channels if x.get("alive"))
        log(f"checked {done}/{total} | high-speed alive {alive_now}")
        save_checkpoint(channels, checked_feed_urls, original_created_at)

        if done < total and (time.time() - run_start_time >= MAX_RUNTIME_SECONDS):
            paused = True
            break

    dead = Counter(c["reason"] for c in channels if c.get("checked") and not c.get("alive") and c.get("reason"))
    log("Slow/Dead/Invalid er karon: " + ", ".join(f"{k}: {v}" for k, v in dead.most_common(8)))
    return paused


# ------------------------------------------------------------------ canonical name normalization & deduplication
NOISE = {
    "hd", "fhd", "sd", "uhd", "4k", "live", "official", "hevc", "h265", "h264",
    "bd", "in", "ind", "india", "bangladesh", "bangladeshi", "uk", "us", "usa", "ca", "canada",
    "ch", "channel", "tv", "television", "news", "network", "digital", "plus"
}

SOURCE_RANK = {
    "AynaOTT": 0, "JagoBD": 1, "FAST-TV": 2, "IPTV-org": 3,
    "GitHub": 4, "GitLab": 5, "Codeberg": 6, "Bitbucket": 7, "HuggingFace": 8,
    "GitHub-Gist": 9, "WebPaste": 10, "OpenDir": 11
}

CANONICAL_ALIASES = {
    "aakaashaath": "akashaath",
    "akashahtt": "akashaath",
    "akashaath": "akashaath",
    "gazitv": "gtv",
    "gtv": "gtv",
    "satv": "satv",
    "btvctg": "btvchattogram",
    "btvchattrogram": "btvchattogram",
    "btvchattogram": "btvchattogram",
    "btvshangshad": "sangsadbangladesh",
    "sangsadbangladesh": "sangsadbangladesh",
    "ekushe": "ekusheytv",
    "ekusheetv": "ekusheytv",
    "ekusheytv": "ekusheytv",
    "ekusheyetv": "ekusheytv",
    "ekusheytv1": "ekusheytv",
    "somoy": "somoytv",
    "somoytv": "somoytv",
    "jamuna": "jamunatv",
    "jamunatv": "jamunatv",
    "ekattor": "ekattortv",
    "ekattortv": "ekattortv",
    "maasranga": "maasrangatv",
    "maasrangatv": "maasrangatv",
    "deepto": "deeptotv",
    "deeptotv": "deeptotv",
    "boishakhi": "boishakhitv",
    "boishakhitv": "boishakhitv",
    "desh": "deshtv",
    "deshtv": "deshtv",
    "bijoy": "bijoytv",
    "bijoytv": "bijoytv",
    "mohona": "mohonatv",
    "mohonatv": "mohonatv",
    "nagorik": "nagoriktv",
    "nagoriktv": "nagoriktv",
    "ananda": "anandatv",
    "anandatv": "anandatv",
    "ekhon": "ekhontv",
    "ekhontv": "ekhontv",
    "global": "globaltv",
    "globaltv": "globaltv",
    "banglavision": "banglavision",
    "ntveurope": "ntveurope",
    "atnbanglauk": "atnbanglauk",
    "channelsuk": "channelsuk",
    "biswabangla24": "biswabangla24",
    "বিশ্ববাংলা২৪": "biswabangla24",
    "abpananda": "abpananda",
    "zeebangla": "zeebangla",
    "starjalsha": "starjalsha",
    "colorsbangla": "colorsbangla",
    "sunbangla": "sunbangla",
    "sonyaath": "sonyaath",
    "news18bangla": "news18bangla",
    "tv9bangla": "tv9bangla",
    "zee24ghanta": "zee24ghanta",
    "kolkatatv": "kolkatatv",
    "calcuttanews": "calcuttanews",
    "enter10bangla": "enter10bangla",
    "enterr10bangla": "enter10bangla",
    "enteertenbangla": "enter10bangla",
    "rupashibangla": "rupashibangla",
    "ruposhibangla": "rupashibangla",
    "rupasibangla": "rupashibangla",
    "khushboobangla": "khushboobangla",
    "khusbobangla": "khushboobangla",
    "b4umovie": "b4umovies",
    "b4umovies": "b4umovies",
    "goldminesmovie": "goldminesmovies",
    "goldminesmovies": "goldminesmovies",
    "nationalgiography": "nationalgeographic",
    "nationalgeographic": "nationalgeographic",
    "natgeo": "nationalgeographic",
    "travelxp": "travelchannel",
    "travelchannel": "travelchannel",
    "cgtndocumentary": "cgtndocumentary",
    "dwnews": "dw",
    "aljazeera": "aljazeera",
    "duronto": "durontotv",
    "durontotv": "durontotv",
    "rongeen": "rongeentv",
    "rongeentv": "rongeentv",
    "cartoonnetwork": "cartoonnetwork",
    "sonyyay": "sonyyay",
    "nickjr": "nickjr",
}

VALID_SHORT_NAMES = {
    "bbc", "cnn", "ndtv", "mtv", "btv", "rtv", "ntv", "gtv", "atn", "dbc", "satv",
    "hbo", "axn", "tlc", "fx", "fxx", "wb", "cbs", "nbc", "abc", "itv", "ptv", "ary",
    "geo", "dd", "wion", "cnbc", "espn", "tnt", "tbs", "amc", "vtv", "otv", "ktv",
    "mh1", "9xm", "e24", "b4u", "tv9", "tv5", "sky", "fox", "pbs", "hln", "cw", "id",
    "osn", "mbc", "dw", "nhk", "cgtn", "rt", "trt", "tcm", "mgm", "mnx", "vh1", "zing",
    "axs", "etv", "eet"
}

GARBAGE_EXACT_RE = re.compile(
    r"^(unknown|untitled|no\s*name|none|null|test|testing|test\s*live\s*tv|demo|sample|offline|error|"
    r"live|stream|live\s*stream|tv|channel|ch|server|link|backup|main|default|"
    r"index|playlist|master|chunklist|mono|tracks|video|audio|hd|fhd|sd|uhd|4k|"
    r"720p|1080p|480p|360p|hls|m3u8|ts|mp4)(\s*[-_#]?\s*\d+)?$",
    re.I
)


def is_garbage_channel_name(name):
    if not name or len(name.strip()) > 65:
        return True
    s = name.strip()
    low_s = s.lower()
    if any(bad in low_s for bad in ("http://", "https://", ".m3u8", ".stream", "#extvlcopt", "http-user-agent", "http-referrer", "user-agent=")):
        return True
    if looks_like_random_id(s):
        return True

    clean = re.sub(r"[\(\[].*?[\)\]]", " ", s).strip()
    clean = re.sub(r"\s+", " ", clean)
    if not clean or looks_like_random_id(clean):
        return True

    alnum_only = re.sub(r"[^a-zA-Z0-9\u0980-\u09ff]", "", clean)
    if not alnum_only or alnum_only.isdigit() or len(alnum_only) < 2:
        return True

    low_clean = clean.lower()
    if low_clean in VALID_SHORT_NAMES or alnum_only.lower() in VALID_SHORT_NAMES:
        return False

    if GARBAGE_EXACT_RE.match(low_clean):
        return True

    if re.search(r"\b(amg\d+[a-z0-9]*|amgplt\d+|tracks[-_]?v\d|chunklist|playlist|master|index|mono|hls|stream\d+|ch[-_]?\d{3,}|tv[-_]?\d{3,}|up\s*off)\b", low_clean):
        return True
    if re.fullmatch(r"[0-9a-f]{8,}", alnum_only.lower()):
        return True

    digits = sum(c.isdigit() for c in alnum_only)
    letters = len(alnum_only) - digits
    if letters == 0 or (digits >= 4 and digits > letters):
        return True

    return False


def norm(name):
    n = name.lower().strip()
    n = re.sub(r"^\d+\s*[.\-_|:]\s*", "", n)
    n = re.sub(r"[_#]\d+$", "", n)
    n = re.sub(r"\s*-\s*\d+$", "", n)
    n = re.sub(r"[\(\[【].*?[\)\]】]", " ", n)
    n = re.sub(r"\b\d{3,4}p\b", " ", n)
    n = re.sub(r"\b24x7\b", " ", n)
    n = n.replace("television", "tv")
    n = re.sub(r"[^a-z0-9\u0980-\u09ff\u09e6-\u09ef]+", " ", n)

    tokens = [t for t in n.split() if t not in NOISE]
    if not tokens:
        tokens = [t for t in n.split() if t not in {"hd", "fhd", "sd", "uhd", "4k", "live"}]
    raw_key = "".join(tokens) or re.sub(r"[^a-z0-9\u0980-\u09ff]+", "", name.lower())
    return CANONICAL_ALIASES.get(raw_key, raw_key)


def is_blocked_or_non_tv(c):
    if c.get("radio_tag"):
        return True
    name = c["name"]
    if is_garbage_channel_name(name):
        return True

    group = c.get("group", "")
    url = c.get("url", "")
    t = f"{name} {group}".lower()

    if any(m in url for m in ("[DEVICE_ID]", "[IFA]", "[APP_BUNDLE]", "[COUNTRY]", "{PSID}", "{APP_DOMAIN}")):
        return True

    if c.get("source") not in ("AynaOTT", "JagoBD", "FAST-TV") and EXPIRED_TOKEN_URL_RE.search(url):
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
    r"sony pal|sony max|sony pix|sony wah|colors|colors tv|colors rishtey|colors cineplex|"
    r"zee tv|zee anmol|zee cinema|zee classic|zee action|zee bollywood|zee news|zee hindustan|zee zing|zee zest|zee cafe|"
    r"zee business|zee salaam|&tv|and tv|and pictures|&pictures|and priv|&priv|dangal|ishara|shemaroo|big magic|bindass|"
    r"aaj tak|abp news|abp ganga|ndtv|india tv|news18|republic bharat|republic tv|tv9|dd national|dd news|dd bharati|"
    r"dd kisan|dd retro|dd india|sanskar|aastha|sadhna|b4u|mtv india|9xm|9x jalwa|9x tashan|9x jhakaas|zing|utv|"
    r"filamchi|goldmines|manoranjan|cineplex|news nation|india news|good news today|bharat 24|tez|zoom tv|mastiii|"
    r"movie plus|enter10|bflix|e24|times now|mirror now|et now|wion|india today|cnbc awaaz|cnbc tv18|zee bihar|"
    r"zee rajasthan|zee mp|zee up|news24 india|sudarshan|jansatta|shubh tv|dhamaal|maha movie|"
    r"wow cinema|abzy|pitara|popkorn|housefull|dil se|dhinchaak|bharat samachar|sadhna prime|tehelka tv|khushboo|yrf music)\b", re.I
)

KIDS_ANIMATION_RE = re.compile(
    r"\b(cartoon|cartoons|animation|anime|animax|crunchyroll|retrocrush|hidive|toon|toons|toonami|"
    r"cartoon network|cn hd|pogo|sony yay|hungama|super hungama|marvel hq|disney|disney channel|disney xd|disney jr|disney junior|"
    r"nickelodeon|nick|nick jr|nickjr|nicktoons|nick bangla|nick rewind|teen nick|"
    r"boomerang|baby tv|babytv|babyfirst|baby first|pbs kids|cbbc|cbeebies|pop tv|tiny pop|kix|"
    r"duronto|duronto tv|rongeen|rongeen tv|zb cartoon|discovery kids|"
    r"moonbug|moonbug kids|3abn kids|kids|kidz|junior|children|"
    r"spongebob|sonic the hedgehog|mario vs sonic|beyblade|pokemon|pokémon|doraemon|shinchan|motu patlu|"
    r"chhota bheem|green gold tv|gubbare|maha cartoon|sonic nickelodeon|tom and jerry|looney tunes|"
    r"peppa pig|paw patrol|blue's clues|blues clues|blaze and the monster|totally turtles|tmnt|ninja turtles|"
    r"icarly|power rangers|transformers|he-man|scooby|flintstones|jetsons|garfield|smurfs|ben 10)\b",
    re.I
)

INFORMATIVE_RE = re.compile(
    r"\b(discovery|discovery channel|discovery science|discovery turbo|discovery world|discovery\+|\bds\b|"
    r"animal planet|national geographic|national giography|nat geo|natgeo|nat geo wild|nat geo people|nat geo adventure|"
    r"history tv|history channel|history 18|history18|history tv18|sony bbc earth|bbc earth|bbc knowledge|"
    r"travel xp|travelxp|travel channel|fyi tv18|fyi tv|investigation discovery|id channel|science channel|"
    r"smithsonian|curiosity|curiositystream|love nature|wild earth|wildearth|sky nature|pbs|"
    r"nasa tv|esa tv|dw documentary|cgtn documentary|cgtndocumentary|epic tv|epic channel|documentary|docubay|planete\+|"
    r"crime \+ investigation|crime and investigation|bio channel|biography|h2 channel|military history|stingray naturescape)\b",
    re.I
)

KOLKATA_RE = re.compile(r"\b(jalsha movies|zee bangla cinema|colors bangla cinema|bangla cinema|"
                        r"bangla movies?|bengali movies?|bengali cinema)\b", re.I)

HINDI_MOVIE_CH_RE = re.compile(
    r"\b(star gold|sony max|sony wah|zee cinema|zee classic|zee action|zee bollywood|zee anmol cinema|"
    r"colors cineplex|and pictures|&pictures|utv movies|utv action|b4u movies|b4u movie|b4u kadak|"
    r"filamchi|goldmines|gold mines|manoranjan|enter10|bflix|movie plus|maha movie|wow cinema|abzy movies|"
    r"housefull movies|bollywood old|hindi movie classic|all time movies|public movies|roja movies|shubh cinema|the movie club)\b", re.I
)
ENGLISH_MOVIE_CH_RE = re.compile(
    r"\b(star movies|hbo|sony pix|zee cafe|and priv|&priv|movies now|mnx|mn\+|romedy now|axn|wb|warner bros|"
    r"paramount|amc|cinemax|showtime|tcm|hallmark|syfy|mgm|universal movies|sony movies|film4|sky cinema|"
    r"rakuten|plex movies|cinevault|miramax|lionsgate|starz|encore|epix)\b", re.I
)
ENGLISH_ENT_CH_RE = re.compile(
    r"\b(star world|colors infinity|comedy central|fox life|fox crime|fox news|fox business|fx|fxx|tlc|"
    r"bbc|cnn|cnbc|bloomberg|al jazeera|aljazeera|dw|dwnews|france 24|france24|france news 24|nhk|cgtn|trt world|euronews|"
    r"arirang|kbs world|cna|channel news asia|cbs|nbc|abc|fox|itv|channel 4|channel 5|e4|more4|voa|voice of america|"
    r"sky one|sky witness|sky atlantic|sky news|sky arts|bravo|lifetime|hgtv|food network|mtv|vh1|"
    r"a&e|tnt|tbs|usa network|cw|msnbc|hln|trutv|vice|dave|gold uk|alibi|drama uk|yesterday uk|eden uk|quest uk|really uk|pick tv|challenge tv|"
    r"axs|axs tv|pix 11|pix11|laff tv|theq|un web tv)\b", re.I
)

PAY_ENT_CH_RE = re.compile(
    r"\b(star plus|star bharat|star utsav|star jalsha|colors|rishtey|sony entertainment|sony tv|set hd|set india|"
    r"sony sab|sab tv|sony pal|sony aath|zee tv|zee anmol|zee bangla|zee news|zee hindustan|"
    r"zee zing|zee zest|zee business|zee salaam|&tv|and tv|dangal|ishara|shemaroo|big magic|bindass)\b", re.I
)
PAY_SPORTS_RE = re.compile(
    r"\b(star sports|sony ten|sony sports|ten sports|t sports|willow|willo tv|ptv sports|eurosport|"
    r"bein sports|beinsports|sky sports|bt sport|tnt sports|fox sports|espn|dazn|super sport|dd sports|sports 18|sports18|"
    r"geo super|a sports|premier sports|arena sport|sport tv|nova sport|canal\+ sport|fancode|"
    r"nfl network|golfchannel|golf channel|psl \+ ipl)\b", re.I
)

FOREIGN_COUNTRY_TAG_RE = re.compile(
    r"(\.(nl|rs|ru|tr|it|es|au|id|bg|gh|vn|ba|xk|al|cl|ec|cr|cm|ci|kh|mn|hu|at|ch|de|fr|br|pe|do|lt|pk|ro|cz|sk|pl|iq|ir|uz|kz|gr|fi|ug|gp|ee|kr|hn|ar|kw|sd|th|er|bo|pt)\b|"
    r"\b(netherlands|serbia|russia|russian|turkiye|turkey|italy|spain|españa|australia|indonesia|bulgaria|"
    r"ghana|vietnam|bosnia|kosovo|albania|chile|ecuador|costa rica|cameroon|ivory coast|cambodia|"
    r"mongolia|hungary|austria|switzerland|brazil|brasil|peru|dominican|san antonio|nashua|santa maria|"
    r"lompoc|east bridgewater|toscana|utrecht|noord|drenthe|rijnmond|oost|maastricht|meppel|"
    r"purmerend|veluwezoom|westerwolde|zulthe|dordrecht|manilva|malaga|calima|balkanweb|rai news|"
    r"bắc ninh|nghệ an|gia lai|silverbird|united kingdom|united states|czech republic|slovakia|colombia|finland|latin america|german|iraq|san clemente|andes|nacional|zeeland)\b)",
    re.I
)

FOREIGN_HOMONYM_DOMAINS = (
    "btnrwanda.com", "balkanweb.com", "ecuaradiotv.net", "rtv-ooe.at",
    "rtv.co.id", "dens.tv", "skygo.mn", "bitel.com.pe", "msinteractiva.com",
    "swagit.com", "telvue.com", "dogus.daioncdn.net", "nossatv", "43.252.18.195",
    "connectiktv.ddns.net", "mediawork.cz", "mediatailor.us-west-2.amazonaws.com",
    "skynewsau-live-premium.akamaized.net", "pishow.tv/ott/live/253", "ottlive.co.in/news24",
    "antenaplay.ro", "polar.cz", "tennet.ro", "cra.cz", "realitatea.net", "rtve.es",
    "rutube.ru", "tustreaming.cl", "tvkaista.net", "staratv.id", "telewebion.ir",
    "cinerama.uz", "desdeparaguay.net", "voxtvhd.com.br", "edgenextcdn.net", "mjunoon.tv",
    "tvradiozap.eu", "zettawiseroutes.com", "sb.err.ee", "artidijitalmedya.com",
    "181.224.255.210", "181.224.200.5", "yuppcdn.net/110322/channel24",
    "elitelatrade.com", "fawanews.sc", "193.47.62.41", "thaipbs", "byteark.com",
    "thaimomo.com", "acessaradios.com.br", "enhdtv.com", "powertvkannada.com"
)

BD_ONLY_RE = re.compile(
    r"(\b(bangladesh|bangladeshi|sangsad|shangshad|somoy|jamuna|ekattor|channel 24|dbc|independent|"
    r"atn bangla|atn news|atn islamic|atn music|news 24 bd|news24 tv|channel i|channel 9|channel s|channel 1 news|channel 52|"
    r"banglavision|bangla vision|maasranga|deepto|boishakhi|desh tv|desh bangla|deshe bideshe|"
    r"deshi tv|nagorik|gazi tv|g tv|asian tv|sa tv|satv|mohona|my tv|mytv|bijoy|ananda tv|anando tv|anandabarta|"
    r"nexus|t sports|gaan bangla|ekhon tv|global tv|green tv|bangla tv|spice tv|diganta|"
    r"islamic tv|madani channel bangla|madani ch\. bangla|peace tv bangla|ekushey|ekushe|etv ekushey|"
    r"enter tv|matribhumi|me tv|movie bangla|rajdhani tv|ja tv|jago news|nan tv|probashi tv|"
    r"jonmobhumi|doyel tv|cb24|ayna tv|ash tv|shikor tv|thikana tv|vision 24|news 21 bangla|news 21 hd|"
    r"banglar kontho|biswa bangla|biswabangla|বিশ্ব বাংলা|azan tv|b prime bangla|bprimebangla|nrb tv|"
    r"star news|time television|ranacable|amar bangla|aamar bangla|amardigital)\b|"
    r"^(?:\d+\s*\.\s*)?(btv|btv news|btv ctg|btv chattogram|btv chattrogram|rtv|rtv hd|ntv|ntv hd|ntv europe|gtv|gtv hd|news 24|news 24 hd|news24|etv)$)",
    re.I
)

INDIA_BANGLA_RE = re.compile(
    r"\b(star jalsha|jalsha|zee bangla|sony aath|colors bangla|sun bangla|akas aath|akash aath|aakaash aath|akash ahtt|"
    r"sangeet bangla|dhoom music|music bangla|musicbangla|rupashi bangla|ruposhi bangla|rupasi bangla|dd bangla|dd tripura|abp ananda|"
    r"star ananda|news18 bangla|news 18 bangla|etv bangla|zee 24 ghanta|24 ghanta|republic bangla|"
    r"r bangla|tv9 bangla|calcutta news|kolkata tv|r plus|rplus|cn tv|tara muzik|tara newz|tara tv|"
    r"bangla time|onkar news|high news|sadhu bangla|khabor 365|bangla jagao|bangla jago|nkh bangla|"
    r"ctvn|akd plus|bflix bangla|khoj khobor|newstime bangla|prime news bangla|"
    r"bangla plus|bangla express|kolkata|calcutta|west bengal|sananda|baraza indian bangla|"
    r"indian bangla|orange bangla|enter 10 bangla|enterr 10 bangla|enterr10 bangla|enteer ten bangla|enter10 bangla|"
    r"khushboo bangla|khusbo bangla|mon bangla|news live bangla|nk tv bangla|raatdin bangla|"
    r"salam bangla|snb cinema|sonar bangla|sonic bangla|u bangla|uphar bangla|upohar bangla|"
    r"zb bhakti|zb music|zb cinema|zodiak tv|jtbs classic|bhi channel|globe tv|"
    r"headlines tripura|friends tv|ananda barta|ananda livetv|ak bangla|ekhonkolkata|jhankar tv|sristi tv|stv bangla|channel vision|deen tv|deentv)\b",
    re.I
)


def classify_by_language_and_name(c):
    name = c["name"]
    group = c.get("group", "")
    tvg_id = c.get("tvg_id", "")
    url = c.get("url", "")
    lname = name.lower()
    lgroup = group.lower()
    lurl = url.lower()
    ltvg = tvg_id.lower()
    t = f"{lname} {lgroup}"

    if any(dom in lurl for dom in FOREIGN_HOMONYM_DOMAINS):
        return "DROP"
    if FOREIGN_COUNTRY_TAG_RE.search(f"{lname} {ltvg}") and not any(x in ltvg for x in (".bd", "aynaott", "jagobd")):
        return "DROP"

    detected_lang = c.get("stream_lang") or c.get("lang") or ""
    if detected_lang in DROP_LANG_CODES:
        return "DROP"

    compact_text = re.sub(r"[^a-z0-9]", "", f"{lname}{ltvg}")
    if any(kw in lname or kw in compact_text for kw in DROP_SUBSTRING_KEYWORDS):
        return "DROP"

    clean_lname = re.sub(r"^\d+\s*\.\s*", "", lname).strip()
    music = bool(re.search(r"\b(music|song|songs|gaan|mtv|vevo|hits|zing|sangeet|muzik|dhoom|9xm|9x)\b", clean_lname))
    movie_ch = bool(re.search(r"\b(movies?\s*(tv|channel|hd|plus|now|action|gold|club)?|cinema\s*(tv|hd)?|flix\s*tv)\b", clean_lname))

    if PAY_SPORTS_RE.search(t) or re.search(r"\b(sport|sports|cricket|football|soccer)\b", clean_lname):
        return "Sports"

    if (KIDS_ANIMATION_RE.search(clean_lname)
            or group == "Kids & Animation"
            or re.search(r"\b(kids|cartoon|animation|anime)\b", lgroup)):
        return "Kids & Animation"

    if INFORMATIVE_RE.search(clean_lname) or re.search(r"\b(documentary|science|wildlife|nature|history)\b", lgroup):
        return "Informative"

    if KOLKATA_RE.search(clean_lname):
        return "Kolkata Movies"
    if (INDIA_BANGLA_RE.search(clean_lname) or "kolkata" in clean_lname or "calcutta" in clean_lname
            or (detected_lang == "ben" and re.search(r"\b(india|indian|west bengal|kolkata)\b", lgroup))):
        return "Bangla Music" if music else "Bangla (India)"

    if HINDI_MOVIE_CH_RE.search(clean_lname):
        return "Hindi Movies"
    if (detected_lang == "hin" or HINDI_RE.search(clean_lname)) and music:
        return "Hindi Music"

    if ENGLISH_MOVIE_CH_RE.search(clean_lname):
        return "English Movies"
    if ENGLISH_ENT_CH_RE.search(clean_lname) and detected_lang != "ben":
        return "English Music" if music else "English Entertainment"

    if (detected_lang == "eng"
            or re.search(r"\b(english|usa|uk|united states|united kingdom|canada|australia|hollywood)\b", lgroup)
            or re.search(r"(\(|\[|\b)(us|usa|uk|ca|au)(\)|\]|\b)", clean_lname)) and not BD_ONLY_RE.search(clean_lname) and not BANGLA_RE.search(clean_lname):
        if music:
            return "English Music"
        if movie_ch:
            return "English Movies"
        return "English Entertainment"

    if (detected_lang == "hin"
            or HINDI_RE.search(clean_lname)
            or PAY_ENT_CH_RE.search(clean_lname)
            or re.search(r"\b(india|indian|hindi|bollywood)\b", lgroup)
            or re.search(r"(\(|\[)(in|ind|india)(\)|\])", clean_lname)):
        if "zing" in clean_lname or music:
            return "Hindi Music"
        if movie_ch:
            return "Hindi Movies"
        return "India"

    if (BD_ONLY_RE.search(clean_lname)
            or ".bd" in ltvg
            or "jagobd.com" in lurl
            or "gpcdn.net" in lurl
            or re.search(r"\bbangladesh\b", lgroup)):
        if music:
            return "Bangla Music"
        return "Bangladesh"

    if "aynaott.com" in lurl:
        if music:
            return "Bangla Music"
        return "Bangladesh"

    if detected_lang == "ben" or BANGLA_RE.search(clean_lname):
        if music:
            return "Bangla Music"
        return "Bangla (India)"

    if re.search(r"\b(english|hollywood|usa|uk|british|american)\b", t):
        if music:
            return "English Music"
        if movie_ch:
            return "English Movies"
        return "English Entertainment"

    if group in ALLOWED_GROUPS and group != "Bangladesh":
        return group

    return "DROP"


def dedupe_by_name(chs, limit):
    specific = {g for g, _c, _l in COMBOS}

    def rank(c):
        g = c["group"]
        return (0 if g in specific else 1 if g in GROUP_ORDER else 2,
                round(c.get("latency", 99), 2),
                -c.get("kbps", 0),
                SOURCE_RANK.get(c["source"], 9))

    kept, count = set(), {}
    for c in sorted(chs, key=rank):
        key = (c["group"], norm(c["name"]) or c["name"].lower())
        if count.get(key, 0) < limit:
            count[key] = count.get(key, 0) + 1
            kept.add(id(c))
    return [c for c in chs if id(c) in kept]


# ------------------------------------------------------------------ source health & sleeping pool sync
def update_source_health(checked_feed_urls, all_candidate_items):
    if not checked_feed_urls:
        return
    try:
        health_data = json.loads(HEALTH_FILE.read_text(encoding="utf-8")) if HEALTH_FILE.exists() else {}
    except Exception:
        health_data = {}

    try:
        sleeping_data = json.loads(SLEEPING_FILE.read_text(encoding="utf-8")) if SLEEPING_FILE.exists() else {}
        sleeping_set = set(sleeping_data.get("sleeping_urls", []))
    except Exception:
        sleeping_set = set()

    always_keep = {u for _, u in DIRECT_GLOBAL_FEEDS}
    alive_hits_by_feed = Counter()
    for c in all_candidate_items:
        f_url = c.get("feed_url")
        if f_url and c.get("alive") and c.get("group") in ALLOWED_GROUPS:
            alive_hits_by_feed[f_url] += 1

    working_count, newly_sleeping = 0, 0
    for f_url in set(checked_feed_urls):
        hits = alive_hits_by_feed.get(f_url, 0)
        if hits > 0:
            health_data[f_url] = {"fails": 0, "last_hits": hits}
            sleeping_set.discard(f_url)
            working_count += 1
        else:
            prev_fails = health_data.get(f_url, {}).get("fails", 0) + 1
            health_data[f_url] = {"fails": prev_fails, "last_hits": 0}
            if prev_fails >= 2 and f_url not in always_keep:
                if f_url not in sleeping_set:
                    sleeping_set.add(f_url)
                    newly_sleeping += 1

    HEALTH_FILE.write_text(json.dumps(health_data, indent=2), encoding="utf-8")
    SLEEPING_FILE.write_text(json.dumps({
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_sleeping": len(sleeping_set),
        "sleeping_urls": sorted(sleeping_set)
    }, indent=2), encoding="utf-8")
    log(f"[SourceHealth] {working_count} working feeds active | {newly_sleeping} moved to Sleeping Pool (Total sleeping: {len(sleeping_set)}).")


# ------------------------------------------------------------------ output (logo-free)
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
            clean_url = clean_stream_url(clean_url)
            for part in extra.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k.strip().lower() == "referer":
                        ref = v.strip()
                    elif k.strip().lower() == "user-agent":
                        ua = v.strip()
        else:
            clean_url = clean_stream_url(raw_url)

        if "jagobd.com" in clean_url and not ref:
            ref = "https://www.jagobd.com/"
        elif "aynaott.com" in clean_url and not ref:
            ref = "https://web.aynaott.com/"

        tvg_id = clean_tvg_id(c.get("tvg_id", ""))
        lang_attr = f' tvg-language="{q(c.get("stream_lang") or c.get("lang") or "")}"' if (c.get("stream_lang") or c.get("lang")) else ""
        lines.append(f'#EXTINF:-1 tvg-id="{q(tvg_id)}" tvg-name="{q(c["name"])}" '
                     f'tvg-logo=""{lang_attr} group-title="{q(c["group"])}",{c["name"]}')
        if ua:
            lines.append(f"#EXTVLCOPT:http-user-agent={ua}")
        if ref:
            lines.append(f"#EXTVLCOPT:http-referrer={ref}")
        lines.append(clean_url)
    target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_custom_channel_keys(path=CUSTOM_CHANNELS_FILE):
    if not path.exists():
        return set()
    keys = set()
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        k = norm(line)
        if k:
            keys.add(k)
    return keys


def build_custom_playlist(alive_channels, target_path=CUSTOM_OUTPUT):
    wanted_keys = load_custom_channel_keys()
    if not wanted_keys:
        return 0
    matched = [c for c in alive_channels if norm(c["name"]) in wanted_keys]
    matched.sort(key=lambda c: (
        GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99,
        c["name"].lower(),
        round(c.get("latency", 99), 2),
        -c.get("kbps", 0),
        SOURCE_RANK.get(c["source"], 9),
    ))
    write_playlist(matched, target_path)
    return len(matched)


def write_source_report(alive, custom_count):
    by_source = defaultdict(list)
    by_domain = defaultdict(list)

    for c in alive:
        src = c.get("source", "Unknown")
        clean_u = c["url"].split("|")[0].strip()
        dom = urllib.parse.urlparse(clean_u).netloc.lower() or "unknown"
        entry = f"{c['name']} [{c['group']}] ({int(c.get('latency', 0) * 1000)}ms | {int(c.get('kbps', 0))}KB/s)"
        by_source[src].append(entry)
        by_domain[dom].append(entry)

    rep = [
        f"=== Global IPTV Source & Channel Breakdown Report ({time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}) ===",
        f"Total High-Speed Alive Streams: {len(alive)} | Custom Matched Streams: {custom_count}\n",
        "--- 1. SUMMARY BY SOURCE PLATFORM / ENGINE ---"
    ]
    for src, ch_list in sorted(by_source.items(), key=lambda x: -len(x[1])):
        rep.append(f"  * {src}: {len(ch_list)} channels")

    rep.append("\n--- 2. TOP STREAMING SERVERS / DOMAINS ---")
    for dom, ch_list in sorted(by_domain.items(), key=lambda x: -len(x[1]))[:40]:
        rep.append(f"  * {dom}: {len(ch_list)} channels")

    rep.append("\n--- 3. CHANNEL LIST BY SOURCE ENGINE ---")
    for src, ch_list in sorted(by_source.items(), key=lambda x: -len(x[1])):
        rep.append(f"\n[{src}] ({len(ch_list)} channels):")
        for item in sorted(set(ch_list)):
            rep.append(f"  - {item}")

    rep.append("\n--- 4. CHANNEL LIST BY SERVER / DOMAIN (TOP 25) ---")
    for dom, ch_list in sorted(by_domain.items(), key=lambda x: -len(x[1]))[:25]:
        rep.append(f"\n[{dom}] ({len(ch_list)} channels):")
        for item in sorted(set(ch_list)):
            rep.append(f"  - {item}")

    SOURCE_REPORT.write_text("\n".join(rep) + "\n", encoding="utf-8")


def main():
    start_time = time.time()
    id_to_name = fetch_official_channel_names()

    ckpt = load_checkpoint()
    if ckpt:
        items = ckpt.get("channels", [])
        checked_feed_urls = ckpt.get("checked_feed_urls", [])
        original_created_at = ckpt.get("created_at", start_time)
    else:
        pool = []
        checked_feed_urls = []
        original_created_at = start_time

        scrapers = (
            ("AynaOTT", scrape_aynaott),
            ("JagoBD", scrape_jagobd),
            ("InternetSources", lambda: scrape_discovered_and_global_feeds(checked_feed_urls)),
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
            raw = clean_stream_url(c["url"].split("|")[0].strip())
            if not raw or raw in seen or is_blocked_or_non_tv(c):
                continue
            if any(b in c["url"] for b in BLOCK_URLS):
                continue

            pre_grp = classify_by_language_and_name(c)
            if pre_grp == "DROP":
                continue

            seen.add(raw)
            c["checked"] = False
            items.append(c)

        log(f"Total unique BD/Bangla-Hindi/English/Informative/Kids candidate: {len(items)}")
        save_checkpoint(items, checked_feed_urls, original_created_at)

    paused = check_all_with_resume(items, checked_feed_urls, start_time, original_created_at)
    if paused:
        done_cnt = sum(1 for c in items if c.get("checked"))
        alive_cnt = sum(1 for c in items if c.get("alive"))
        log(f"[PAUSED] Time limit ({MAX_RUNTIME_SECONDS}s) reached! Progress saved: {done_cnt}/{len(items)} checked ({alive_cnt} alive). Poroborti run e ekhan thekei resume hobe.")
        return 0

    alive_raw = [c for c in items if c.get("alive")]

    alive = []
    for c in alive_raw:
        grp = classify_by_language_and_name(c)
        if grp not in ALLOWED_GROUPS:
            continue
        if (c["source"] not in ("AynaOTT", "JagoBD") and grp in ("Bangladesh", "Kolkata Movies", "Bangla Music")
                and any(h in c["url"].lower() for h in NOT_BANGLA_HOSTS)):
            continue
        c["group"] = grp
        alive.append(c)

    update_source_health(checked_feed_urls, alive)

    log(f"High-Speed Alive (Filtered Groups only): {len(alive)} / {len(items)}")

    prev = OUTPUT.read_text(encoding="utf-8", errors="ignore").count("#EXTINF") if OUTPUT.exists() else 0
    if prev >= 20 and len(alive) < prev * MIN_KEEP_RATIO:
        log(f"[SAFETY] Alive ({len(alive)}) ager playlist ({prev}) er theke onek kom. Overwrite kora holo na.")
        CHECKPOINT_FILE.unlink(missing_ok=True)
        return 1

    for c in alive:
        tid = clean_tvg_id(c.get("tvg_id", "")).lower()
        tid_base = tid.split("@")[0]

        if tid in id_to_name:
            c["name"] = id_to_name[tid]
        elif tid_base in id_to_name:
            c["name"] = id_to_name[tid_base]
        else:
            c["name"] = clean_display_name(c["name"])

    alive = [c for c in alive if not is_garbage_channel_name(c["name"])]

    if MAX_LINKS_PER_CHANNEL:
        before = len(alive)
        alive = dedupe_by_name(alive, MAX_LINKS_PER_CHANNEL)
        log(f"Duplicate naam bad: {before} -> {len(alive)}")

    alive.sort(key=lambda c: (GROUP_ORDER.index(c["group"]) if c["group"] in GROUP_ORDER else 99,
                              c["name"].lower(),
                              round(c.get("latency", 99), 2),
                              -c.get("kbps", 0),
                              SOURCE_RANK.get(c["source"], 9)))

    cnt = Counter(c["group"] for c in alive)
    log("Language & Category wise high-speed channel: " + ", ".join(f"{g}: {n}" for g, n in cnt.most_common()))

    write_playlist(alive, OUTPUT)

    custom_count = build_custom_playlist(alive, CUSTOM_OUTPUT)
    write_source_report(alive, custom_count)

    CHECKPOINT_FILE.unlink(missing_ok=True)

    elapsed = int(time.time() - start_time)
    log(f"{OUTPUT} e {len(alive)} ta high-speed channel, {CUSTOM_OUTPUT} e {custom_count} ta custom channel link ebong {SOURCE_REPORT} save hoise (Total time: {elapsed // 60}m {elapsed % 60}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
