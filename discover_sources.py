"""
Zero-Rate-Limit Deep Multi-Platform IPTV Source Discovery Engine
Features:
- PAT Authentication + ETag / If-None-Match Conditional Caching (stores lightweight ETag strings only)
- Header-Aware Sequential Request Queue with Secondary Rate-Limit Cool-Down on 403/429
- Deep Tree & Raw File Harvesting (zero rate limit on raw.githubusercontent.com)
- Crawls Priority Repos, Forks, Maintainer Sibling Repos, GitLab, Codeberg, Gists & Web Dorks
"""
import json, os, random, re, ssl, time
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
OUT_JSON = Path("discovered_sources.json")
OUT_REPORT = Path("bot_report.txt")
HEALTH_JSON = Path("source_health.json")
SLEEPING_JSON = Path("sleeping_sources.json")
ETAG_CACHE_JSON = Path("etag_cache.json")

CANARY_BATCH_SIZE = 1200

BD_PRIORITY_REPOS = [
    "saeidrahmanbd/BDIX-IPTV",
    "abusaeeidx/Mrgify-BDIX-IPTV",
    "time2shine/Rokon-IPTV",
    "dhrubonai/Free-BD-IPTV",
    "Zaman-Topu/Ip-tv-Collection",
    "Shadmanislam/bdiptv",
    "sm-monirulislam/SM-IPTV",
    "bugsfreeweb/LiveTVCollector",
    "iptv-org/iptv",
    "Free-TV/IPTV",
    "amazeyourself/adaptive-streams",
]

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
GH_HEADERS = {
    "User-Agent": UA,
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2026-03-10",
}
if GITHUB_TOKEN:
    GH_HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

BOT_SEARCH_QUERIES = [
    "iptv pushed:>2026-01-01",
    "m3u8 playlist pushed:>2026-01-01",
    "bdix tv",
    "bdix iptv",
    "bangla tv m3u8",
    "bangladesh iptv",
    "indian tv m3u8",
    "hindi tv m3u8",
    "kolkata tv m3u8",
    "english tv m3u8",
    "fancode m3u8",
    "sports iptv m3u8",
    "cartoon m3u8",
    "kids tv m3u8",
    "anime m3u8",
    "documentary m3u8",
    "samsungtvplus m3u8",
    "plutotv m3u8",
    "topic:iptv",
    "topic:m3u8",
    "topic:bangla-tv",
    "topic:indian-tv",
    "topic:cartoon",
    "topic:anime",
    "star jalsha m3u8",
    "zee bangla m3u8",
    "colors bangla m3u8",
    "sony aath m3u8",
    "bdix playlist m3u",
    "bangladesh live tv m3u8",
]

CODE_SIGNATURE_QUERIES = [
    '"cron:" "m3u" path:.github/workflows',
    '"schedule:" "m3u8" path:.github/workflows',
    '"EXTM3U" "requests" extension:py',
    '"EXTM3U" "urllib" extension:py',
    '"#EXTINF" "Somoy" extension:m3u',
    '"#EXTINF" "Jamuna" extension:m3u8',
    '"#EXTINF" "Star Jalsha" extension:m3u',
    '"#EXTINF" "Zee Bangla" extension:m3u8',
    '"#EXTINF" "Aaj Tak" extension:m3u',
    '"#EXTINF" "Cartoon Network" extension:m3u',
    '"#EXTINF" "Discovery" extension:m3u',
    '"aynaott" extension:py',
    '"jagobd" extension:py',
    '"gpcdn.net" extension:m3u',
    '"app24.jagobd.com.bd" extension:m3u',
    '"ncare.live" extension:m3u',
    '"tangotv.in" extension:m3u',
]

ALT_GIT_KEYWORDS = [
    "iptv", "m3u8", "m3u", "bdix", "bangla-tv", "bangladesh-tv", "live-tv",
    "star-jalsha", "somoy-tv", "indian-tv", "hindi-tv", "sports-tv", "fast-tv",
    "cartoon", "kids-tv", "anime", "documentary", "playlist", "bdix-m3u", "bdix-iptv"
]

WEB_DORK_QUERIES = [
    'site:pastebin.com "#EXTINF" "Somoy"',
    'site:pastebin.com "#EXTINF" "Jamuna"',
    'site:pastebin.com "#EXTINF" "Star Jalsha"',
    'site:pastebin.com "#EXTINF" "Zee Bangla"',
    'site:pastebin.com "#EXTINF" "Hindi"',
    'site:pastebin.com "#EXTINF" "Cartoon Network"',
    'site:pastebin.com "#EXTINF" "Discovery"',
    'site:pastebin.com "#EXTINF" "m3u8"',
    'site:rentry.co "#EXTINF" "m3u8"',
    'site:telegra.ph "#EXTINF" "m3u8"',
    'site:dpaste.org "#EXTINF" "m3u8"',
    'site:paste.ee "#EXTINF" "m3u8"',
    'site:bitbucket.org "#EXTINF" "m3u8"',
    'site:huggingface.co/datasets "m3u8" "iptv"',
    'site:gist.github.com "#EXTINF" "Bangla"',
    'site:gist.github.com "#EXTINF" "Hindi"',
    'site:gist.github.com "#EXTINF" "Cartoon"',
    'site:gist.github.com "#EXTINF" "m3u8"',
    '"#EXTINF:-1" "gpcdn.net"',
    '"#EXTINF:-1" "app24.jagobd.com.bd"',
    '"#EXTINF:-1" "workers.dev" "m3u8"',
    'intitle:"index of" "playlist.m3u"',
    'intitle:"index of" "tv.m3u8"',
    'site:pastebin.com "#EXTINF" "Jalsha"',
    'site:pastebin.com "#EXTINF" "BDIX"',
    'site:rentry.co "#EXTINF" "Bangla"',
    'site:gist.github.com "#EXTINF" "Jalsha"',
]

BUILTIN_DIRECT_FEEDS = [
    "https://i.mjh.nz/SamsungTVPlus/all.m3u8",
    "https://i.mjh.nz/PlutoTV/all.m3u8",
    "https://i.mjh.nz/Plex/all.m3u8",
    "https://i.mjh.nz/Roku/all.m3u8",
    "https://i.mjh.nz/Stirr/all.m3u8",
    "https://www.apsattv.com/lg.m3u",
    "https://www.apsattv.com/vizio.m3u",
    "https://www.apsattv.com/xumo.m3u",
    "https://www.apsattv.com/distrotv.m3u",
    "https://www.apsattv.com/localnow.m3u",
    "https://www.apsattv.com/tubi.m3u",
    "https://www.apsattv.com/redbox.m3u",
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
    "https://iptv-org.github.io/iptv/countries/us.m3u",
    "https://iptv-org.github.io/iptv/countries/uk.m3u",
    "https://iptv-org.github.io/iptv/countries/ca.m3u",
    "https://iptv-org.github.io/iptv/countries/au.m3u",
    "https://iptv-org.github.io/iptv/countries/nz.m3u",
    "https://iptv-org.github.io/iptv/countries/sg.m3u",
    "https://iptv-org.github.io/iptv/categories/kids.m3u",
    "https://iptv-org.github.io/iptv/categories/animation.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
    "https://raw.githubusercontent.com/saeidrahmanbd/BDIX-IPTV/main/IPTV-Playlist.m3u",
    "https://raw.githubusercontent.com/saeidrahmanbd/BDIX-IPTV/main/International-Playlist.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/Mrgify-BDIX-IPTV/main/playlist.m3u",
    "https://raw.githubusercontent.com/time2shine/Rokon-IPTV/main/playlist.m3u",
]

URL_RE = re.compile(r'https?://[a-zA-Z0-9.-]+(?:/[a-zA-Z0-9._~:/?#\[\]@!$&\'()*+,;=-]*)?')

IGNORE_DOMAINS = (
    "github.com", "gitlab.com", "codeberg.org", "duckduckgo.com",
    "shields.io", "google.com", "w3.org", "schemas.xmlsoap.org",
    "json-schema.org", "opensource.org", "creativecommons.org",
    "example.com", "localhost", "127.0.0.1", "api.github.com",
    "twitter.com", "x.com", "facebook.com", "youtube.com", "youtu.be",
    "t.me", "telegram.me", "discord.gg", "discord.com", "linkedin.com",
    "wikipedia.org", "stackoverflow.com", "reddit.com", "medium.com"
)

IGNORE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tiff",
    ".css", ".scss", ".less", ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mkv", ".avi", ".mp3", ".wav", ".ogg",
    ".zip", ".tar", ".gz", ".rar", ".7z", ".pdf", ".md", ".exe", ".apk"
)

CAPTURE_URL_HINTS = (
    "/api/", "api.", "/live", "live.", "/stream", "stream.", ".m3u", ".m3u8",
    "/cdn", "cdn.", "/portal", "portal.", "/play", "play.", "iptv",
    "mjh.nz", "apsattv.com", "gist.githubusercontent", "pastebin.com/raw",
    "rentry.co", "telegra.ph", "dpaste.org", "paste.ee", "huggingface.co",
    "bitbucket.org", "bdix", "bangla", "jagobd", "aynaott", "workers.dev",
    "pages.dev", "onrender.com", "vercel.app", "gpcdn.net", "ncare.live",
    "tangotv.in", "ottlive.co.in", "livebox.co.in", "sonarbanglatv.com",
    "amagi.tv", "akamaized.net", "cloudfront.net", "wurl.com", "pluto.tv", "samsungtv.plus"
)


def log(msg):
    print(msg, flush=True)


def load_json_safe(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def is_syntactically_valid_url(url, self_repo_prefix=""):
    if "@" in url.split("//", 1)[-1].split("/", 1)[0]:
        return False
    low_u = url.lower().split("?")[0].split("#")[0]
    if low_u.endswith(IGNORE_EXTENSIONS):
        return False
    if self_repo_prefix and self_repo_prefix in low_u:
        return False
    if any(d in low_u for d in IGNORE_DOMAINS):
        return False
    if url in BUILTIN_DIRECT_FEEDS:
        return True
    if any(k in low_u for k in CAPTURE_URL_HINTS):
        return True
    return False


class GitHubAPIClient:
    """Sequential, ETag-cached, header-aware GitHub API client with secondary-limit protection."""

    def __init__(self, headers, etag_path):
        self.headers = dict(headers)
        self.etag_path = etag_path
        raw_cache = load_json_safe(etag_path, {})
        self.cache = {}
        if isinstance(raw_cache, dict):
            for k, v in raw_cache.items():
                if isinstance(v, str):
                    self.cache[k] = v
                elif isinstance(v, dict) and v.get("etag"):
                    self.cache[k] = v["etag"]
        self.search_paused_until = 0.0
        self.code_search_paused_until = 0.0
        self.core_paused_until = 0.0
        self.consecutive_core_403 = 0
        self.stats = {"304_hits": 0, "200_hits": 0, "rate_waits": 0}

    def save_cache(self):
        if len(self.cache) > 5000:
            keys = list(self.cache.keys())[-5000:]
            self.cache = {k: self.cache[k] for k in keys}
        tmp_path = self.etag_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(self.cache, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(self.etag_path)

    def get(self, url, min_delay=0.8, retries=2, category="core"):
        now = time.time()
        if category == "search" and now < self.search_paused_until:
            time.sleep(self.search_paused_until - now)
        elif category == "code_search" and now < self.code_search_paused_until:
            time.sleep(self.code_search_paused_until - now)
        elif category == "core" and now < self.core_paused_until:
            time.sleep(min(self.core_paused_until - now, 30))

        req_headers = dict(self.headers)
        cached_etag = self.cache.get(url)
        if cached_etag:
            req_headers["If-None-Match"] = cached_etag

        for attempt in range(retries + 1):
            time.sleep(min_delay)
            try:
                req = urllib.request.Request(url, headers=req_headers)
                with urllib.request.urlopen(req, timeout=20, context=SSL_CTX) as r:
                    self._respect_rate_headers(r.headers, category)
                    if category == "core":
                        self.consecutive_core_403 = 0
                    etag = r.headers.get("ETag")
                    data = json.loads(r.read().decode("utf-8", errors="ignore"))
                    if etag:
                        self.cache[url] = etag
                    self.stats["200_hits"] += 1
                    return data
            except urllib.error.HTTPError as e:
                if e.code == 304:
                    self._respect_rate_headers(e.headers, category)
                    if category == "core":
                        self.consecutive_core_403 = 0
                    self.stats["304_hits"] += 1
                    return {}
                if e.code in (403, 429):
                    self.stats["rate_waits"] += 1
                    if category == "core":
                        self.consecutive_core_403 += 1
                        if self.consecutive_core_403 >= 4:
                            self.core_paused_until = time.time() + 75
                            self.consecutive_core_403 = 0
                            log("[Queue Backoff 403] core secondary limit -> 75s cool-down...")
                            time.sleep(75)
                            return {}
                    ra = int(e.headers.get("Retry-After", 0) or 0)
                    rem = e.headers.get("X-RateLimit-Remaining")
                    rst = int(e.headers.get("X-RateLimit-Reset", 0) or 0)
                    if rem == "0" and rst > time.time():
                        wait_sec = min(int(rst - time.time()) + 3, 90)
                    elif ra > 0:
                        wait_sec = min(ra + 2, 90)
                    else:
                        wait_sec = min(18 * (2 ** attempt) + random.uniform(1, 4), 60)
                    if attempt < retries:
                        log(f"[Queue Backoff {e.code}] {category} limit -> {int(wait_sec)}s wait...")
                        time.sleep(wait_sec)
                        continue
                    return {}
                return {}
            except Exception:
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))
                    continue
                return {}
        return {}

    def _respect_rate_headers(self, hdrs, category):
        try:
            rem = int(hdrs.get("X-RateLimit-Remaining", 999))
            rst = int(hdrs.get("X-RateLimit-Reset", 0))
            if rem <= 2 and rst > time.time():
                pause = min(rst - time.time() + 2, 90)
                if category == "search":
                    self.search_paused_until = time.time() + pause
                elif category == "code_search":
                    self.code_search_paused_until = time.time() + pause
                elif category == "core":
                    self.core_paused_until = time.time() + pause
        except Exception:
            pass


def fetch_json_external(url, headers=None, retries=1):
    hdrs = headers or {"User-Agent": UA, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=18, context=SSL_CTX) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except Exception:
            if attempt < retries:
                time.sleep(2)
    return {}


def fetch_text(url, timeout=10, post_data=None):
    try:
        hdrs = {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if post_data is not None:
            hdrs["Content-Type"] = "application/x-www-form-urlencoded"
            hdrs["Referer"] = "https://lite.duckduckgo.com/"
        req = urllib.request.Request(url, data=post_data, headers=hdrs)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return r.read(1_000_000).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def canary_probe_url(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=4, context=SSL_CTX) as r:
            if r.status not in (200, 206):
                return False
            sample = r.read(4096).decode("utf-8", errors="ignore")
            return "#EXTINF" in sample or ".m3u8" in sample.lower()
    except Exception:
        return False


def revive_sleeping_sources(sleeping_set, health_data):
    if not sleeping_set:
        return set()
    candidates = list(sleeping_set)
    batch = random.sample(candidates, min(CANARY_BATCH_SIZE, len(candidates)))
    revived = set()
    with ThreadPoolExecutor(max_workers=80) as ex:
        fut_map = {ex.submit(canary_probe_url, u): u for u in batch}
        for f in as_completed(fut_map):
            u = fut_map[f]
            if f.result():
                revived.add(u)
                health_data[u] = {"fails": 0, "last_hits": 1, "revived_at": time.strftime("%Y-%m-%d")}
    if revived:
        log(f"[CanaryRevival] Sleeping pool theke {len(revived)}/{len(batch)} ti source abar active hoyeche!")
    return revived


def load_existing_data():
    old = load_json_safe(OUT_JSON, {})
    old_bots = {}
    for b in old.get("bots", []):
        if "repo" in b:
            prefix = f"raw.githubusercontent.com/{b['repo'].lower()}/"
            b["upstream_sources"] = [u for u in b.get("upstream_sources", []) if is_syntactically_valid_url(u, prefix)]
            old_bots[b["repo"]] = b
    old_upstream = {u for u in old.get("all_upstream_sources", []) if is_syntactically_valid_url(u)}
    old_upstream.update(BUILTIN_DIRECT_FEEDS)
    old_playlists = set(old.get("all_bot_playlists", []))
    log(f"Ager {len(old_bots)} ti repo o {len(old_upstream)} ti active upstream source load holo.")
    return old_bots, old_upstream, old_playlists


def add_repo_item(found, item, is_auto_bot=False):
    fn = item.get("full_name")
    if not fn or fn in found:
        return
    found[fn] = {
        "repo": fn,
        "platform": "github",
        "branch": item.get("default_branch", "main"),
        "updated": (item.get("pushed_at") or "")[:10],
        "stars": item.get("stargazers_count", 0),
        "is_auto_bot": is_auto_bot,
        "playlists": [],
        "upstream_sources": [],
    }


def crawl_forks_and_owners(gh, found, seed_repos):
    owners_seen = set()
    for fn in seed_repos:
        if fn.lower() in ("iptv-org/iptv", "free-tv/iptv"):
            continue
        owner = fn.split("/")[0]
        owners_seen.add(owner)
        forks = gh.get(f"https://api.github.com/repos/{fn}/forks?sort=newest&per_page=100", min_delay=0.7, retries=1, category="core")
        if isinstance(forks, list):
            for fk in forks:
                if (fk.get("pushed_at") or "") >= "2025-06-01":
                    add_repo_item(found, fk, is_auto_bot=True)
                    owners_seen.add(fk["full_name"].split("/")[0])

    for owner in list(owners_seen)[:140]:
        if owner.lower() in ("iptv-org", "free-tv"):
            continue
        urepos = gh.get(f"https://api.github.com/users/{owner}/repos?sort=pushed&per_page=100", min_delay=0.7, retries=1, category="core")
        if isinstance(urepos, list):
            for r in urepos:
                r_text = f"{r.get('name', '')} {r.get('description', '')}".lower()
                if any(k in r_text for k in ("iptv", "m3u", "tv", "bdix", "live", "stream", "playlist")):
                    add_repo_item(found, r, is_auto_bot=True)


def fetch_bd_priority_and_network(gh):
    found = {}
    for fn in BD_PRIORITY_REPOS:
        item = gh.get(f"https://api.github.com/repos/{fn}", min_delay=0.6, retries=1, category="core")
        if isinstance(item, dict) and item.get("full_name"):
            add_repo_item(found, item, is_auto_bot=True)

    crawl_forks_and_owners(gh, found, list(found.keys()))
    log(f"[Deep-Network] Priority repos + Forks + Maintainer repos milie {len(found)} ti repo load holo.")
    return found


def search_github(gh):
    found = fetch_bd_priority_and_network(gh)

    for q in BOT_SEARCH_QUERIES:
        for page in (1, 2):
            url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}&sort=updated&per_page=100&page={page}"
            data = gh.get(url, min_delay=3.2, retries=2, category="search")
            items = data.get("items", []) if isinstance(data, dict) else []
            if not items:
                break
            for item in items:
                add_repo_item(found, item, is_auto_bot=False)
            if len(items) < 100:
                break

    if GITHUB_TOKEN:
        for cq in CODE_SIGNATURE_QUERIES:
            c_url = f"https://api.github.com/search/code?q={urllib.parse.quote(cq)}&per_page=100&page=1"
            c_data = gh.get(c_url, min_delay=7.2, retries=2, category="code_search")
            for item in (c_data.get("items", []) if isinstance(c_data, dict) else []):
                repo_obj = item.get("repository", {})
                fn = repo_obj.get("full_name")
                if fn and fn not in found:
                    found[fn] = {
                        "repo": fn,
                        "platform": "github",
                        "branch": repo_obj.get("default_branch", "main"),
                        "updated": time.strftime("%Y-%m-%d"),
                        "stars": 0,
                        "is_auto_bot": True,
                        "playlists": [],
                        "upstream_sources": [],
                    }

    bd_in_seeds = [
        fn for fn in found
        if any(k in fn.lower() for k in ("bd", "bangla", "bdix", "india", "hindi", "kolkata", "jalsha", "zee", "sports"))
    ][:60]
    crawl_forks_and_owners(gh, found, bd_in_seeds)
    return found


def search_gitlab_and_codeberg():
    extra_playlists = set()
    extra_repos = {}

    for kw in ALT_GIT_KEYWORDS:
        for page in (1, 2, 3):
            gl_url = f"https://gitlab.com/api/v4/projects?search={urllib.parse.quote(kw)}&order_by=last_activity_at&per_page=100&page={page}"
            projects = fetch_json_external(gl_url, retries=0)
            if isinstance(projects, list) and projects:
                for p in projects:
                    path_ns = p.get("path_with_namespace")
                    branch = p.get("default_branch") or "main"
                    if path_ns:
                        repo_key = f"gitlab:{path_ns}"
                        extra_repos[repo_key] = {
                            "repo": repo_key,
                            "platform": "gitlab",
                            "branch": branch,
                            "updated": (p.get("last_activity_at") or "")[:10],
                            "stars": p.get("star_count", 0),
                            "is_auto_bot": True,
                            "playlists": [],
                            "upstream_sources": [],
                        }
                        for fname in ("playlist.m3u", "playlist.m3u8", "index.m3u", "index.m3u8", "iptv.m3u", "tv.m3u", "bangla.m3u", "india.m3u", "live.m3u", "kids.m3u", "cartoon.m3u", "all.m3u"):
                            extra_playlists.add(f"https://gitlab.com/{path_ns}/-/raw/{branch}/{fname}")
                if len(projects) < 100:
                    break
            else:
                break
            time.sleep(1.2)

    cb_headers = {
        "User-Agent": "IPTV-Playlist-Aggregator/1.0 (+https://github.com)",
        "Accept": "application/json"
    }
    for kw in ALT_GIT_KEYWORDS[:6]:
        time.sleep(2.5)
        cb_url = f"https://codeberg.org/api/v1/repos/search?q={urllib.parse.quote(kw)}&sort=updated&limit=50"
        res = fetch_json_external(cb_url, headers=cb_headers, retries=0)
        for item in res.get("data", []):
            fn = item.get("full_name")
            branch = item.get("default_branch") or "main"
            if fn:
                repo_key = f"codeberg:{fn}"
                extra_repos[repo_key] = {
                    "repo": repo_key,
                    "platform": "codeberg",
                    "branch": branch,
                    "updated": (item.get("updated_at") or "")[:10],
                    "stars": item.get("stars_count", 0),
                    "is_auto_bot": True,
                    "playlists": [],
                    "upstream_sources": [],
                }
                for fname in ("playlist.m3u", "playlist.m3u8", "index.m3u", "tv.m3u", "iptv.m3u", "live.m3u"):
                    extra_playlists.add(f"https://codeberg.org/{fn}/raw/branch/{branch}/{fname}")

    log(f"[GitLab & Codeberg] {len(extra_repos)} ti external repo o {len(extra_playlists)} ti candidate feed paoa gese.")
    return extra_repos, extra_playlists


def normalize_web_discovered_url(decoded):
    decoded = decoded.strip().rstrip(".,;\'\")}]")
    if not decoded.startswith("http"):
        return None
    if "pastebin.com/" in decoded and "/raw/" not in decoded:
        paste_id = decoded.rstrip("/").split("/")[-1]
        if len(paste_id) >= 6 and paste_id.isalnum():
            return f"https://pastebin.com/raw/{paste_id}"
    elif "rentry.co/" in decoded and not decoded.endswith("/raw"):
        slug = decoded.rstrip("/").split("/")[-1]
        if slug:
            return f"https://rentry.co/{slug}/raw"
    elif "dpaste.org/" in decoded and "/raw" not in decoded:
        return decoded.rstrip("/") + "/raw"
    elif "paste.ee/p/" in decoded:
        return decoded.replace("/p/", "/r/")
    elif "huggingface.co/datasets/" in decoded and "/blob/" in decoded:
        return decoded.replace("/blob/", "/resolve/")
    elif "bitbucket.org/" in decoded and "/src/" in decoded:
        return decoded.replace("/src/", "/raw/")
    elif "gist.github.com/" in decoded and "gist.githubusercontent.com" not in decoded:
        parts = [p for p in urllib.parse.urlparse(decoded).path.split("/") if p]
        if len(parts) >= 2:
            return f"https://gist.githubusercontent.com/{parts[0]}/{parts[1]}/raw"
    elif any(h in decoded for h in ("telegra.ph/", "gist.githubusercontent.com/", ".m3u", ".m3u8")):
        return decoded
    return None


def search_web_pastes_and_gists(gh):
    discovered_urls = set()

    for page in range(1, 8):
        gists = gh.get(f"https://api.github.com/gists/public?per_page=100&page={page}", min_delay=0.8, retries=1, category="core")
        if isinstance(gists, list) and gists:
            for g in gists:
                desc = (g.get("description") or "").lower()
                files = g.get("files") or {}
                for fname, fmeta in files.items():
                    low_f = fname.lower()
                    if low_f.endswith((".m3u", ".m3u8")) or any(k in f"{desc} {low_f}" for k in ("iptv", "m3u", "bangla", "hindi", "bdix", "live tv", "cartoon", "kids", "sports")):
                        raw_u = fmeta.get("raw_url")
                        if raw_u:
                            discovered_urls.add(raw_u)

    for dq in WEB_DORK_QUERIES:
        post_bytes = urllib.parse.urlencode({"q": dq}).encode("utf-8")
        html = fetch_text("https://lite.duckduckgo.com/lite/", timeout=12, post_data=post_bytes)
        if html:
            raw_links = re.findall(r'uddg=([^&"\']+)', html)
            if not raw_links:
                raw_links = re.findall(r'class=["\']result-link["\'][^>]*href=["\']([^"\']+)["\']', html, re.I)
            for raw_link in raw_links:
                decoded = urllib.parse.unquote(raw_link)
                norm_u = normalize_web_discovered_url(decoded)
                if norm_u:
                    discovered_urls.add(norm_u)
        time.sleep(3.0)

    log(f"[Web Pastes, Datasets & Gists] {len(discovered_urls)} ti global URL paoa gese.")
    return discovered_urls


def discover():
    if not GITHUB_TOKEN:
        log("[Warning] GITHUB_TOKEN set kora nai! Rate limit hote pare.")

    gh = GitHubAPIClient(GH_HEADERS, ETAG_CACHE_JSON)
    health_data = load_json_safe(HEALTH_JSON, {})
    sleeping_data = load_json_safe(SLEEPING_JSON, {})
    sleeping_set = set(sleeping_data.get("sleeping_urls", []))

    merged_bots, merged_upstream, merged_playlists = load_existing_data()

    gh_found = search_github(gh)
    alt_repos, alt_playlists = search_gitlab_and_codeberg()
    web_paste_urls = search_web_pastes_and_gists(gh)

    repo_updated_revivals = 0
    for repo_key, new_info in {**gh_found, **alt_repos}.items():
        old_info = merged_bots.get(repo_key)
        if old_info and new_info.get("updated") and new_info["updated"] > old_info.get("updated", ""):
            for u in old_info.get("playlists", []) + old_info.get("upstream_sources", []):
                if u in sleeping_set:
                    sleeping_set.remove(u)
                    health_data[u] = {"fails": 0, "last_hits": 1, "revived_by": "repo_push"}
                    repo_updated_revivals += 1
    if repo_updated_revivals:
        log(f"[EventRevival] Repo update dekhe {repo_updated_revivals} ti sleeping source sathe sathe active kora holo!")

    revived_by_probe = revive_sleeping_sources(sleeping_set, health_data)
    sleeping_set -= revived_by_probe

    merged_playlists.update(alt_playlists)
    merged_upstream.update(web_paste_urls)
    merged_bots.update(alt_repos)

    log(f"GitHub er {len(gh_found)} ti repo sequential ETag queue te deeply analyze kora hocche...")

    repo_trees = {}
    scanned_trees = 0
    for repo_info in gh_found.values():
        repo = repo_info["repo"]
        branch = repo_info["branch"]
        old_b = merged_bots.get(repo, {})
        if old_b.get("updated") and repo_info.get("updated") == old_b.get("updated") and (old_b.get("playlists") or old_b.get("upstream_sources")):
            repo_info["playlists"] = old_b.get("playlists", [])
            repo_info["upstream_sources"] = old_b.get("upstream_sources", [])
            repo_info["is_auto_bot"] = old_b.get("is_auto_bot", repo_info["is_auto_bot"])
            continue
        tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
        tree_data = gh.get(tree_url, min_delay=0.75, retries=0, category="core")
        if isinstance(tree_data, dict) and tree_data.get("tree"):
            repo_trees[repo] = tree_data["tree"]
        scanned_trees += 1
        if scanned_trees % 150 == 0:
            log(f"[TreeScan] {scanned_trees} ti repo tree checked...")

    def harvest_raw_files(repo_info):
        repo = repo_info["repo"]
        branch = repo_info["branch"]
        tree = repo_trees.get(repo)
        if not tree:
            return repo_info

        is_priority = repo.lower() in {r.lower() for r in BD_PRIORITY_REPOS}
        script_paths = []
        max_pl = 250 if is_priority else 90
        max_scripts = 120 if is_priority else 55

        for f in tree:
            if f.get("type") != "blob":
                continue
            path = f.get("path", "")
            lpath = path.lower()
            if ".github/workflows/" in lpath:
                repo_info["is_auto_bot"] = True
                if f.get("size", 0) < 200000:
                    script_paths.append(path)
            elif lpath.endswith((".m3u", ".m3u8")) and f.get("size", 0) > 150:
                if len(repo_info["playlists"]) < max_pl:
                    raw_pl = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(path)}"
                    repo_info["playlists"].append(raw_pl)
            elif lpath.endswith((".py", ".js", ".ts", ".sh", ".json", ".txt", ".yml", ".yaml", ".php", ".md")) and f.get("size", 0) < 450000:
                script_paths.append(path)

        upstream = set()
        self_repo_prefix = f"raw.githubusercontent.com/{repo.lower()}/"
        for sp in script_paths[:max_scripts]:
            raw_code_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(sp)}"
            code_txt = fetch_text(raw_code_url, timeout=8)
            if code_txt:
                for found_url in URL_RE.findall(code_txt):
                    clean_u = found_url.rstrip(".,;\'\")}]").split("?")[0]
                    if is_syntactically_valid_url(clean_u, self_repo_prefix):
                        upstream.add(clean_u)
        repo_info["upstream_sources"] = sorted(upstream)
        return repo_info

    with ThreadPoolExecutor(max_workers=20) as ex:
        for f in as_completed([ex.submit(harvest_raw_files, info) for info in gh_found.values()]):
            b = f.result()
            repo_key = b["repo"]
            if repo_key in merged_bots:
                old_b = merged_bots[repo_key]
                b["is_auto_bot"] = b["is_auto_bot"] or old_b.get("is_auto_bot", False)
                b["playlists"] = sorted(set(old_b.get("playlists", [])) | set(b["playlists"]))
                b["upstream_sources"] = sorted(set(old_b.get("upstream_sources", [])) | set(b["upstream_sources"]))
            merged_bots[repo_key] = b

    gh.save_cache()
    log(f"[API Stats] 304 Cache Hits: {gh.stats['304_hits']} | 200 Fresh: {gh.stats['200_hits']} | Rate Waits: {gh.stats['rate_waits']}")

    all_bots_list = list(merged_bots.values())
    all_bots_list.sort(key=lambda x: (not x.get("is_auto_bot", False), x.get("updated", "")), reverse=False)

    candidate_upstream = {u for u in merged_upstream if is_syntactically_valid_url(u)}
    for b in all_bots_list:
        candidate_upstream.update(b.get("upstream_sources", []))
        merged_playlists.update(b.get("playlists", []))

    active_upstream = set(BUILTIN_DIRECT_FEEDS)
    active_playlists = set()

    for u in candidate_upstream:
        if u in BUILTIN_DIRECT_FEEDS:
            continue
        h = health_data.get(u, {})
        if h.get("last_hits", 0) > 0 or h.get("fails", 0) < 2:
            active_upstream.add(u)
            sleeping_set.discard(u)
        else:
            sleeping_set.add(u)

    for p in merged_playlists:
        h = health_data.get(p, {})
        if h.get("last_hits", 0) > 0 or h.get("fails", 0) < 2:
            active_playlists.add(p)
            sleeping_set.discard(p)
        else:
            sleeping_set.add(p)

    HEALTH_JSON.write_text(json.dumps(health_data, indent=2), encoding="utf-8")
    SLEEPING_JSON.write_text(json.dumps({
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_sleeping": len(sleeping_set),
        "sleeping_urls": sorted(sleeping_set)
    }, indent=2), encoding="utf-8")

    bot_only_playlists = set()
    for b in all_bots_list:
        if b.get("is_auto_bot"):
            bot_only_playlists.update(b.get("playlists", []))

    all_upstream_urls = sorted(active_upstream)
    all_bot_playlists = sorted(
        active_playlists,
        key=lambda u: (0 if u in bot_only_playlists else 1, u)
    )

    result_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_bots_found": len(all_bots_list),
        "total_sleeping_sources": len(sleeping_set),
        "bots": all_bots_list,
        "all_upstream_sources": all_upstream_urls,
        "all_bot_playlists": all_bot_playlists,
    }
    OUT_JSON.write_text(json.dumps(result_data, indent=2), encoding="utf-8")

    report_lines = [
        f"=== Zero-Rate-Limit Deep IPTV Discovery Report ({result_data['generated_at']}) ===",
        f"Total Repositories Tracked (GitHub + GitLab + Codeberg): {len(all_bots_list)}",
        f"Active Upstream Scraping Sources & Pastes: {len(all_upstream_urls)}",
        f"Active Direct M3U Feeds: {len(all_bot_playlists)}",
        f"Sleeping Pool (Auto-Probed Daily for Revival): {len(sleeping_set)}\n",
        "--- 1. ACTIVE UPSTREAM WEBSITES / APIS / PASTES ---"
    ]
    for u in all_upstream_urls:
        report_lines.append(f"  - {u}")

    report_lines.append("\n--- 2. DISCOVERED REPOSITORIES & BOTS ---")
    for b in all_bots_list:
        plat = b.get("platform", "github").upper()
        tag = f"[{plat}-BOT]" if b.get("is_auto_bot") else f"[{plat}]"
        report_lines.append(f"\n{tag} {b['repo']} (Updated: {b.get('updated', 'N/A')}, Stars: {b.get('stars', 0)})")
        if b.get("upstream_sources"):
            report_lines.append("  Scrapes from:")
            for src in b["upstream_sources"]:
                report_lines.append(f"    * {src}")
        if b.get("playlists"):
            report_lines.append("  Outputs (M3U):")
            for pl in b["playlists"]:
                report_lines.append(f"    > {pl}")

    OUT_REPORT.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    log(f"Done! Active: {len(all_upstream_urls)} upstream + {len(all_bot_playlists)} M3U feeds | Sleeping: {len(sleeping_set)} sources.")


if __name__ == "__main__":
    discover()
