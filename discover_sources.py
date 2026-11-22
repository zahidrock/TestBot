"""
Global Multi-Platform Live TV & IPTV Source Discovery Engine (BD, Indian & English Focused)
Searches across:
1. GitHub (Repositories + Topics + Code/M3U Signatures for BD, Indian & English streams)
2. GitLab Public API (Projects + Public Snippets)
3. Codeberg Public API (Repositories with Adaptive Backoff & 429 Circuit Breaker)
4. GitHub Public Gists (Targeted M3U/IPTV Gist Discovery)
5. DuckDuckGo Lite Search (Pastebin, Rentry, Telegraph, Bitbucket, HuggingFace, dpaste, Open Indexes)
6. Built-in Official FAST TV & Open Directory Feeds (SamsungTVPlus, PlutoTV, Plex, Roku, Stirr, LG, Vizio, Xumo, DistroTV, Free-TV, iptv-org)
"""
import json, os, re, time
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")
OUT_JSON = Path("discovered_sources.json")
OUT_REPORT = Path("bot_report.txt")
HEALTH_JSON = Path("source_health.json")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
GH_HEADERS = {"User-Agent": UA, "Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    GH_HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN.strip()}"

# 1. GitHub Repository & Topic Queries (Worldwide BD, Indian & English IPTV)
BOT_SEARCH_QUERIES = [
    "iptv pushed:>2026-06-01",
    "m3u8 playlist pushed:>2026-06-01",
    "bdix tv",
    "bangla tv m3u8",
    "bangladesh iptv",
    "indian tv m3u8",
    "hindi tv m3u8",
    "kolkata tv m3u8",
    "english tv m3u8",
    "usa uk tv m3u8",
    "fancode m3u8",
    "sports iptv m3u8",
    "samsungtvplus m3u8",
    "plutotv m3u8",
    "topic:iptv",
    "topic:m3u8",
    "topic:m3u",
    "topic:bangla-tv",
    "topic:indian-tv",
]

# 2. Content & Code Signatures ('IPTV' naam na thakleo vetorer stream signature dekhe khujbe)
CODE_SIGNATURE_QUERIES = [
    '"cron:" "m3u" path:.github/workflows',
    '"cron:" "m3u8" path:.github/workflows',
    '"#EXTINF" "Somoy" extension:m3u',
    '"#EXTINF" "Jamuna" extension:m3u8',
    '"#EXTINF" "Ekattor" extension:m3u',
    '"#EXTINF" "Star Jalsha" extension:m3u',
    '"#EXTINF" "Zee Bangla" extension:m3u8',
    '"#EXTINF" "Aaj Tak" extension:m3u',
    '"#EXTINF" "Star Plus" extension:m3u8',
    '"#EXTINF" "BBC News" extension:m3u',
    '"#EXTINF" "aynaott" extension:py',
    '"#EXTINF" "jagobd" extension:py',
    '"gpcdn.net" extension:m3u',
    '"gpcdn.net" extension:m3u8',
    '"app24.jagobd.com.bd" extension:m3u',
    '"ncare.live" extension:m3u',
    '"tangotv.in" extension:m3u',
    '"amagi.tv" extension:m3u',
    '"mjh.nz" extension:m3u',
    '"apsattv.com" extension:m3u',
]

# 3. GitLab & Codeberg Search Keywords
ALT_GIT_KEYWORDS = [
    "iptv", "m3u8", "m3u", "bdix", "bangla-tv", "live-tv",
    "star-jalsha", "somoy-tv", "indian-tv", "hindi-tv", "sports-tv", "fast-tv"
]

# 4. Global Web Paste, Datasets & Open Index Dorks
WEB_DORK_QUERIES = [
    'site:pastebin.com "#EXTINF" "Somoy"',
    'site:pastebin.com "#EXTINF" "Star Jalsha"',
    'site:pastebin.com "#EXTINF" "Hindi"',
    'site:pastebin.com "#EXTINF" "m3u8"',
    'site:rentry.co "#EXTINF" "m3u8"',
    'site:telegra.ph "#EXTINF" "m3u8"',
    'site:dpaste.org "#EXTINF" "m3u8"',
    'site:paste.ee "#EXTINF" "m3u8"',
    'site:bitbucket.org "#EXTINF" "m3u8"',
    'site:huggingface.co/datasets "m3u8" "iptv"',
    'site:gist.github.com "#EXTINF" "Bangla"',
    'site:gist.github.com "#EXTINF" "Hindi"',
    'site:gist.github.com "#EXTINF" "English"',
    'site:gist.github.com "#EXTINF" "m3u8"',
    '"#EXTINF:-1" "gpcdn.net"',
    '"#EXTINF:-1" "app24.jagobd.com.bd"',
    'intitle:"index of" "playlist.m3u"',
    'intitle:"index of" "tv.m3u8"',
]

# 5. Built-in Official FAST TV & Global Open Directory Feeds (Always Included)
BUILTIN_DIRECT_FEEDS = [
    # Matt Huisman Official FAST TV Feeds
    "https://i.mjh.nz/SamsungTVPlus/all.m3u8",
    "https://i.mjh.nz/PlutoTV/all.m3u8",
    "https://i.mjh.nz/Plex/all.m3u8",
    "https://i.mjh.nz/Roku/all.m3u8",
    "https://i.mjh.nz/Stirr/all.m3u8",
    # ApsatTV Official Open FAST Feeds (LG, Vizio, Xumo, DistroTV, LocalNow, Tubi, Redbox)
    "https://www.apsattv.com/lg.m3u",
    "https://www.apsattv.com/vizio.m3u",
    "https://www.apsattv.com/xumo.m3u",
    "https://www.apsattv.com/distrotv.m3u",
    "https://www.apsattv.com/localnow.m3u",
    "https://www.apsattv.com/tubi.m3u",
    "https://www.apsattv.com/redbox.m3u",
    # iptv-org Official Country & Master Feeds (BD, IN & Major English Countries)
    "https://iptv-org.github.io/iptv/countries/bd.m3u",
    "https://iptv-org.github.io/iptv/countries/in.m3u",
    "https://iptv-org.github.io/iptv/countries/us.m3u",
    "https://iptv-org.github.io/iptv/countries/uk.m3u",
    "https://iptv-org.github.io/iptv/countries/ca.m3u",
    "https://iptv-org.github.io/iptv/countries/au.m3u",
    "https://iptv-org.github.io/iptv/countries/nz.m3u",
    "https://iptv-org.github.io/iptv/countries/sg.m3u",
    # Free-TV Official Curated Master Feed
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
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


def log(msg):
    print(msg, flush=True)


def load_health_data():
    if not HEALTH_JSON.exists():
        return {}
    try:
        return json.loads(HEALTH_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def is_valid_upstream_url(url, self_repo_prefix="", health_data=None):
    if url in BUILTIN_DIRECT_FEEDS:
        return True
    # Protect any source that produced working BD/Indian/English channels
    if health_data and url in health_data:
        info = health_data[url]
        if info.get("last_hits", 0) > 0:
            return True
        if info.get("fails", 0) >= 3:
            return False

    low_u = url.lower().split("?")[0].split("#")[0]
    if low_u.endswith(IGNORE_EXTENSIONS):
        return False
    if self_repo_prefix and self_repo_prefix in low_u:
        return False
    if any(d in low_u for d in IGNORE_DOMAINS):
        return False
    if any(k in low_u for k in (
        "api", "live", "tv", "stream", "m3u", "cdn", "103.", "portal", "play",
        "iptv", "mjh.nz", "apsattv.com", "gist.githubusercontent", "pastebin.com/raw",
        "rentry.co", "telegra.ph", "dpaste.org", "paste.ee", "huggingface.co", "bitbucket.org"
    )):
        return True
    return False


def fetch_json(url, headers=None, retries=2):
    hdrs = headers or {"User-Agent": UA, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 403) and attempt < retries:
                wait_sec = max(int(e.headers.get("Retry-After", 0)), 65)
                log(f"[Rate Limit {e.code}] {wait_sec}s wait korchi...")
                time.sleep(wait_sec)
            else:
                raise
    return {}


def fetch_text(url, timeout=12, post_data=None):
    hdrs = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if post_data is not None:
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
        hdrs["Referer"] = "https://lite.duckduckgo.com/"
    req = urllib.request.Request(url, data=post_data, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(1_000_000).decode("utf-8", errors="ignore")


def load_existing_data(health_data):
    if not OUT_JSON.exists():
        return {}, set(BUILTIN_DIRECT_FEEDS), set()
    try:
        old = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        old_bots = {}
        for b in old.get("bots", []):
            if "repo" in b:
                prefix = f"raw.githubusercontent.com/{b['repo'].lower()}/"
                b["upstream_sources"] = [
                    u for u in b.get("upstream_sources", [])
                    if is_valid_upstream_url(u, prefix, health_data)
                ]
                old_bots[b["repo"]] = b
        old_upstream = {
            u for u in old.get("all_upstream_sources", [])
            if is_valid_upstream_url(u, health_data=health_data)
        }
        old_upstream.update(BUILTIN_DIRECT_FEEDS)
        old_playlists = {
            p for p in old.get("all_bot_playlists", [])
            if health_data.get(p, {}).get("fails", 0) < 3 or health_data.get(p, {}).get("last_hits", 0) > 0
        }
        log(f"Ager {len(old_bots)} ti repo o {len(old_upstream)} ti upstream source load holo.")
        return old_bots, old_upstream, old_playlists
    except Exception as e:
        log(f"[Warning] Purono JSON pora jay ni: {e}")
        return {}, set(BUILTIN_DIRECT_FEEDS), set()


# ------------------------------------------------------------------ Engine 1: GitHub
def search_github():
    found = {}
    for q in BOT_SEARCH_QUERIES:
        try:
            url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}&sort=updated&per_page=30"
            data = fetch_json(url, GH_HEADERS)
            for item in data.get("items", []):
                fn = item["full_name"]
                if fn not in found:
                    found[fn] = {
                        "repo": fn,
                        "platform": "github",
                        "branch": item.get("default_branch", "main"),
                        "updated": item.get("pushed_at", "")[:10],
                        "stars": item.get("stargazers_count", 0),
                        "is_auto_bot": False,
                        "playlists": [],
                        "upstream_sources": [],
                    }
        except Exception as e:
            log(f"[GitHub Repo Error] {q}: {e}")
        time.sleep(3)

    if GITHUB_TOKEN:
        for cq in CODE_SIGNATURE_QUERIES:
            try:
                c_url = f"https://api.github.com/search/code?q={urllib.parse.quote(cq)}&per_page=30"
                c_data = fetch_json(c_url, GH_HEADERS)
                for item in c_data.get("items", []):
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
            except Exception as e:
                log(f"[GitHub Code Error] {cq}: {e}")
            time.sleep(6)
    return found


# ------------------------------------------------------------------ Engine 2: GitLab (Projects + Snippets) & Codeberg
def search_gitlab_and_codeberg():
    extra_playlists = set()
    extra_repos = {}

    for kw in ALT_GIT_KEYWORDS:
        try:
            gl_url = f"https://gitlab.com/api/v4/projects?search={urllib.parse.quote(kw)}&order_by=last_activity_at&per_page=25"
            projects = fetch_json(gl_url)
            if isinstance(projects, list):
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
                        for fname in ("playlist.m3u", "playlist.m3u8", "index.m3u", "index.m3u8", "iptv.m3u", "tv.m3u", "bangla.m3u", "india.m3u", "live.m3u"):
                            extra_playlists.add(f"https://gitlab.com/{path_ns}/-/raw/{branch}/{fname}")
        except Exception as e:
            log(f"[GitLab Search Error] {kw}: {e}")
        time.sleep(2)

    # GitLab Public Snippets
    try:
        snippets = fetch_json("https://gitlab.com/api/v4/snippets/public?per_page=100", retries=0)
        if isinstance(snippets, list):
            for sn in snippets:
                title = (sn.get("title") or "").lower()
                fname = (sn.get("file_name") or "").lower()
                raw_u = sn.get("raw_url")
                if raw_u and (fname.endswith((".m3u", ".m3u8")) or any(k in f"{title} {fname}" for k in ("iptv", "m3u", "bangla", "hindi", "bdix", "tv"))):
                    extra_playlists.add(raw_u)
    except Exception as e:
        log(f"[GitLab Snippets Error] {e}")

    # Codeberg with Adaptive Backoff & Graceful Skip (avoids noisy 429 errors on shared CI IPs)
    cb_headers = {
        "User-Agent": "IPTV-Playlist-Aggregator/1.0 (+https://github.com)",
        "Accept": "application/json"
    }
    cb_blocked = False
    for idx, kw in enumerate(ALT_GIT_KEYWORDS[:4]):
        if cb_blocked:
            break
        time.sleep(15 if idx == 0 else 20)
        cb_url = f"https://codeberg.org/api/v1/repos/search?q={urllib.parse.quote(kw)}&sort=updated&limit=25"
        for attempt in range(2):
            try:
                res = fetch_json(cb_url, headers=cb_headers, retries=0)
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
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    if attempt == 0:
                        log(f"[Codeberg] 429 cooldown (45s) for '{kw}'...")
                        time.sleep(45)
                    else:
                        log("[Codeberg] Shared IP rate-limited by Codeberg; skipping Codeberg for this run.")
                        cb_blocked = True
                        break
                else:
                    log(f"[Codeberg Info] {kw}: HTTP {e.code}")
                    break
            except Exception as e:
                log(f"[Codeberg Info] {kw}: {e}")
                break

    log(f"[GitLab & Codeberg] {len(extra_repos)} ti external repo o {len(extra_playlists)} ti candidate feed paoa gese.")
    return extra_repos, extra_playlists


# ------------------------------------------------------------------ Engine 3: Public Gists, Pastes, Datasets & Open Web
def normalize_web_discovered_url(decoded):
    decoded = decoded.strip().rstrip(".,;'\")}]")
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


def search_web_pastes_and_gists():
    discovered_urls = set()

    # 1. Public GitHub Gists (multiple pages for better hit rate)
    for page in (1, 2):
        try:
            gists = fetch_json(f"https://api.github.com/gists/public?per_page=100&page={page}", GH_HEADERS, retries=0)
            if isinstance(gists, list):
                for g in gists:
                    desc = (g.get("description") or "").lower()
                    files = g.get("files") or {}
                    for fname, fmeta in files.items():
                        low_f = fname.lower()
                        if low_f.endswith((".m3u", ".m3u8")) or any(k in f"{desc} {low_f}" for k in ("iptv", "m3u", "bangla", "hindi", "bdix", "live tv")):
                            raw_u = fmeta.get("raw_url")
                            if raw_u:
                                discovered_urls.add(raw_u)
        except Exception as e:
            log(f"[Gist Feed Info] page {page}: {e}")
        time.sleep(1)

    # 2. DuckDuckGo Lite Search (POST requests avoid HTML anti-bot empty responses)
    for dq in WEB_DORK_QUERIES:
        try:
            post_bytes = urllib.parse.urlencode({"q": dq}).encode("utf-8")
            html = fetch_text("https://lite.duckduckgo.com/lite/", timeout=15, post_data=post_bytes)
            raw_links = re.findall(r'uddg=([^&"\']+)', html)
            if not raw_links:
                raw_links = re.findall(r'class=["\']result-link["\'][^>]*href=["\']([^"\']+)["\']', html, re.I)
            for raw_link in raw_links:
                decoded = urllib.parse.unquote(raw_link)
                norm_u = normalize_web_discovered_url(decoded)
                if norm_u:
                    discovered_urls.add(norm_u)
        except Exception as e:
            log(f"[Web Dork Info] {dq}: {e}")
        time.sleep(4)

    log(f"[Web Pastes, Datasets & Gists] {len(discovered_urls)} ti global URL paoa gese.")
    return discovered_urls


def discover():
    if not GITHUB_TOKEN:
        log("[Warning] GITHUB_TOKEN set kora nai! Rate limit hote pare.")

    health_data = load_health_data()
    merged_bots, merged_upstream, merged_playlists = load_existing_data(health_data)

    gh_found = search_github()
    alt_repos, alt_playlists = search_gitlab_and_codeberg()
    web_paste_urls = search_web_pastes_and_gists()

    merged_playlists.update(alt_playlists)
    merged_upstream.update(web_paste_urls)
    merged_bots.update(alt_repos)

    log(f"GitHub er {len(gh_found)} ti repo analyze kora hocche...")

    def analyze_github_repo(repo_info):
        repo = repo_info["repo"]
        branch = repo_info["branch"]
        try:
            tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
            tree = fetch_json(tree_url, GH_HEADERS).get("tree", [])
        except Exception:
            return repo_info

        script_paths = []
        for f in tree:
            if f.get("type") != "blob":
                continue
            path = f.get("path", "")
            lpath = path.lower()
            if ".github/workflows/" in lpath:
                repo_info["is_auto_bot"] = True
                if f.get("size", 0) < 100000:
                    script_paths.append(path)
            elif lpath.endswith((".m3u", ".m3u8")) and f.get("size", 0) > 400:
                if len(repo_info["playlists"]) < 12:
                    raw_pl = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(path)}"
                    repo_info["playlists"].append(raw_pl)
            elif lpath.endswith((".py", ".js", ".ts", ".sh")) and f.get("size", 0) < 150000:
                script_paths.append(path)

        upstream = set()
        self_repo_prefix = f"raw.githubusercontent.com/{repo.lower()}/"
        for sp in script_paths[:12]:
            raw_code_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(sp)}"
            try:
                code_txt = fetch_text(raw_code_url, timeout=10)
                for found_url in URL_RE.findall(code_txt):
                    clean_u = found_url.rstrip(".,;'\")}]").split("?")[0]
                    if is_valid_upstream_url(clean_u, self_repo_prefix, health_data):
                        upstream.add(clean_u)
            except Exception:
                pass
        repo_info["upstream_sources"] = sorted(upstream)
        return repo_info

    with ThreadPoolExecutor(max_workers=5) as ex:
        for f in as_completed([ex.submit(analyze_github_repo, info) for info in gh_found.values()]):
            b = f.result()
            repo_key = b["repo"]
            if repo_key in merged_bots:
                old_b = merged_bots[repo_key]
                b["is_auto_bot"] = b["is_auto_bot"] or old_b.get("is_auto_bot", False)
                b["playlists"] = sorted(set(old_b.get("playlists", [])) | set(b["playlists"]))
                b["upstream_sources"] = sorted(set(old_b.get("upstream_sources", [])) | set(b["upstream_sources"]))
            merged_bots[repo_key] = b

    all_bots_list = list(merged_bots.values())
    all_bots_list.sort(key=lambda x: (not x.get("is_auto_bot", False), x.get("updated", "")), reverse=False)

    clean_merged_upstream = {u for u in merged_upstream if is_valid_upstream_url(u, health_data=health_data)}
    for b in all_bots_list:
        clean_merged_upstream.update(b.get("upstream_sources", []))
        merged_playlists.update(b.get("playlists", []))

    all_upstream_urls = sorted(clean_merged_upstream)
    all_bot_playlists = sorted(merged_playlists)

    result_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_bots_found": len(all_bots_list),
        "bots": all_bots_list,
        "all_upstream_sources": all_upstream_urls,
        "all_bot_playlists": all_bot_playlists,
    }
    OUT_JSON.write_text(json.dumps(result_data, indent=2), encoding="utf-8")

    report_lines = [
        f"=== Global Multi-Source IPTV Discovery Report ({result_data['generated_at']}) ===",
        f"Total Repositories Saved (GitHub + GitLab + Codeberg): {len(all_bots_list)}",
        f"Total Unique Upstream Scraping Sources & Pastes Saved: {len(all_upstream_urls)}",
        f"Total Direct M3U Feeds Saved: {len(all_bot_playlists)}\n",
        "--- 1. UPSTREAM WEBSITES / APIS / PASTES ---"
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
    log(f"Done! Total: {len(all_bots_list)} repos, {len(all_upstream_urls)} upstream/paste sources, {len(all_bot_playlists)} M3U feeds saved.")


if __name__ == "__main__":
    discover()
