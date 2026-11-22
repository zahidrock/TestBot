"""
Multi-Platform Internet-Wide Live TV & IPTV Source Discovery Engine
Searches across:
1. GitHub (Repositories + Code/M3U Signatures even without 'IPTV' in repo name)
2. GitLab Public API (Projects matching IPTV, M3U8, BDIX, Bangla, Live TV signatures)
3. Codeberg Public API (Repositories)
4. GitHub Public Gists (Live stream & M3U gists)
5. DuckDuckGo Web Search (Pastebin, Rentry, Telegraph, Open Web M3U8 indexes)
6. Built-in Official FAST TV & Open Directory Feeds (SamsungTVPlus, PlutoTV, Plex, Roku, Free-TV, iptv-org)
"""
import json, os, re, time
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")
OUT_JSON = Path("discovered_sources.json")
OUT_REPORT = Path("bot_report.txt")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36"
GH_HEADERS = {"User-Agent": UA, "Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    GH_HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN.strip()}"

# 1. GitHub Repository Name Queries
BOT_SEARCH_QUERIES = [
    "iptv pushed:>2026-09-01",
    "bdix pushed:>2026-09-01",
    "bangla tv m3u8 pushed:>2026-09-01",
    "live tv m3u pushed:>2026-09-01",
    "indian tv m3u8 pushed:>2026-09-01",
    "fancode m3u8 pushed:>2026-09-01",
    "samsungtvplus m3u8 pushed:>2026-09-01",
    "plutotv m3u8 pushed:>2026-09-01",
]

# 2. Content & Code Signatures ('IPTV' naam na thakleo vetorer stream signature dekhe khujbe)
CODE_SIGNATURE_QUERIES = [
    '"cron:" "m3u" path:.github/workflows',
    '"cron:" "m3u8" path:.github/workflows',
    '"#EXTINF" "Somoy" extension:m3u',
    '"#EXTINF" "Jamuna" extension:m3u8',
    '"#EXTINF" "Star Jalsha" extension:m3u',
    '"#EXTINF" "Zee Bangla" extension:m3u8',
    '"#EXTINF" "Aaj Tak" extension:m3u',
    '"#EXTINF" "aynaott" extension:py',
    '"#EXTINF" "jagobd" extension:py',
    '"gpcdn.net" extension:m3u',
    '"gpcdn.net" extension:m3u8',
    '"app24.jagobd.com.bd" extension:m3u',
    '"ncare.live" extension:m3u',
    '"tangotv.in" extension:m3u',
    '"amagi.tv" extension:m3u',
]

# 3. GitLab & Codeberg Search Keywords (IPTV charao onno naame thaka list khujbe)
ALT_GIT_KEYWORDS = [
    "iptv", "m3u8", "m3u", "bdix", "bangla-tv", "live-tv", "star-jalsha", "somoy-tv", "sports-tv", "fast-tv"
]

# 4. Web Paste & Open Index Dorks (Pastebin, Rentry, Telegraph, Gist, Open Web)
WEB_DORK_QUERIES = [
    'site:pastebin.com "#EXTINF" "Somoy"',
    'site:pastebin.com "#EXTINF" "Star Jalsha"',
    'site:pastebin.com "#EXTINF" "m3u8"',
    'site:rentry.co "#EXTINF" "m3u8"',
    'site:telegra.ph "#EXTINF" "m3u8"',
    'site:gist.github.com "#EXTINF" "Bangla"',
    'site:gist.github.com "#EXTINF" "Hindi"',
    'site:gist.github.com "#EXTINF" "m3u8"',
    '"#EXTINF:-1" "gpcdn.net"',
    '"#EXTINF:-1" "app24.jagobd.com.bd"',
]

# 5. Built-in Official FAST TV & Open Directory Feeds (Always Included)
BUILTIN_DIRECT_FEEDS = [
    "https://i.mjh.nz/SamsungTVPlus/all.m3u8",
    "https://i.mjh.nz/PlutoTV/all.m3u8",
    "https://i.mjh.nz/Plex/all.m3u8",
    "https://i.mjh.nz/Roku/all.m3u8",
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
]

URL_RE = re.compile(r'https?://[a-zA-Z0-9.-]+(?:/[a-zA-Z0-9._~:/?#\[\]@!$&\'()*+,;=-]*)?')

IGNORE_DOMAINS = (
    "github.com", "gitlab.com", "codeberg.org", "duckduckgo.com",
    "shields.io", "google.com", "w3.org", "schemas.xmlsoap.org",
    "json-schema.org", "opensource.org", "creativecommons.org",
    "example.com", "localhost", "127.0.0.1", "api.github.com"
)

IGNORE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tiff",
    ".css", ".scss", ".less", ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mkv", ".avi", ".mp3", ".wav", ".ogg",
    ".zip", ".tar", ".gz", ".rar", ".7z", ".pdf", ".md", ".exe", ".apk"
)


def log(msg):
    print(msg, flush=True)


def is_valid_upstream_url(url, self_repo_prefix=""):
    low_u = url.lower().split("?")[0].split("#")[0]
    if low_u.endswith(IGNORE_EXTENSIONS):
        return False
    if self_repo_prefix and self_repo_prefix in low_u:
        return False
    if any(d in low_u for d in IGNORE_DOMAINS):
        return False
    if any(k in low_u for k in (
        "api", "live", "tv", "stream", "m3u", "cdn", "103.", "portal", "play",
        "iptv", "mjh.nz", "gist.githubusercontent", "pastebin.com/raw", "rentry.co", "telegra.ph"
    )):
        return True
    return False


def fetch_json(url, headers=None, retries=2):
    hdrs = headers or {"User-Agent": UA}
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


def fetch_text(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(1_000_000).decode("utf-8", errors="ignore")


def load_existing_data():
    if not OUT_JSON.exists():
        return {}, set(BUILTIN_DIRECT_FEEDS), set()
    try:
        old = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        old_bots = {}
        for b in old.get("bots", []):
            if "repo" in b:
                prefix = f"raw.githubusercontent.com/{b['repo'].lower()}/"
                b["upstream_sources"] = [u for u in b.get("upstream_sources", []) if is_valid_upstream_url(u, prefix)]
                old_bots[b["repo"]] = b
        old_upstream = {u for u in old.get("all_upstream_sources", []) if is_valid_upstream_url(u)}
        old_upstream.update(BUILTIN_DIRECT_FEEDS)
        old_playlists = set(old.get("all_bot_playlists", []))
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
        time.sleep(4)

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
            time.sleep(7)
    return found


# ------------------------------------------------------------------ Engine 2: GitLab & Codeberg
def search_gitlab_and_codeberg():
    extra_playlists = set()
    extra_repos = {}

    for kw in ALT_GIT_KEYWORDS:
        try:
            gl_url = f"https://gitlab.com/api/v4/projects?search={urllib.parse.quote(kw)}&order_by=last_activity_at&per_page=20"
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
                        for fname in ("playlist.m3u", "playlist.m3u8", "index.m3u", "index.m3u8", "iptv.m3u", "tv.m3u", "bangla.m3u", "live.m3u"):
                            extra_playlists.add(f"https://gitlab.com/{path_ns}/-/raw/{branch}/{fname}")
        except Exception as e:
            log(f"[GitLab Search Error] {kw}: {e}")
        time.sleep(2)

    for kw in ALT_GIT_KEYWORDS[:6]:
        try:
            cb_url = f"https://codeberg.org/api/v1/repos/search?q={urllib.parse.quote(kw)}&sort=updated&limit=20"
            res = fetch_json(cb_url)
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
        except Exception as e:
            log(f"[Codeberg Search Error] {kw}: {e}")
        time.sleep(2)

    log(f"[GitLab & Codeberg] {len(extra_repos)} ti external repo o {len(extra_playlists)} ti candidate feed paoa gese.")
    return extra_repos, extra_playlists


# ------------------------------------------------------------------ Engine 3: Public Gists & Web Pastes
def search_web_pastes_and_gists():
    discovered_urls = set()

    try:
        gists = fetch_json("https://api.github.com/gists/public?per_page=100", GH_HEADERS)
        if isinstance(gists, list):
            for g in gists:
                desc = (g.get("description") or "").lower()
                files = g.get("files") or {}
                for fname, fmeta in files.items():
                    low_f = fname.lower()
                    if low_f.endswith((".m3u", ".m3u8")) or any(k in f"{desc} {low_f}" for k in ("iptv", "m3u", "bangla", "bdix", "live tv")):
                        raw_u = fmeta.get("raw_url")
                        if raw_u:
                            discovered_urls.add(raw_u)
    except Exception as e:
        log(f"[Gist Feed Error] {e}")

    for dq in WEB_DORK_QUERIES:
        try:
            ddg_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(dq)}"
            html = fetch_text(ddg_url, timeout=12)
            for raw_link in re.findall(r'uddg=([^&"\']+)', html):
                decoded = urllib.parse.unquote(raw_link)
                if "pastebin.com/" in decoded and "/raw/" not in decoded:
                    paste_id = decoded.rstrip("/").split("/")[-1]
                    if len(paste_id) >= 6:
                        discovered_urls.add(f"https://pastebin.com/raw/{paste_id}")
                elif "rentry.co/" in decoded:
                    slug = decoded.rstrip("/").split("/")[-1]
                    if slug:
                        discovered_urls.add(f"https://rentry.co/{slug}/raw")
                elif "telegra.ph/" in decoded or "gist.githubusercontent.com/" in decoded:
                    discovered_urls.add(decoded)
        except Exception as e:
            log(f"[Web Dork Error] {dq}: {e}")
        time.sleep(3)

    log(f"[Web Pastes & Gists] {len(discovered_urls)} ti non-GitHub paste/gist URL paoa gese.")
    return discovered_urls


def discover():
    if not GITHUB_TOKEN:
        log("[Warning] GITHUB_TOKEN set kora nai! Rate limit hote pare.")

    merged_bots, merged_upstream, merged_playlists = load_existing_data()

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
                if len(repo_info["playlists"]) < 8:
                    raw_pl = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(path)}"
                    repo_info["playlists"].append(raw_pl)
            elif lpath.endswith((".py", ".js", ".ts", ".sh", ".json", ".txt")) and f.get("size", 0) < 200000:
                if lpath != "discovered_sources.json":
                    script_paths.append(path)

        upstream = set()
        self_repo_prefix = f"raw.githubusercontent.com/{repo.lower()}/"
        for sp in script_paths[:12]:
            raw_code_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(sp)}"
            try:
                code_txt = fetch_text(raw_code_url, timeout=10)
                for found_url in URL_RE.findall(code_txt):
                    clean_u = found_url.rstrip(".,;'\")}]").split("?")[0]
                    if is_valid_upstream_url(clean_u, self_repo_prefix):
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

    clean_merged_upstream = {u for u in merged_upstream if is_valid_upstream_url(u)}
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
        f"=== Multi-Source Internet IPTV Discovery Report ({result_data['generated_at']}) ===",
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
