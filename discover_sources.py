"""
GitHub IPTV Bot & Upstream Source Discovery Script
1. Finds all active GitHub repositories running automated IPTV playlist bots.
2. Inspects their source code (.py, .js, .sh, .yml) to extract the upstream websites/APIs they scrape.
3. Saves the complete structured list into 'discovered_sources.json' and 'bot_report.txt' for auto_update.py.
"""
import json, os, re, sys, time
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

GITHUB_TOKEN = "" or os.getenv("GITHUB_TOKEN")
OUT_JSON = Path("discovered_sources.json")
OUT_REPORT = Path("bot_report.txt")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/vnd.github+json",
}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN.strip()}"

# Active bot repositories khujar search queries
BOT_SEARCH_QUERIES = [
    "bdix iptv pushed:>2026-09-01",
    "bangla iptv m3u pushed:>2026-09-01",
    "iptv auto update m3u pushed:>2026-09-01",
    "live tv m3u8 bangladesh pushed:>2026-09-01",
    "indian iptv m3u8 pushed:>2026-09-01",
    "iptv scraper m3u8 pushed:>2026-09-01",
]

URL_RE = re.compile(r'https?://[a-zA-Z0-9.-]+(?:/[a-zA-Z0-9._~:/?#\[\]@!$&\'()*+,;=-]*)?')
IGNORE_DOMAINS = (
    "github.com", "githubusercontent.com", "shields.io", "google.com",
    "w3.org", "schemas.xmlsoap.org", "json-schema.org", "opensource.org",
    "creativecommons.org", "example.com", "localhost", "127.0.0.1"
)


def log(msg):
    print(msg, flush=True)


def api_get(url, max_retries=2):
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 403) and attempt < max_retries:
                wait_sec = max(int(e.headers.get("Retry-After", 0)), 65)
                log(f"[Rate Limit {e.code}] {wait_sec}s wait korchi...")
                time.sleep(wait_sec)
            else:
                raise
    return {}


def raw_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=12) as r:
        return r.read().decode("utf-8", errors="ignore")


def discover():
    if not GITHUB_TOKEN:
        log("[Warning] GITHUB_TOKEN set kora nai! Rate limit hote pare.")

    # 1. Active GitHub Bot Repository Khuja
    active_bots = {}
    for q in BOT_SEARCH_QUERIES:
        try:
            url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}&sort=updated&per_page=30"
            data = api_get(url)
            for item in data.get("items", []):
                full_name = item["full_name"]
                if full_name not in active_bots:
                    active_bots[full_name] = {
                        "repo": full_name,
                        "branch": item.get("default_branch", "main"),
                        "updated": item.get("pushed_at", "")[:10],
                        "stars": item.get("stargazers_count", 0),
                        "is_auto_bot": False,
                        "playlists": [],
                        "upstream_sources": [],
                    }
        except Exception as e:
            log(f"[Search Error] {q}: {e}")
        time.sleep(3)

    log(f"Mot {len(active_bots)} ti active repository paoa gese. Vetorer code o source analyze kora hocche...")

    # 2. Protiti Bot Repo-r file tree check kore script o playlist ber kora
    def analyze_repo(repo_info):
        repo = repo_info["repo"]
        branch = repo_info["branch"]
        try:
            tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
            tree = api_get(tree_url).get("tree", [])
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
            elif lpath.endswith((".m3u", ".m3u8")) and f.get("size", 0) > 400:
                if len(repo_info["playlists"]) < 5:
                    raw_pl = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(path)}"
                    repo_info["playlists"].append(raw_pl)
            elif lpath.endswith((".py", ".js", ".ts", ".sh")) and f.get("size", 0) < 150000:
                script_paths.append(path)

        # Script gulo pore dekha tara kon website/API theke scrape korche
        upstream = set()
        for sp in script_paths[:5]:
            raw_code_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{urllib.parse.quote(sp)}"
            try:
                code_txt = raw_get(raw_code_url)
                for found_url in URL_RE.findall(code_txt):
                    clean_u = found_url.rstrip(".,;'\")}]")
                    if any(d in clean_u.lower() for d in IGNORE_DOMAINS):
                        continue
                    if any(k in clean_u.lower() for k in ("api", "live", "tv", "stream", "m3u", "cdn", "103.", "portal", "play")):
                        upstream.add(clean_u.split("?")[0])
            except Exception:
                pass
        repo_info["upstream_sources"] = sorted(upstream)
        return repo_info

    analyzed = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for f in as_completed([ex.submit(analyze_repo, info) for info in active_bots.values()]):
            analyzed.append(f.result())

    # Prioritize auto-bots first
    analyzed.sort(key=lambda x: (not x["is_auto_bot"], x["updated"]), reverse=False)

    all_upstream_urls = sorted({u for b in analyzed for u in b["upstream_sources"]})
    all_bot_playlists = sorted({p for b in analyzed for p in b["playlists"]})

    result_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_bots_found": len(analyzed),
        "bots": analyzed,
        "all_upstream_sources": all_upstream_urls,
        "all_bot_playlists": all_bot_playlists,
    }
    OUT_JSON.write_text(json.dumps(result_data, indent=2), encoding="utf-8")

    # Human-readable list report (bot_report.txt)
    report_lines = [
        f"=== GitHub IPTV Bots & Upstream Sources Report ({result_data['generated_at']}) ===",
        f"Total Active Repositories Scanned: {len(analyzed)}",
        f"Total Unique Upstream Scraping Sources Found: {len(all_upstream_urls)}",
        f"Total Bot Playlists Found: {len(all_bot_playlists)}\n",
        "--- 1. UPSTREAM WEBSITES / APIS USED BY BOTS ---"
    ]
    for u in all_upstream_urls:
        report_lines.append(f"  - {u}")

    report_lines.append("\n--- 2. ACTIVE GITHUB BOTS & THEIR SOURCES ---")
    for b in analyzed:
        tag = "[AUTO-BOT]" if b["is_auto_bot"] else "[REPO]"
        report_lines.append(f"\n{tag} {b['repo']} (Updated: {b['updated']}, Stars: {b['stars']})")
        if b["upstream_sources"]:
            report_lines.append("  Scrapes from:")
            for src in b["upstream_sources"]:
                report_lines.append(f"    * {src}")
        if b["playlists"]:
            report_lines.append("  Outputs:")
            for pl in b["playlists"]:
                report_lines.append(f"    > {pl}")

    OUT_REPORT.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    log(f"Done! {len(analyzed)} ti bot, {len(all_upstream_urls)} ti upstream source o {len(all_bot_playlists)} ti playlist '{OUT_JSON}' & '{OUT_REPORT}' e save hoyeche.")


if __name__ == "__main__":
    discover()
