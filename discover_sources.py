"""
GitHub IPTV Bot & Upstream Source Discovery Script (24-Hour Rate-Limit Safe + Persistent Merge)
1. Loads previously saved bots & upstream sources from 'discovered_sources.json'.
2. Finds new active GitHub repositories running automated IPTV playlist bots.
3. Inspects their source code (.py, .js, .ts, .sh) to extract the upstream websites/APIs they scrape.
4. Merges old + new sources and saves them permanently into 'discovered_sources.json' & 'bot_report.txt'.
"""
import json, os, re, time
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
                wait_sec = max(int(e.headers.get("Retry-After", 0)), 70)
                log(f"[Rate Limit {e.code}] {wait_sec}s wait korchi...")
                time.sleep(wait_sec)
            else:
                raise
    return {}


def raw_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=12) as r:
        return r.read().decode("utf-8", errors="ignore")


def load_existing_data():
    """Ager save kora discovered_sources.json theke purono bot o source load korbe"""
    if not OUT_JSON.exists():
        return {}, set(), set()
    try:
        old = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        old_bots = {b["repo"]: b for b in old.get("bots", []) if "repo" in b}
        old_upstream = set(old.get("all_upstream_sources", []))
        old_playlists = set(old.get("all_bot_playlists", []))
        log(f"Ager save kora {len(old_bots)} ti bot o {len(old_upstream)} ti upstream source load holo.")
        return old_bots, old_upstream, old_playlists
    except Exception as e:
        log(f"[Warning] Purono JSON pora jay ni: {e}")
        return {}, set(), set()


def discover():
    if not GITHUB_TOKEN:
        log("[Warning] GITHUB_TOKEN set kora nai! Rate limit hote pare.")

    merged_bots, merged_upstream, merged_playlists = load_existing_data()

    newly_found = {}
    for q in BOT_SEARCH_QUERIES:
        try:
            url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}&sort=updated&per_page=30"
            data = api_get(url)
            for item in data.get("items", []):
                full_name = item["full_name"]
                if full_name not in newly_found:
                    newly_found[full_name] = {
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
        time.sleep(5)  # 5 sekond gap dile secondary rate limit ashbe na

    log(f"Ebar {len(newly_found)} ti active repository paoa gese. Vetorer code o source analyze kora hocche...")

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

    # max_workers=5 rakha hoyeche jate GitHub API te ek sathe beshi request giye rate limit na hoy
    with ThreadPoolExecutor(max_workers=5) as ex:
        for f in as_completed([ex.submit(analyze_repo, info) for info in newly_found.values()]):
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

    for b in all_bots_list:
        merged_upstream.update(b.get("upstream_sources", []))
        merged_playlists.update(b.get("playlists", []))

    all_upstream_urls = sorted(merged_upstream)
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
        f"=== GitHub IPTV Bots & Upstream Sources Report ({result_data['generated_at']}) ===",
        f"Total Active Repositories Saved (Merged): {len(all_bots_list)}",
        f"Total Unique Upstream Scraping Sources Saved: {len(all_upstream_urls)}",
        f"Total Bot Playlists Saved: {len(all_bot_playlists)}\n",
        "--- 1. UPSTREAM WEBSITES / APIS USED BY BOTS ---"
    ]
    for u in all_upstream_urls:
        report_lines.append(f"  - {u}")

    report_lines.append("\n--- 2. ACTIVE GITHUB BOTS & THEIR SOURCES ---")
    for b in all_bots_list:
        tag = "[AUTO-BOT]" if b.get("is_auto_bot") else "[REPO]"
        report_lines.append(f"\n{tag} {b['repo']} (Updated: {b.get('updated', 'N/A')}, Stars: {b.get('stars', 0)})")
        if b.get("upstream_sources"):
            report_lines.append("  Scrapes from:")
            for src in b["upstream_sources"]:
                report_lines.append(f"    * {src}")
        if b.get("playlists"):
            report_lines.append("  Outputs:")
            for pl in b["playlists"]:
                report_lines.append(f"    > {pl}")

    OUT_REPORT.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    log(f"Done! Total Merged: {len(all_bots_list)} ti bot, {len(all_upstream_urls)} ti upstream source o {len(all_bot_playlists)} ti playlist '{OUT_JSON}' & '{OUT_REPORT}' e save hoyeche.")


if __name__ == "__main__":
    discover()
