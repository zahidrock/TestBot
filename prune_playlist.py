"""
Custom-First Single-Link Priority IPTV Playlist Pruner & Auto-Replacer
- Reads 'custom_channels.txt' and maintains a SINGLE best working link per channel in 'playlist_custom.m3u'
- Priority order for selecting/replacing custom channel links:
    1. AynaOTT
    2. JagoBD
    3. YuppTV / YuppCDN
    4. Others
- Checks existing custom links FIRST; if dead/slow/missing, checks candidates from 'playlist.m3u'
  strictly in priority order (AynaOTT -> JagoBD -> Yupp -> Others) and picks the first working link
- If ALL custom channels have a working link and none are missing/dead, SKIPS scanning the full 'playlist.m3u'
"""
import re, socket, ssl, sys, threading, time
from collections import Counter, defaultdict
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

socket.setdefaulttimeout(5)

PLAYLIST_FILE = Path("playlist.m3u")
CUSTOM_CHANNELS_FILE = Path("custom_channels.txt")
CUSTOM_PLAYLIST_FILE = Path("playlist_custom.m3u")

WORKERS = 180
CHECK_TIMEOUT = 3.0
MAX_MANIFEST_LATENCY = 2.2
MIN_CHUNK_KBPS = 25.0
MIN_KEEP_RATIO = 0.20

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

KEY_URI_RE = re.compile(r'#EXT-X-KEY:[^\n]*URI="([^"]+)"', re.I)
VOD_EXT_RE = re.compile(r"\.(mp4|mkv|avi|mov|wmv|flv|webm|m4v|mpg|mpeg|3gp|vob)(\?|$)", re.I)
AUDIO_EXT_RE = re.compile(r"\.(mp3|aac|ogg|oga|wav|flac|m4a|wma|opus|pls|asx)(\?|$)", re.I)

NOISE = {
    "hd", "fhd", "sd", "uhd", "4k", "live", "official", "hevc", "h265", "h264",
    "bd", "in", "ind", "india", "bangladesh", "bangladeshi", "uk", "us", "usa", "ca", "canada",
    "ch", "channel", "tv", "television", "news", "network", "digital", "plus"
}

DEAD_HOSTS = set()
DEAD_HOST_LOCK = threading.Lock()
HOST_FAIL_COUNTS = Counter()


def log(msg):
    print(msg, flush=True)


def detect_entry_priority(entry):
    extinf = (entry.get("extinf") or "").lower()
    url = (entry.get("url") or "").lower()
    src = ""
    m = re.search(r'tvg-id="([^"]*)"', extinf, re.I)
    if m:
        src = m.group(1).lower()
    if "aynaott" in src or "aynaott.com" in url:
        return 0
    if "jagobd" in src or "jagobd.com" in url:
        return 1
    if "yupp" in src or "yupp" in url:
        return 2
    return 3


def parse_existing_m3u(path):
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8", errors="ignore")
    entries = []
    cur = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line == "#EXTM3U":
            continue
        if line.startswith("#EXTINF"):
            def attr(k):
                m = re.search(rf'{k}="([^"]*)"', line, re.I)
                return m.group(1).strip() if m else ""
            name = line.rsplit(",", 1)[-1].strip() if "," in line else "Unknown"
            cur = {
                "extinf": line,
                "name": name,
                "group": attr("group-title") or "Others",
                "vlc_opts": [],
                "ua": UA,
                "ref": "",
                "url": "",
            }
        elif cur and line.startswith("#EXTVLCOPT:"):
            cur["vlc_opts"].append(line)
            opt = line.split(":", 1)[1].strip()
            if opt.lower().startswith("http-user-agent="):
                cur["ua"] = opt.split("=", 1)[1].strip()
            elif opt.lower().startswith("http-referrer="):
                cur["ref"] = opt.split("=", 1)[1].strip()
        elif cur and not line.startswith("#"):
            cur["url"] = line
            cur["priority"] = detect_entry_priority(cur)
            entries.append(cur)
            cur = None
    return entries


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


def verify_hls_segment_speed(m3u8_text, base_url, headers, timeout=3.0):
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


def verify_entry(entry):
    url = entry["url"]
    if not url:
        return False, 99.0, 0.0, "empty"

    host = urllib.parse.urlparse(url).netloc.lower()
    if host in DEAD_HOSTS:
        return False, 99.0, 0.0, "dead host"

    exp_m = re.search(r"(?:exp|expires)=(\d{10})", url, re.I)
    if exp_m and int(exp_m.group(1)) < int(time.time()) + 3600:
        return False, 99.0, 0.0, "token expired"

    headers = dict(HEADERS)
    if entry.get("ua"):
        headers["User-Agent"] = entry["ua"]
    if entry.get("ref"):
        headers["Referer"] = entry["ref"]
    elif "jagobd.com" in url:
        headers["Referer"] = "https://www.jagobd.com/"
    elif "aynaott.com" in url:
        headers["Referer"] = "https://web.aynaott.com/"

    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=CHECK_TIMEOUT, context=SSL_CTX) as r:
            final_url = r.geturl() or url
            if VOD_EXT_RE.search(final_url) or AUDIO_EXT_RE.search(final_url):
                return False, 99.0, 0.0, "non-live"
            raw = r.read(6144)
            text_body = raw.decode("utf-8", errors="ignore")
            low = text_body.strip().lower()
            lat = time.time() - t0

            if lat > MAX_MANIFEST_LATENCY:
                return False, lat, 0.0, "slow manifest"
            if r.status not in (200, 206):
                return False, lat, 0.0, f"HTTP {r.status}"
            if "#ext-x-endlist" in low or "#extm3u" not in low:
                return False, lat, 0.0, "invalid/VOD m3u8"
            if any(drm in low for drm in ("sample-aes", "widevine", "playready", "skd://")):
                return False, lat, 0.0, "DRM"

            ok_seg, kbps = verify_hls_segment_speed(text_body, final_url, headers, timeout=3.0)
            if ok_seg:
                return True, lat, kbps, "ok"
            return False, lat, 0.0, "slow/dead segment"
    except urllib.error.HTTPError as e:
        return False, 99.0, 0.0, f"HTTP {e.code}"
    except urllib.error.URLError as e:
        msg = str(e.reason).lower()
        if "name or service not known" in msg or "errno -2" in msg or "errno 111" in msg:
            with DEAD_HOST_LOCK:
                HOST_FAIL_COUNTS[host] += 1
                if HOST_FAIL_COUNTS[host] >= 2:
                    DEAD_HOSTS.add(host)
        return False, 99.0, 0.0, "connect error"
    except Exception as e:
        return False, 99.0, 0.0, type(e).__name__


def norm_key(name):
    n = name.lower().strip()
    n = re.sub(r"[\(\[【].*?[\)\]】]", " ", n)
    n = re.sub(r"\b\d{3,4}p\b", " ", n)
    n = n.replace("television", "tv")
    n = re.sub(r"[^a-z0-9\u0980-\u09ff]+", " ", n)
    tokens = [t for t in n.split() if t not in NOISE]
    return "".join(tokens) or re.sub(r"[^a-z0-9\u0980-\u09ff]+", "", name.lower())


def clone_entry(e):
    return {
        "extinf": e["extinf"],
        "name": e["name"],
        "group": e["group"],
        "vlc_opts": list(e.get("vlc_opts", [])),
        "ua": e.get("ua", UA),
        "ref": e.get("ref", ""),
        "url": e["url"],
        "priority": e.get("priority", detect_entry_priority(e)),
        "latency": e.get("latency", 99.0),
        "kbps": e.get("kbps", 0.0),
    }


def write_entries(entries, target_path):
    lines = ["#EXTM3U"]
    for e in entries:
        lines.append(e["extinf"])
        for opt in e["vlc_opts"]:
            lines.append(opt)
        lines.append(e["url"])
    target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_wanted_channels(path):
    wanted = []
    seen = set()
    if not path.exists():
        return wanted
    for raw_ln in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        ln = raw_ln.strip()
        if not ln or ln.startswith("#"):
            continue
        nk = norm_key(ln)
        if nk and nk not in seen:
            seen.add(nk)
            wanted.append((nk, ln))
    return wanted


def is_hd_entry(entry):
    txt = f"{entry.get('name', '')} {entry.get('extinf', '')} {entry.get('url', '')}".lower()
    return bool(re.search(r"(\b(?:hd|fhd|uhd|4k|1080p|720p)\b|_hd\b|-hd\b)", txt))


def pick_best_per_key(candidates):
    if not candidates:
        return None
    candidates.sort(key=lambda x: (
        x.get("priority", 3),
        0 if is_hd_entry(x) else 1,
        round(x.get("latency", 99), 2),
        -x.get("kbps", 0),
    ))
    return candidates[0]


def main():
    t_start = time.time()
    main_entries = parse_existing_m3u(PLAYLIST_FILE)
    if not main_entries:
        log("[Pruner] playlist.m3u paoa jayni ba khali.")
        return 0

    wanted_channels = load_wanted_channels(CUSTOM_CHANNELS_FILE)
    wanted_keys = {k for k, _ in wanted_channels}

    existing_custom = parse_existing_m3u(CUSTOM_PLAYLIST_FILE)
    had_existing_custom = bool(existing_custom)

    # Group existing custom entries by normalized key (keep highest priority first)
    existing_by_key = defaultdict(list)
    for e in existing_custom:
        nk = norm_key(e["name"])
        if not wanted_keys or nk in wanted_keys:
            existing_by_key[nk].append(clone_entry(e))

    # Step 1: Check existing custom links FIRST (one primary check per channel)
    first_check_list = []
    for nk, cand_list in existing_by_key.items():
        cand_list.sort(key=lambda x: x.get("priority", 3))
        first_check_list.append((nk, cand_list[0]))

    alive_by_key = {}
    dead_or_missing_keys = set()
    dead_urls = set()
    had_dead_custom_link = False

    if first_check_list:
        log(f"[Custom-First] {len(first_check_list)} ti custom channel link age check hocche...")
        with ThreadPoolExecutor(max_workers=min(WORKERS, len(first_check_list))) as ex:
            fut_map = {ex.submit(verify_entry, e): (nk, e) for nk, e in first_check_list}
            for f in as_completed(fut_map):
                nk, e = fut_map[f]
                ok, lat, kbps, reason = f.result()
                if ok:
                    e["latency"] = lat
                    e["kbps"] = kbps
                    alive_by_key[nk] = e
                else:
                    had_dead_custom_link = True
                    dead_urls.add(e["url"])
                    dead_or_missing_keys.add(nk)
                    log(f"[Custom-Dead] {e['name']} -> dead/slow ({reason}), priority order-e replacement khoja hobe.")

    # Any wanted channel from custom_channels.txt not yet alive -> needs lookup in priority order
    for nk, _raw_nm in wanted_channels:
        if nk not in alive_by_key:
            dead_or_missing_keys.add(nk)

    # Smart Skip: If custom playlist already existed, ALL checked links were alive, and NO wanted channels are missing -> do not touch custom playlist or scan full playlist!
    if had_existing_custom and not had_dead_custom_link and not dead_or_missing_keys:
        Path("missing_custom.flag").unlink(missing_ok=True)
    Path("missing_custom.txt").unlink(missing_ok=True)
        elapsed = int(time.time() - t_start)
        log(f"[Skip-Full-Scan] Sob {len(alive_by_key)} ti custom channel active ache! playlist_custom.m3u untouched rakha holo o full playlist.m3u check skip kora holo ({elapsed}s).")
        return 0

    # Step 2: Priority tier lookup (0=AynaOTT -> 1=JagoBD -> 2=Yupp -> 3=Others)
    # Stop checking lower tiers for a channel once a working link is found in a higher priority tier
    if dead_or_missing_keys:
        pool_by_key = defaultdict(list)
        for e in main_entries:
            nk = norm_key(e["name"])
            if nk in dead_or_missing_keys and e["url"] not in dead_urls:
                pool_by_key[nk].append(clone_entry(e))

        unresolved_keys = set(dead_or_missing_keys)
        tier_names = {0: "AynaOTT", 1: "JagoBD", 2: "Yupp", 3: "Others"}

        for tier in (0, 1, 2, 3):
            if not unresolved_keys:
                break
            tier_candidates = []
            for nk in list(unresolved_keys):
                tier_list = [c for c in pool_by_key.get(nk, []) if c.get("priority", 3) == tier]
                tier_list.sort(key=lambda x: (0 if is_hd_entry(x) else 1, round(x.get("latency", 99), 2), -x.get("kbps", 0)))
                for cand in tier_list[:4]:
                    tier_candidates.append((nk, cand))

            if not tier_candidates:
                continue

            log(f"[Priority-{tier_names[tier]}] {len(unresolved_keys)} ti channel-er jonno {len(tier_candidates)} ti {tier_names[tier]} link check hocche...")
            found_in_tier = defaultdict(list)
            with ThreadPoolExecutor(max_workers=min(WORKERS, len(tier_candidates))) as ex:
                fut_map = {ex.submit(verify_entry, cand): (nk, cand) for nk, cand in tier_candidates}
                for f in as_completed(fut_map):
                    nk, cand = fut_map[f]
                    ok, lat, kbps, _reason = f.result()
                    if ok:
                        cand["latency"] = lat
                        cand["kbps"] = kbps
                        found_in_tier[nk].append(cand)

            for nk, matches in found_in_tier.items():
                best = pick_best_per_key(matches)
                if best and nk not in alive_by_key:
                    alive_by_key[nk] = best
                    unresolved_keys.discard(nk)
                    log(f"[Replaced-{tier_names[tier]}] {best['name']} -> working link paoa geche ({int(best['latency']*1000)}ms | {int(best['kbps'])}KB/s)")

    order_map = {nk: idx for idx, (nk, _nm) in enumerate(wanted_channels)}
    single_custom = sorted(
        alive_by_key.values(),
        key=lambda e: order_map.get(norm_key(e["name"]), 999999)
    )
    write_entries(single_custom, CUSTOM_PLAYLIST_FILE)
    found_keys = {norm_key(e["name"]) for e in single_custom}
    still_missing = [raw_nm for nk, raw_nm in wanted_channels if nk not in found_keys]
    missing_list_file = Path("missing_custom.txt")
    Path("missing_custom.flag").unlink(missing_ok=True)
    Path("missing_custom.txt").unlink(missing_ok=True)
    if still_missing:
        missing_list_file.write_text("\n".join(still_missing) + "\n", encoding="utf-8")
        log(f"[Custom-Missing] {len(still_missing)} ti channel missing_custom.txt e save kora holo: {still_missing}")
    else:
        missing_list_file.unlink(missing_ok=True)
    log(f"[Custom-Done] {CUSTOM_PLAYLIST_FILE} e {len(single_custom)} ti single-link channel save hoyeche.")

    # Step 3: Prune dead/slow links from main playlist.m3u
    total = len(main_entries)
    log(f"[Pruner] {PLAYLIST_FILE} theke {total} ti link health-check shuru hocche...")

    alive_entries = []
    reasons = Counter()
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut_map = {ex.submit(verify_entry, e): e for e in main_entries}
        for f in as_completed(fut_map):
            e = fut_map[f]
            ok, lat, kbps, reason = f.result()
            done += 1
            if ok:
                e["latency"] = lat
                e["kbps"] = kbps
                alive_entries.append(e)
            else:
                reasons[reason] += 1
            if done % 1000 == 0 or done == total:
                log(f"[Pruner] checked {done}/{total} | alive {len(alive_entries)}")

    removed = total - len(alive_entries)
    log(f"[Pruner] Alive: {len(alive_entries)}/{total} | Dead/Slow removed: {removed}")
    if reasons:
        log("[Pruner] Bad dewar karon: " + ", ".join(f"{k}: {v}" for k, v in reasons.most_common(6)))

    if total >= 20 and len(alive_entries) < total * MIN_KEEP_RATIO:
        log(f"[SAFETY] Alive ({len(alive_entries)}) khub kom hoye geche (network issue hote pare). {PLAYLIST_FILE} overwrite kora holo na.")
        return 0

    orig_index = {id(e): idx for idx, e in enumerate(main_entries)}
    alive_entries.sort(key=lambda e: orig_index[id(e)])
    write_entries(alive_entries, PLAYLIST_FILE)

    elapsed = int(time.time() - t_start)
    log(f"[Pruner] Done in {elapsed // 60}m {elapsed % 60}s! {PLAYLIST_FILE}: {len(alive_entries)} | {CUSTOM_PLAYLIST_FILE}: {len(single_custom)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
