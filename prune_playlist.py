"""
Lightweight Fast IPTV Playlist Pruner (Dead Link Remover)
- Reads existing 'playlist.m3u' directly without scraping new sources or calling GitHub APIs
- Verifies live HLS stream liveness, manifest latency (<= 2.2s), and segment throughput (>= 25 KB/s)
- Removes dead/offline/slow entries from 'playlist.m3u' and regenerates 'playlist_clean.m3u'
- Includes safety ratio guard so temporary network glitches never wipe out the playlist
"""
import re, socket, ssl, sys, threading, time
from collections import Counter
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


def write_entries(entries, target_path):
    lines = ["#EXTM3U"]
    for e in entries:
        lines.append(e["extinf"])
        for opt in e["vlc_opts"]:
            lines.append(opt)
        lines.append(e["url"])
    target_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    t_start = time.time()
    entries = parse_existing_m3u(PLAYLIST_FILE)
    if not entries:
        log("[Pruner] playlist.m3u paoa jayni ba khali.")
        return 0

    total = len(entries)
    log(f"[Pruner] {PLAYLIST_FILE} theke {total} ti link fast health-check shuru hocche...")

    alive_entries = []
    reasons = Counter()
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut_map = {ex.submit(verify_entry, e): e for e in entries}
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
        log(f"[SAFETY] Alive ({len(alive_entries)}) khub kom hoye geche (network issue hote pare). Overwrite kora holo na.")
        return 0

    # Keep original group order while sorting fastest stream first per channel
    orig_index = {id(e): idx for idx, e in enumerate(entries)}
    alive_entries.sort(key=lambda e: (orig_index[id(e)]))
    write_entries(alive_entries, PLAYLIST_FILE)

    # Rebuild playlist_custom.m3u matching desired channel names from custom_channels.txt
    wanted_keys = set()
    if CUSTOM_CHANNELS_FILE.exists():
        for raw_ln in CUSTOM_CHANNELS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            ln = raw_ln.strip()
            if ln and not ln.startswith("#"):
                nk = norm_key(ln)
                if nk:
                    wanted_keys.add(nk)

    custom_entries = [e for e in alive_entries if norm_key(e["name"]) in wanted_keys]
    custom_entries.sort(key=lambda e: (
        round(e.get("latency", 99), 2),
        -e.get("kbps", 0),
        orig_index[id(e)],
    ))
    write_entries(custom_entries, CUSTOM_PLAYLIST_FILE)

    elapsed = int(time.time() - t_start)
    log(f"[Pruner] Done in {elapsed // 60}m {elapsed % 60}s! {PLAYLIST_FILE}: {len(alive_entries)} | {CUSTOM_PLAYLIST_FILE}: {len(custom_entries)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
