"""Record every new Solana DEX pool from GeckoTerminal's free `new_pools` endpoint.

Runs for RUN_MIN minutes, sweeping pages 1-10 once a minute (10 calls/min, free limit ~30), deduping
by pool, and flushing a gzipped CSV + git push every FLUSH_MIN minutes so a crash loses <= 10 minutes.
"""
import csv, gzip, json, os, subprocess, time, urllib.request, datetime as dt

G = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?include=base_token&page={}"
RUN_MIN = float(os.environ.get("RUN_MIN", 340))
FLUSH_MIN = float(os.environ.get("FLUSH_MIN", 10))
COLS = ["seen_at", "pool_created_at", "dex", "pool", "mint", "symbol", "name", "fdv_usd",
        "reserve_usd", "vol_h1_usd", "buys_h1", "sells_h1"]


def get(url):
    for k in range(4):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "meme-census"})
            return json.load(urllib.request.urlopen(req, timeout=20))
        except Exception:
            time.sleep(5 * (k + 1))
    return None


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True).returncode == 0


def flush(rows):
    if not rows:
        return
    path = dt.datetime.utcnow().strftime("census/%Y/%m/%d/%H%M%S.csv.gz")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        w.writerows(rows)
    git("add", path)
    git("commit", "-q", "-m", f"census {path[7:-7]} ({len(rows)} pools)")
    for i in range(5):
        if git("push", "-q", "origin", "HEAD:main"):
            break
        git("pull", "-q", "--rebase", "origin", "main")
        time.sleep(5 * (i + 1))
    print(len(rows), "pools ->", path, flush=True)


def main():
    seen, rows = set(), []
    t_end = time.time() + RUN_MIN * 60
    t_flush = time.time() + FLUSH_MIN * 60
    while time.time() < t_end:
        sweep = time.time()
        for pg in range(1, 11):
            d = get(G.format(pg))
            if d:
                toks = {t["id"]: t["attributes"] for t in d.get("included", [])}
                for p in d.get("data", []):
                    a, r = p["attributes"], p["relationships"]
                    if a["address"] in seen:
                        continue
                    seen.add(a["address"])
                    tid = r["base_token"]["data"]["id"]
                    tk = toks.get(tid, {})
                    tx = a.get("transactions", {}).get("h1", {})
                    rows.append([dt.datetime.utcnow().isoformat(timespec="seconds"), a["pool_created_at"],
                                 r["dex"]["data"]["id"], a["address"], tid.split("_", 1)[-1], tk.get("symbol", ""),
                                 (tk.get("name") or "")[:60], a.get("fdv_usd"), a.get("reserve_in_usd"),
                                 a.get("volume_usd", {}).get("h1"), tx.get("buys"), tx.get("sells")])
            time.sleep(2.5)
        if time.time() >= t_flush:
            flush(rows)
            rows, t_flush = [], time.time() + FLUSH_MIN * 60
        time.sleep(max(0, 60 - (time.time() - sweep)))
    flush(rows)


if __name__ == "__main__":
    main()
