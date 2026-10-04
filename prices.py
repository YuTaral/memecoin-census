"""Pull hourly OHLCV for census pools from GeckoTerminal (free API keeps only 180 days of history).

Targets (Round 23 protocol): every PumpSwap pool, every other non-pump-fun pool whose mint launched on pump.fun
(graduations), and a seeded 5 % sample (seed 23) of pump-fun bonding-curve pools.

Each pool is fetched once per age milestone (MILESTONES days after creation). The call is anchored at
before_timestamp = created + milestone, so a late fetch returns the identical window; lateness only matters
against the 180-day history limit, which is why the oldest pools go first. One call = 1000 hourly candles
(41.7 days), so milestones 41/82/123/164/180 cover the first 180 days. A pool with no candle in the last
DEAD_DAYS of a fetched window is marked dead and not fetched again.

Storage: candles -> parquet asset on GitHub release `ohlcv-YYYY-MM` (keeps git small); progress log ->
ohlcv_log/YYYY/MM/DD/HHMMSS.csv.gz in git, written only after its asset uploaded, so the log never points at
missing data.
"""
import csv, glob, gzip, hashlib, io, json, os, subprocess, sys, time, urllib.error, urllib.request
import datetime as dt
import pandas as pd

MILESTONES = [41, 82, 123, 164, 180]
TEST = os.environ.get("PRICES_TEST") == "1"   # live smoke test: 30-min milestone, separate release/log, 20 pools
if TEST:
    MILESTONES = [0.02]
PFX = "ohlcv-test" if TEST else "ohlcv"
LOGDIR = "ohlcv_log_test" if TEST else "ohlcv_log"
DEAD_DAYS = 30
HISTORY_DAYS = 179          # API keeps 180 days; 1 day margin
WINDOW_H = 1000
SAMPLE_PCT, SEED = 5, 23
DONE = {"ok", "empty", "missing", "expired"}
LOG_COLS = ["pool", "milestone", "fetched_at", "status", "n", "first_ts", "last_ts", "asset"]
OHLCV = ("https://api.geckoterminal.com/api/v2/networks/solana/pools/{}/ohlcv/hour"
         "?aggregate=1&limit=1000&before_timestamp={}&currency=usd&token=base")


# ---------------------------------------------------------------- planning (pure, unit-tested)
def in_sample(pool):
    return int(hashlib.sha1(f"{SEED}:{pool}".encode()).hexdigest(), 16) % 100 < SAMPLE_PCT


def targets(census):
    """census: DataFrame with pool, mint, dex, pool_created_at. Returns deduped target pools."""
    c = census.drop_duplicates("pool")
    pf_mints = set(c.loc[c.dex == "pump-fun", "mint"])
    grad = (c.dex == "pumpswap") | ((c.dex != "pump-fun") & c.mint.isin(pf_mints))
    samp = (c.dex == "pump-fun") & c.pool.map(in_sample)
    t = c[grad | samp].copy()
    t["kind"] = "grad"
    t.loc[samp[grad | samp], "kind"] = "sample"
    t["created"] = pd.to_datetime(t.pool_created_at, utc=True).map(lambda x: int(x.timestamp()))
    return t[["pool", "mint", "dex", "kind", "created"]]


def plan(t, log, now):
    """Next due milestone per pool, oldest pool first. log: DataFrame of LOG_COLS (may be empty).
    Returns list of (pool, milestone, before_ts, expired)."""
    created = dict(zip(t.pool, t.created))
    done, dead = set(), set()
    if len(log):
        errs = log[log.status == "error"].groupby(["pool", "milestone"]).size()
        for (p, m), k in errs.items():
            if k >= 3:
                done.add((p, m))
        for r in log[log.status.isin(DONE)].itertuples():
            done.add((r.pool, r.milestone))
            b = created.get(r.pool, 0) + int(r.milestone * 86400)
            if r.status in ("empty", "missing") or (r.status == "ok" and r.last_ts < b - DEAD_DAYS * 86400):
                dead.add(r.pool)
    out = []
    for r in t.sort_values("created").itertuples():
        if r.pool in dead:
            continue
        for m in MILESTONES:
            if (r.pool, m) in done:
                continue
            b = r.created + int(m * 86400)
            if b <= now:
                out.append((r.pool, m, b, b - WINDOW_H * 3600 < now - HISTORY_DAYS * 86400))
            break
    return out


# ---------------------------------------------------------------- I/O
def http(url, data=None, headers=None, method=None):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception:
        return 0, b""


def fetch(pool, b):
    for k in range(5):
        st, body = http(OHLCV.format(pool, b), headers={"Accept": "application/json", "User-Agent": "meme-census"})
        if st == 200:
            return "ok", json.loads(body)["data"]["attributes"]["ohlcv_list"]
        if st == 404:
            return "missing", []
        time.sleep(30 if st == 429 else 5 * (k + 1))
    return "error", []


class Releases:
    def __init__(self, repo, token):
        self.repo, self.h = repo, {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        self.ids = {}

    def rid(self, tag):
        if tag not in self.ids:
            st, body = http(f"https://api.github.com/repos/{self.repo}/releases/tags/{tag}", headers=self.h)
            if st != 200:
                st, body = http(f"https://api.github.com/repos/{self.repo}/releases", method="POST", headers=self.h,
                                data=json.dumps({"tag_name": tag, "name": tag, "target_commitish": "main",
                                                 "body": "Hourly OHLCV parquet assets (see prices.py)."}).encode())
                if st not in (200, 201):
                    raise RuntimeError(f"release {tag}: {st} {body[:200]}")
            self.ids[tag] = json.loads(body)["id"]
        return self.ids[tag]

    def upload(self, tag, name, blob):
        url = f"https://uploads.github.com/repos/{self.repo}/releases/{self.rid(tag)}/assets?name={name}"
        for k in range(5):
            st, body = http(url, data=blob, method="POST", headers={**self.h, "Content-Type": "application/octet-stream"})
            if st in (200, 201):
                return True
            time.sleep(5 * (k + 1))
        print("upload failed", st, body[:200], flush=True)
        return False


def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True).returncode == 0


def read_all(pattern):
    fs = glob.glob(pattern, recursive=True)
    return pd.concat([pd.read_csv(f) for f in fs], ignore_index=True) if fs else pd.DataFrame()


def flush(rel, candles, logrows):
    if not logrows:
        return True
    now = dt.datetime.utcnow()
    tag, name = now.strftime(PFX + "-%Y-%m"), now.strftime("ohlcv-%Y%m%d-%H%M%S.parquet")
    if candles:
        df = pd.DataFrame(candles, columns=["pool", "ts", "o", "h", "l", "c", "v_usd"])
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="zstd")
        if not rel.upload(tag, name, buf.getvalue()):
            return False
    path = now.strftime(LOGDIR + "/%Y/%m/%d/%H%M%S.csv.gz")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(LOG_COLS)
        w.writerows([r + [f"{tag}/{name}" if candles else ""] for r in logrows])
    git("add", path)
    git("commit", "-q", "-m", f"prices {path[len(LOGDIR) + 1:-7]} ({len(logrows)} fetches, {len(candles)} candles)")
    for i in range(5):
        if git("push", "-q", "origin", "HEAD:main"):
            break
        git("pull", "-q", "--rebase", "origin", "main")
        time.sleep(5 * (i + 1))
    print(len(logrows), "fetches,", len(candles), "candles ->", name, flush=True)
    return True


def main():
    run_min, flush_min = float(os.environ.get("RUN_MIN", 330)), float(os.environ.get("FLUSH_MIN", 30))
    gap = 60 / float(os.environ.get("CALLS_PER_MIN", 25))
    t_end, t_flush = time.time() + run_min * 60, time.time() + flush_min * 60
    rel = Releases(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
    t = targets(read_all("census/**/*.csv.gz"))
    log = read_all(LOGDIR + "/**/*.csv.gz")
    todo = plan(t, log, int(time.time()))[:20 if TEST else None]
    print(len(t), "targets,", len(todo), "due", flush=True)
    candles, logrows = [], []
    for pool, m, b, expired in todo:
        if time.time() > t_end:
            break
        now = dt.datetime.utcnow().isoformat(timespec="seconds")
        if expired:
            logrows.append([pool, m, now, "expired", 0, "", ""])
            continue
        st, rows = fetch(pool, b)
        candles += [[pool, *r] for r in rows]
        ts = [r[0] for r in rows]
        logrows.append([pool, m, now, "empty" if st == "ok" and not rows else st, len(rows),
                        min(ts) if ts else "", max(ts) if ts else ""])
        if time.time() >= t_flush:
            if flush(rel, candles, logrows):
                candles, logrows = [], []
            t_flush = time.time() + flush_min * 60
        time.sleep(gap)
    flush(rel, candles, logrows)
    remaining = len(plan(t, read_all(LOGDIR + "/**/*.csv.gz"), int(time.time())))
    print("remaining due:", remaining, flush=True)
    with open(os.environ.get("GITHUB_OUTPUT", os.devnull), "a") as f:
        f.write(f"more={'true' if remaining and not TEST else 'false'}\n")


if __name__ == "__main__":
    sys.exit(main())
