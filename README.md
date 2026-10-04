# memecoin-census

Survivorship-free record of every new Solana DEX pool (pump.fun, PumpSwap, Meteora, ...) from the free
GeckoTerminal `new_pools` endpoint, captured continuously by GitHub Actions. Dead coins are included.

- `census.py` — recorder (1 sweep/min of pages 1-10, deduped by pool, flushed every 10 min).
- `.github/workflows/census.yml` — ~5h40m runs that dispatch their own successor; 30-min watchdog cron.
- `census/YYYY/MM/DD/HHMMSS.csv.gz` — columns: seen_at, pool_created_at, dex, pool, mint, symbol, name,
  fdv_usd, reserve_usd, vol_h1_usd, buys_h1, sells_h1 (all UTC; values are at first sighting).

## Prices (`prices.py`, `.github/workflows/prices.yml`)
- Targets: every PumpSwap pool, every other pool of a pump.fun-launched mint (graduations), seeded 5 % sample
  (seed 23) of pump-fun bonding-curve pools.
- One hourly-OHLCV call per pool at ages 41/82/123/164/180 days (anchored at creation + age, so late runs return
  the same window). Pools with no trade in the last 30 days of a window are marked dead and dropped.
- Candles: parquet assets on releases `ohlcv-YYYY-MM`. Progress log: `ohlcv_log/YYYY/MM/DD/*.csv.gz`
  (pool, milestone, fetched_at, status, n, first_ts, last_ts, asset).
- Runs every 6 h (and chains itself while work remains); idle until the first pools reach 41 days (2026-11-14).
