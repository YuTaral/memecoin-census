# memecoin-census

Survivorship-free record of every new Solana DEX pool (pump.fun, PumpSwap, Meteora, ...) from the free
GeckoTerminal `new_pools` endpoint, captured continuously by GitHub Actions. Dead coins are included.

- `census.py` — recorder (1 sweep/min of pages 1-10, deduped by pool, flushed every 10 min).
- `.github/workflows/census.yml` — ~5h40m runs that dispatch their own successor; 30-min watchdog cron.
- `census/YYYY/MM/DD/HHMMSS.csv.gz` — columns: seen_at, pool_created_at, dex, pool, mint, symbol, name,
  fdv_usd, reserve_usd, vol_h1_usd, buys_h1, sells_h1 (all UTC; values are at first sighting).
