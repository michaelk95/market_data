# Backlog

Wishlist items — not needed for MVP. Revisit when the core pipeline is stable.

---

## Data History

- **Extend history back to 1990**
  Currently pulling 10 years of history per ticker. Eventually pull from 1990
  where data is available. yfinance supports `period="max"` for this.

## Data Quality

- **Survivorship bias mitigation**
  The current ticker list reflects the *current* Russell 2000 composition only.
  This means backtests will be biased toward companies that survived and grew
  large enough to remain in the index. To fix this properly:
  - Source historical Russell 2000 membership data (e.g. from FTSE Russell
    directly, a data vendor like Tiingo/Quandl/CRSP, or an academic dataset)
  - Track index entry/exit dates per ticker
  - Include delisted/acquired/bankrupt tickers with data up to their last
    trading day
  - Tag each ticker row with whether it was in the index on that date

## Scheduling & Orchestration

- **Automated scheduler**
  Replace Windows Task Scheduler with a built-in scheduling solution so the
  pipeline is self-contained and portable (e.g. APScheduler, Prefect, or a
  Claude Code scheduled task).
