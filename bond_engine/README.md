# Bond aggregation engine (simplified)

Event-driven loop with persistent in-memory state: bonds master, market snapshots, positions, cash, and PnL. Queries read the live state without replaying the full history.

## Layout

- `state.py` — `EngineState` (bonds index, market, firm positions, per-desk / per-trader ledgers, cash, realized PnL)
- `engine.py` — `BondAggregationEngine`, `load_data`, command-line `run_cli`
- `main.py` — loads CSVs from `data/` and starts the CLI
- `data/bonds.csv`, `data/events.csv` — canonical schemas (see below)

## Run

From this directory so imports resolve:

```bash
cd bond_engine
python main.py
```

## CSV schemas

**bonds.csv:** `bond_id`, `issuer`, `coupon`, `maturity_date`, `face_value`, `sector`, `rating` (extend as needed).

**events.csv:** `event_id`, `event_time`, `event_type`, `bond_id`, `portfolio_id`, `quantity`, `price`, `spread`, optional `desk`, `trader`

If `desk` / `trader` are omitted, they default to `DEFAULT` and `_UNASSIGNED`. Trades update both the firm-wide book and the attributed desk/trader sub-ledgers; **sells** are validated against that desk’s (and trader’s) inventory for that bond.

Supported `event_type` values in this build: `MARKET_PRICE_UPDATE`, `TRADE_BUY`, `TRADE_SELL`, `SPREAD_UPDATE`. Non-market rows use `price` or `quantity` as required; unused numeric cells can be empty in CSV.

## Commands (interactive)

At the `>` prompt:

| Command | Action |
|--------|--------|
| `show instrument <bond_id>` | Last price, spread, quantity, avg cost, PV (`qty × price`), optional notional face |
| `show desk <code>` | Cash, realized/unrealized PnL, sum of MV, open positions for that desk |
| `show trader <id>` | Same for trader |
| `next` | Process the next event in order |
| `all` | Process all remaining events |
| `summary` | Firm-wide portfolio summary |
| `positions` | Firm open positions |
| `bond <bond_id>` | Raw dict (bond + market + position) |
| `help` | List commands |
| `quit` | Exit |

## Next steps

Accrued interest, dirty/clean price, coupons, maturity, multiple portfolios—add handlers and state slices without replacing the event-loop pattern.
