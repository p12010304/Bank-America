from __future__ import annotations

import shlex
from typing import Any

import pandas as pd

from state import EngineState


def load_data(bonds_path: str, events_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    bonds = pd.read_csv(bonds_path)
    events = pd.read_csv(events_path)
    events = events.sort_values("event_id").reset_index(drop=True)
    _normalize_event_columns(events)
    return bonds, events


def _normalize_event_columns(events: pd.DataFrame) -> None:
    if "desk" not in events.columns:
        events["desk"] = "DEFAULT"
    else:
        events["desk"] = events["desk"].fillna("DEFAULT").astype(str).str.strip()
        events.loc[events["desk"] == "", "desk"] = "DEFAULT"
    if "trader" not in events.columns:
        events["trader"] = ""
    else:
        events["trader"] = events["trader"].fillna("").astype(str).str.strip()


def _entity_trader(raw: str) -> str:
    return raw if raw else "_UNASSIGNED"


class BondAggregationEngine:
    def __init__(self, bonds_df: pd.DataFrame):
        self.state = EngineState(bonds_df)

    def process_event(self, event: pd.Series) -> None:
        event_type = event["event_type"]
        self.state.last_event_id = int(event["event_id"])

        if event_type == "MARKET_PRICE_UPDATE":
            self._handle_market_price_update(event)
        elif event_type == "SPREAD_UPDATE":
            self._handle_spread_update(event)
        elif event_type == "TRADE_BUY":
            self._handle_trade_buy(event)
        elif event_type == "TRADE_SELL":
            self._handle_trade_sell(event)
        else:
            raise ValueError(f"Unsupported event_type: {event_type!r}")

        self._refresh_portfolio_metrics()

    @staticmethod
    def _leg_buy(df: pd.DataFrame, bond_id: str, qty: float, trade_price: float) -> None:
        pos = df.loc[bond_id]
        old_qty = float(pos["quantity"])
        old_cost = float(pos["avg_cost"])
        new_qty = old_qty + qty
        new_avg_cost = (
            (old_qty * old_cost + qty * trade_price) / new_qty if new_qty != 0 else 0.0
        )
        df.at[bond_id, "quantity"] = new_qty
        df.at[bond_id, "avg_cost"] = new_avg_cost

    @staticmethod
    def _leg_sell(df: pd.DataFrame, bond_id: str, qty: float, trade_price: float) -> float:
        old_qty = float(df.at[bond_id, "quantity"])
        avg_cost = float(df.at[bond_id, "avg_cost"])
        if qty > old_qty:
            raise ValueError(f"Cannot sell {qty}; only {old_qty} available for {bond_id} on this book")
        realized = qty * (trade_price - avg_cost)
        new_qty = old_qty - qty
        df.at[bond_id, "quantity"] = new_qty
        if new_qty == 0:
            df.at[bond_id, "avg_cost"] = 0.0
        return float(realized)

    def _sync_global_positions_from_desks(self) -> None:
        for bond_id in self.state.bonds.index:
            total_q = 0.0
            basis = 0.0
            for _desk, df in self.state.desk_positions.items():
                q = float(df.at[bond_id, "quantity"])
                ac = float(df.at[bond_id, "avg_cost"])
                total_q += q
                basis += q * ac
            self.state.positions.at[bond_id, "quantity"] = total_q
            self.state.positions.at[bond_id, "avg_cost"] = (
                (basis / total_q) if total_q > 1e-15 else 0.0
            )

    def _handle_market_price_update(self, event: pd.Series) -> None:
        bond_id = event["bond_id"]
        new_price = float(event["price"])
        self.state.market.at[bond_id, "last_price"] = new_price

    def _handle_spread_update(self, event: pd.Series) -> None:
        bond_id = event["bond_id"]
        spread = float(event["spread"])
        self.state.market.at[bond_id, "last_spread"] = spread

    def _handle_trade_buy(self, event: pd.Series) -> None:
        bond_id = event["bond_id"]
        qty = float(event["quantity"])
        trade_price = float(event["price"])
        desk = str(event["desk"])
        trader = _entity_trader(str(event["trader"]))

        ddf = self.state.ensure_desk(desk)
        tdf = self.state.ensure_trader(trader)
        self._leg_buy(ddf, bond_id, qty, trade_price)
        self._leg_buy(tdf, bond_id, qty, trade_price)

        cash_delta = -qty * trade_price
        self.state.cash += cash_delta
        self.state.desk_cash[desk] = self.state.desk_cash.get(desk, 0.0) + cash_delta
        self.state.trader_cash[trader] = self.state.trader_cash.get(trader, 0.0) + cash_delta

        self._sync_global_positions_from_desks()

    def _handle_trade_sell(self, event: pd.Series) -> None:
        bond_id = event["bond_id"]
        qty = float(event["quantity"])
        trade_price = float(event["price"])
        desk = str(event["desk"])
        trader = _entity_trader(str(event["trader"]))

        ddf = self.state.ensure_desk(desk)
        tdf = self.state.ensure_trader(trader)
        r_desk = self._leg_sell(ddf, bond_id, qty, trade_price)
        r_trader = self._leg_sell(tdf, bond_id, qty, trade_price)
        assert abs(r_desk - r_trader) < 1e-9

        cash_delta = qty * trade_price
        self.state.cash += cash_delta
        self.state.desk_cash[desk] = self.state.desk_cash.get(desk, 0.0) + cash_delta
        self.state.trader_cash[trader] = self.state.trader_cash.get(trader, 0.0) + cash_delta

        self.state.realized_pnl += r_desk
        self.state.desk_realized[desk] = self.state.desk_realized.get(desk, 0.0) + r_desk
        self.state.trader_realized[trader] = self.state.trader_realized.get(trader, 0.0) + r_trader

        self._sync_global_positions_from_desks()

    def _refresh_portfolio_metrics(self) -> None:
        prices = self.state.market["last_price"].fillna(0.0)

        for _desk, df in self.state.desk_positions.items():
            q = df["quantity"]
            avg = df["avg_cost"]
            df["market_value"] = q * prices
            df["unrealized_pnl"] = q * (prices - avg)

        for _tr, df in self.state.trader_positions.items():
            q = df["quantity"]
            avg = df["avg_cost"]
            df["market_value"] = q * prices
            df["unrealized_pnl"] = q * (prices - avg)

        g = self.state.positions
        g["market_value"] = g["quantity"] * prices
        g["unrealized_pnl"] = g["quantity"] * (prices - g["avg_cost"])

    def get_portfolio_summary(self) -> dict[str, Any]:
        positions = self.state.positions
        total_mv = float(positions["market_value"].sum())
        total_unrealized = float(positions["unrealized_pnl"].sum())

        return {
            "last_event_id": self.state.last_event_id,
            "cash": self.state.cash,
            "realized_pnl": self.state.realized_pnl,
            "unrealized_pnl": total_unrealized,
            "portfolio_market_value": total_mv,
            "portfolio_total_value": total_mv + self.state.cash,
        }

    def get_positions(self) -> pd.DataFrame:
        return self.state.positions[self.state.positions["quantity"] != 0].copy()

    def get_bond_state(self, bond_id: str) -> dict[str, Any]:
        return {
            "bond": self.state.bonds.loc[bond_id].to_dict(),
            "market": self.state.market.loc[bond_id].to_dict(),
            "position": self.state.positions.loc[bond_id].to_dict(),
        }

    def get_instrument_view(self, bond_id: str) -> dict[str, Any]:
        """Position + price + PV (market value) for one bond."""
        if bond_id not in self.state.bonds.index:
            raise KeyError(f"Unknown bond_id: {bond_id!r}")
        bond = self.state.bonds.loc[bond_id].to_dict()
        mkt = self.state.market.loc[bond_id].to_dict()
        pos = self.state.positions.loc[bond_id].to_dict()
        last_raw = mkt.get("last_price")
        last_f = float(last_raw) if not pd.isna(last_raw) else None
        face = float(bond.get("face_value", 0) or 0)
        qty = float(pos.get("quantity", 0) or 0)
        pv = float(pos.get("market_value", 0) or 0)
        return {
            "bond_id": bond_id,
            "bond": bond,
            "last_price": last_f,
            "last_spread": mkt.get("last_spread"),
            "quantity": qty,
            "avg_cost": pos.get("avg_cost"),
            "pv": pv,
            "market_value": pv,
            "unrealized_pnl": pos.get("unrealized_pnl"),
            "notional_face": qty * face if face else None,
        }

    def get_desk_aggregate(self, desk: str) -> dict[str, Any]:
        desk = desk.strip()
        if desk not in self.state.desk_positions:
            return {
                "desk": desk,
                "found": False,
                "cash": 0.0,
                "realized_pnl": 0.0,
                "positions": pd.DataFrame(),
                "market_value": 0.0,
                "unrealized_pnl": 0.0,
                "portfolio_total_value": 0.0,
            }
        df = self.state.desk_positions[desk]
        open_pos = df[df["quantity"] != 0].copy()
        mv = float(df["market_value"].sum())
        ur = float(df["unrealized_pnl"].sum())
        cash = float(self.state.desk_cash.get(desk, 0.0))
        realized = float(self.state.desk_realized.get(desk, 0.0))
        return {
            "desk": desk,
            "found": True,
            "cash": cash,
            "realized_pnl": realized,
            "unrealized_pnl": ur,
            "market_value": mv,
            "portfolio_total_value": mv + cash,
            "positions": open_pos,
        }

    def get_trader_aggregate(self, trader: str) -> dict[str, Any]:
        tr = trader.strip()
        key = _entity_trader(tr)
        if key not in self.state.trader_positions:
            return {
                "trader": tr,
                "found": False,
                "cash": 0.0,
                "realized_pnl": 0.0,
                "positions": pd.DataFrame(),
                "market_value": 0.0,
                "unrealized_pnl": 0.0,
                "portfolio_total_value": 0.0,
            }
        df = self.state.trader_positions[key]
        open_pos = df[df["quantity"] != 0].copy()
        mv = float(df["market_value"].sum())
        ur = float(df["unrealized_pnl"].sum())
        cash = float(self.state.trader_cash.get(key, 0.0))
        realized = float(self.state.trader_realized.get(key, 0.0))
        return {
            "trader": key,
            "found": True,
            "cash": cash,
            "realized_pnl": realized,
            "unrealized_pnl": ur,
            "market_value": mv,
            "portfolio_total_value": mv + cash,
            "positions": open_pos,
        }


def _print_instrument(view: dict[str, Any]) -> None:
    print(f"instrument {view['bond_id']}")
    print(f"  last_price: {view['last_price']}")
    print(f"  last_spread: {view['last_spread']}")
    print(f"  quantity:   {view['quantity']}")
    print(f"  avg_cost:   {view['avg_cost']}")
    print(f"  PV (qty * price): {view['pv']}")
    if view.get("notional_face") is not None:
        print(f"  notional face (qty * face_value): {view['notional_face']}")


def _print_desk_or_trader(label: str, agg: dict[str, Any]) -> None:
    if not agg.get("found"):
        print(f"No activity for {label} {agg.get('desk') or agg.get('trader')!r}.")
        return
    name = agg.get("desk") or agg.get("trader")
    print(f"{label} {name}")
    print(f"  cash:                 {agg['cash']}")
    print(f"  realized_pnl:         {agg['realized_pnl']}")
    print(f"  unrealized_pnl:       {agg['unrealized_pnl']}")
    print(f"  market_value (sum):   {agg['market_value']}")
    print(f"  portfolio_total_value (MV + cash): {agg['portfolio_total_value']}")
    pos = agg["positions"]
    if len(pos) == 0:
        print("  open positions: (none)")
    else:
        print("  open positions:")
        print(pos.to_string())


def parse_command_line(line: str) -> tuple[str | None, list[str]]:
    line = line.strip()
    if not line:
        return None, []
    try:
        parts = shlex.split(line)
    except ValueError as e:
        raise ValueError(str(e)) from e
    cmd = parts[0].lower()
    return cmd, parts


def run_cli(engine: BondAggregationEngine, events_df: pd.DataFrame) -> None:
    pointer = 0
    total_events = len(events_df)

    print("Bond engine — type a command (help for usage).")

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            print()
            break

        if not line:
            continue

        lower = line.lower()
        if lower in ("quit", "exit", "q"):
            break

        try:
            _cmd, parts = parse_command_line(line)
        except ValueError as e:
            print(f"Parse error: {e}")
            continue

        if not parts:
            continue

        head = parts[0].lower()

        # show instrument BOND1 | show desk NY | show trader T_NY_1
        if head == "show" and len(parts) >= 2:
            sub = parts[1].lower()
            if sub == "instrument" and len(parts) >= 3:
                bond_id = parts[2]
                try:
                    v = engine.get_instrument_view(bond_id)
                    _print_instrument(v)
                except KeyError as e:
                    print(e)
                continue
            if sub == "desk" and len(parts) >= 3:
                desk = " ".join(parts[2:])  # allow "NY" or multi-token if ever needed
                agg = engine.get_desk_aggregate(desk)
                _print_desk_or_trader("desk", agg)
                continue
            if sub == "trader" and len(parts) >= 3:
                trader = " ".join(parts[2:])
                agg = engine.get_trader_aggregate(trader)
                _print_desk_or_trader("trader", agg)
                continue
            print('Unknown show command. Try: show instrument <bond_id>, show desk <code>, show trader <id>')
            continue

        if head in ("help", "h", "?"):
            print(
                "Commands:\n"
                "  show instrument <bond_id>   — price, qty, avg cost, PV\n"
                "  show desk <code>            — cash, PnL, MV, open positions for that desk\n"
                "  show trader <id>            — same for trader\n"
                "  next                        — process one event\n"
                "  all                         — process all remaining events\n"
                "  summary | portfolio         — firm-wide summary\n"
                "  positions                   — firm open positions\n"
                "  bond <bond_id>              — raw bond / market / position dict\n"
                "  quit                        — exit"
            )
            continue

        if head == "next":
            if pointer < total_events:
                event = events_df.iloc[pointer]
                engine.process_event(event)
                print(f"Processed event {event['event_id']} ({event['event_type']})")
                pointer += 1
            else:
                print("No more events.")
            continue

        if head in ("all", "run", "process-all"):
            while pointer < total_events:
                engine.process_event(events_df.iloc[pointer])
                pointer += 1
            print(f"All events processed ({total_events} total).")
            continue

        if head in ("summary", "portfolio"):
            print(engine.get_portfolio_summary())
            continue

        if head in ("positions", "pos"):
            print(engine.get_positions())
            continue

        if head == "bond" and len(parts) >= 2:
            bond_id = parts[1]
            try:
                print(engine.get_bond_state(bond_id))
            except KeyError:
                print(f"Unknown bond_id: {bond_id!r}")
            continue

        print(f"Unknown command: {line!r}. Type help.")
