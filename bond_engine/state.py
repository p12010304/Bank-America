import numpy as np
import pandas as pd


class EngineState:
    """In-memory aggregate state updated incrementally by the event loop."""

    def __init__(self, bonds_df: pd.DataFrame):
        self.bonds = bonds_df.set_index("bond_id").copy()

        self.market = pd.DataFrame(index=self.bonds.index, dtype=float)
        self.market["last_price"] = np.nan
        self.market["last_spread"] = np.nan

        self.positions = self._empty_positions_frame()

        # Firm-wide (synced as sum / rollup of desk ledgers after each trade)
        self.cash = 0.0
        self.realized_pnl = 0.0
        self.last_event_id = None

        # Per desk / trader: quantity & cost for attribution (same schema as positions)
        self.desk_positions: dict[str, pd.DataFrame] = {}
        self.trader_positions: dict[str, pd.DataFrame] = {}
        self.desk_cash: dict[str, float] = {}
        self.desk_realized: dict[str, float] = {}
        self.trader_cash: dict[str, float] = {}
        self.trader_realized: dict[str, float] = {}

    def _empty_positions_frame(self) -> pd.DataFrame:
        df = pd.DataFrame(index=self.bonds.index)
        df["quantity"] = 0.0
        df["avg_cost"] = 0.0
        df["market_value"] = 0.0
        df["unrealized_pnl"] = 0.0
        return df

    def ensure_desk(self, desk: str) -> pd.DataFrame:
        if desk not in self.desk_positions:
            self.desk_positions[desk] = self._empty_positions_frame().copy()
            self.desk_cash.setdefault(desk, 0.0)
            self.desk_realized.setdefault(desk, 0.0)
        return self.desk_positions[desk]

    def ensure_trader(self, trader: str) -> pd.DataFrame:
        if trader not in self.trader_positions:
            self.trader_positions[trader] = self._empty_positions_frame().copy()
            self.trader_cash.setdefault(trader, 0.0)
            self.trader_realized.setdefault(trader, 0.0)
        return self.trader_positions[trader]
