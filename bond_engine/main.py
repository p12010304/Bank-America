from pathlib import Path

from engine import BondAggregationEngine, load_data, run_cli

DATA_DIR = Path(__file__).resolve().parent / "data"


def main() -> None:
    bonds, events = load_data(str(DATA_DIR / "bonds.csv"), str(DATA_DIR / "events.csv"))
    engine = BondAggregationEngine(bonds)
    run_cli(engine, events)


if __name__ == "__main__":
    main()
