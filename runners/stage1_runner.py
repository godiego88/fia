from __future__ import annotations
from fia.config_loader import get_config
from brains.qb1 import build_qb1

def main():
    cfg = get_config()
    ticker = "AAPL"  # temporary for local testing

    qb1 = build_qb1(ticker)
    print("\n[QB1 OUTPUT]")
    print(qb1.model_dump_json(indent=2))

if __name__ == "__main__":
    main()
