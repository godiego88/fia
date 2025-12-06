# tools/test_qb1_local.py
"""
Local validation script for QB1 + Stage1.
Runs QB1 in DRY mode using current config, validates structure,
and writes trigger_context.json. Final and production-ready.
"""

import json
import os
from brains.qb1.core import run_qb1
from fia.config_loader import get_config


def main():
    cfg = get_config()

    print("Running QB1 in DRY mode...")
    result = run_qb1(cfg)

    # Validate structure
    assert isinstance(result, dict), "QB1 output must be a dict"
    assert "signals" in result, "QB1 output missing 'signals'"
    assert "meta" in result, "QB1 output missing 'meta'"
    assert isinstance(result["signals"], list), "'signals' must be a list"
    assert isinstance(result["meta"], dict), "'meta' must be a dict"

    # Write output
    outfile = "trigger_context.json"
    with open(outfile, "w") as fh:
        json.dump(result, fh, indent=2, default=str)

    size = os.path.getsize(outfile)
    print(f"trigger_context.json written ({size} bytes)")

    # Basic integrity checks
    print(f"Signals produced: {len(result['signals'])}")
    print(f"Triggers: {len(result.get('triggers', []))}")

    print("QB1 validation completed successfully.")


if __name__ == "__main__":
    main()
