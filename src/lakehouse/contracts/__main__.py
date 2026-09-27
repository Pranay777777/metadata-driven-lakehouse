"""Validate contracts, and optionally sync them into the control plane.

    python -m lakehouse.contracts                 # validate only
    python -m lakehouse.contracts --sync          # validate, then apply

Validation needs no database, so it runs in CI as a cheap gate on every
pull request that touches a contract.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.contracts.sync import ContractError, load_contracts, sync_contract


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.contracts", description=__doc__)
    parser.add_argument("--dir", type=Path, default=Path("contracts"))
    parser.add_argument(
        "--sync", action="store_true", help="write the contracts into the control plane"
    )
    args = parser.parse_args(argv)

    if not args.dir.exists():
        print(f"no contract directory at {args.dir}", file=sys.stderr)
        return 2

    try:
        contracts = load_contracts(args.dir)
    except ContractError as exc:
        print(f"invalid contract — {exc}", file=sys.stderr)
        return 1

    print(f"{len(contracts)} contract(s) valid")
    if not args.sync:
        return 0

    engine = create_engine(Settings().database_url)
    with Session(engine) as session:
        for contract in contracts:
            try:
                result = sync_contract(session, contract)
            except ContractError as exc:
                print(f"could not sync — {exc}", file=sys.stderr)
                return 1
            print(
                f"{result.object_name}: {result.rules_written} rule(s) written, "
                f"{result.rules_removed} replaced"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
