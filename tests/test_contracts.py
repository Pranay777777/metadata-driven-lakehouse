"""Tests for data contracts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.contracts import (
    Contract,
    check_conformance,
    load_contract,
    load_contracts,
    sync_contract,
)
from lakehouse.contracts.__main__ import main
from lakehouse.contracts.sync import ContractError, resolve_object
from lakehouse.metadata.enums import LoadStrategy, RuleType, Severity, SourceKind
from lakehouse.metadata.models import (
    Base,
    DataQualityRule,
    SourceObject,
    SourceSystem,
)

VALID = """
version: 1
source_system: seed
object: customers
owner: data-platform@example.com
freshness_sla_minutes: 60
freshness_column: updated_at
columns:
  - name: customer_id
    type: string
    nullable: false
    expect:
      unique: true
      severity: fail
  - name: score
    type: int64
    expect:
      minimum: 0
      maximum: 100
      severity: quarantine
  - name: updated_at
    type: int64
    nullable: false
"""


@pytest.fixture
def session() -> Iterator[Session]:
    eng: Engine = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _fk(conn: object, _: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    with Session(eng) as s:
        yield s


@pytest.fixture
def obj(session: Session) -> SourceObject:
    system = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(system)
    session.commit()
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name="customers",
        target_path="bronze/seed/customers",
        load_strategy=LoadStrategy.FULL,
        primary_key_columns="customer_id",
    )
    session.add(o)
    session.commit()
    return o


def write(tmp_path: Path, text: str, name: str = "customers.yml") -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


def test_a_valid_contract_loads(tmp_path: Path) -> None:
    contract = load_contract(write(tmp_path, VALID))
    assert contract.object == "customers"
    assert contract.owner == "data-platform@example.com"
    assert len(contract.columns) == 3


def test_the_shipped_example_contract_is_valid() -> None:
    """The example in the repo must not rot."""
    contracts = load_contracts(Path("contracts"))
    assert contracts
    assert all(c.owner for c in contracts)


def test_malformed_yaml_names_the_file(tmp_path: Path) -> None:
    path = write(tmp_path, "columns: [unclosed")
    with pytest.raises(ContractError, match=str(path.name)):
        load_contract(path)


def test_an_unknown_field_is_rejected(tmp_path: Path) -> None:
    """A typo must fail loudly, not be ignored."""
    path = write(tmp_path, VALID + "  - name: extra\n    type: string\n    nulable: true\n")
    with pytest.raises(ContractError):
        load_contract(path)


def test_a_contract_needs_at_least_one_column(tmp_path: Path) -> None:
    path = write(tmp_path, "version: 1\nsource_system: s\nobject: o\nowner: me\ncolumns: []\n")
    with pytest.raises(ContractError):
        load_contract(path)


def test_duplicate_columns_are_rejected(tmp_path: Path) -> None:
    text = (
        "version: 1\nsource_system: s\nobject: o\nowner: me\ncolumns:\n"
        "  - name: a\n    type: string\n  - name: a\n    type: string\n"
    )
    with pytest.raises(ContractError, match="duplicate"):
        load_contract(write(tmp_path, text))


def test_freshness_requires_a_column(tmp_path: Path) -> None:
    text = (
        "version: 1\nsource_system: s\nobject: o\nowner: me\n"
        "freshness_sla_minutes: 60\ncolumns:\n  - name: a\n    type: string\n"
    )
    with pytest.raises(ContractError, match="freshness_column"):
        load_contract(write(tmp_path, text))


def test_freshness_column_must_be_contracted(tmp_path: Path) -> None:
    text = (
        "version: 1\nsource_system: s\nobject: o\nowner: me\n"
        "freshness_sla_minutes: 60\nfreshness_column: nope\n"
        "columns:\n  - name: a\n    type: string\n"
    )
    with pytest.raises(ContractError, match="not a contracted column"):
        load_contract(write(tmp_path, text))


def test_an_empty_expectation_is_rejected(tmp_path: Path) -> None:
    text = (
        "version: 1\nsource_system: s\nobject: o\nowner: me\ncolumns:\n"
        "  - name: a\n    type: string\n    expect:\n      severity: warn\n"
    )
    with pytest.raises(ContractError, match="at least one check"):
        load_contract(write(tmp_path, text))


def test_an_inverted_range_is_rejected(tmp_path: Path) -> None:
    text = (
        "version: 1\nsource_system: s\nobject: o\nowner: me\ncolumns:\n"
        "  - name: a\n    type: int64\n    expect:\n      minimum: 10\n      maximum: 1\n"
    )
    with pytest.raises(ContractError, match="above maximum"):
        load_contract(write(tmp_path, text))


def test_two_contracts_for_one_object_are_rejected(tmp_path: Path) -> None:
    write(tmp_path, VALID, "a.yml")
    write(tmp_path, VALID, "b.yml")
    with pytest.raises(ContractError, match="two contracts claim"):
        load_contracts(tmp_path)


def test_a_top_level_list_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ContractError, match="mapping"):
        load_contract(write(tmp_path, "- one\n- two\n"))


# --------------------------------------------------------------------------
# Conformance gate
# --------------------------------------------------------------------------


def contract() -> Contract:
    return Contract.model_validate(
        {
            "source_system": "seed",
            "object": "customers",
            "owner": "me",
            "columns": [
                {"name": "customer_id", "type": "string", "nullable": False},
                {"name": "score", "type": "int64"},
            ],
        }
    )


def test_a_matching_table_has_no_breaches() -> None:
    table = pa.table({"customer_id": ["c1"], "score": [5]})
    assert check_conformance(contract(), table) == []


def test_a_missing_column_is_a_breach() -> None:
    breaches = check_conformance(contract(), pa.table({"customer_id": ["c1"]}))
    assert len(breaches) == 1
    assert "missing" in str(breaches[0])


def test_a_wrong_type_is_a_breach() -> None:
    breaches = check_conformance(contract(), pa.table({"customer_id": ["c1"], "score": ["5"]}))
    assert "expected int64, got string" in str(breaches[0])


def test_an_extra_column_is_not_a_breach() -> None:
    """A contract states what must be present, not what must be absent."""
    table = pa.table({"customer_id": ["c1"], "score": [5], "city": ["rio"]})
    assert check_conformance(contract(), table) == []


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------


def test_sync_writes_rules_and_ownership(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    result = sync_contract(session, load_contract(write(tmp_path, VALID)))

    assert result.rules_written == 5, "not-null x2, unique, range, freshness"
    session.refresh(obj)
    assert obj.owner == "data-platform@example.com"
    assert obj.freshness_sla_minutes == 60

    types = {r.rule_type for r in session.query(DataQualityRule).all()}
    assert types == {
        RuleType.NOT_NULL,
        RuleType.UNIQUE,
        RuleType.RANGE,
        RuleType.FRESHNESS,
    }


def test_severity_carries_from_the_contract(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    sync_contract(session, load_contract(write(tmp_path, VALID)))
    rule = session.query(DataQualityRule).filter_by(rule_type=RuleType.RANGE).one()
    assert rule.severity == Severity.QUARANTINE
    assert json.loads(rule.expression or "{}")["maximum"] == 100


def test_resyncing_replaces_rather_than_duplicates(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    sync_contract(session, load_contract(write(tmp_path, VALID)))
    first = session.query(DataQualityRule).count()
    result = sync_contract(session, load_contract(write(tmp_path, VALID)))

    assert session.query(DataQualityRule).count() == first
    assert result.rules_removed == first


def test_removing_an_expectation_removes_the_rule(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    sync_contract(session, load_contract(write(tmp_path, VALID)))
    trimmed = VALID.replace("    expect:\n      unique: true\n      severity: fail\n", "")
    sync_contract(session, load_contract(write(tmp_path, trimmed, "trimmed.yml")))

    assert session.query(DataQualityRule).filter_by(rule_type=RuleType.UNIQUE).count() == 0


def test_hand_written_rules_survive_a_sync(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """The contract owns the rules it generated, and nothing else."""
    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.ROW_COUNT,
            expression='{"minimum": 1}',
            severity=Severity.WARN,
        )
    )
    session.commit()

    sync_contract(session, load_contract(write(tmp_path, VALID)))
    assert session.query(DataQualityRule).filter_by(rule_type=RuleType.ROW_COUNT).count() == 1


def test_a_contract_for_an_unknown_object_is_refused(session: Session, tmp_path: Path) -> None:
    with pytest.raises(ContractError, match="no registered source object"):
        sync_contract(session, load_contract(write(tmp_path, VALID)))


def test_resolve_object_finds_the_registered_object(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    assert resolve_object(session, load_contract(write(tmp_path, VALID))).id == obj.id


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_the_cli_validates_without_a_database(tmp_path: Path) -> None:
    write(tmp_path, VALID)
    assert main(["--dir", str(tmp_path)]) == 0


def test_the_cli_reports_an_invalid_contract(tmp_path: Path) -> None:
    write(tmp_path, "columns: [unclosed")
    assert main(["--dir", str(tmp_path)]) == 1


def test_the_cli_reports_a_missing_directory(tmp_path: Path) -> None:
    assert main(["--dir", str(tmp_path / "nope")]) == 2
