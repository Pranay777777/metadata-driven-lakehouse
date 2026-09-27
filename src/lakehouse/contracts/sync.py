"""Loading contracts, gating on them, and compiling them into rules.

Three jobs, deliberately separate:

**Load.** Read and validate the YAML itself. A malformed contract is
caught here rather than by a rule that quietly never fires.

**Gate.** `check_conformance` compares an arriving table against what
the contract promised — missing columns, wrong types. This is a breach
of the agreement rather than a data-quality problem, so it fails the run
outright. Step 26's drift detection answers "did this source change?";
this answers "does this source match what we agreed?", and a source can
fail one without the other.

**Sync.** `sync_contract` compiles the contract into `dq_rule` rows and
the owner and freshness fields on `source_object`. The control plane
stays the single thing the engine reads, and the contract stays the
single thing a human edits. Rules that came from a contract are owned by
it: syncing replaces them wholesale, so deleting an expectation from the
YAML deletes the rule rather than leaving an orphan running forever.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import yaml
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from lakehouse.contracts.model import ColumnContract, Contract
from lakehouse.metadata.enums import RuleType, Severity
from lakehouse.metadata.models import DataQualityRule, SourceObject, SourceSystem

CONTRACT_MARKER = "contract"
"""Marks a `dq_rule` as generated, so a sync can safely replace it."""


class ContractError(Exception):
    """Raised when a contract cannot be loaded or applied."""


@dataclass(frozen=True)
class ContractBreach:
    """One way an arriving table failed to match its contract."""

    column: str
    problem: str

    def __str__(self) -> str:
        return f"{self.column}: {self.problem}"


def load_contract(path: Path) -> Contract:
    """Read and validate one contract file.

    Raises:
        ContractError: if the file is not valid YAML or not a valid
            contract. The path is included, because "validation error on
            line 12" is useless when forty contracts are being loaded.
    """
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ContractError(f"{path}: not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ContractError(f"{path}: expected a mapping at the top level")
    try:
        return Contract.model_validate(raw)
    except ValueError as exc:
        raise ContractError(f"{path}: {exc}") from exc


def load_contracts(directory: Path) -> list[Contract]:
    """Load every contract under a directory, sorted by path.

    Raises:
        ContractError: on the first invalid file, or if two contracts
            claim the same object.
    """
    contracts = [load_contract(p) for p in sorted(directory.rglob("*.y*ml"))]
    seen: dict[tuple[str, str], Path] = {}
    for contract in contracts:
        key = (contract.source_system, contract.object)
        if key in seen:
            raise ContractError(f"two contracts claim {key[0]}.{key[1]}")
        seen[key] = directory
    return contracts


def check_conformance(contract: Contract, table: pa.Table) -> list[ContractBreach]:
    """Compare an arriving table against what the contract promised.

    Extra columns are not a breach. A contract states what must be
    present, not what must be absent, and treating additions as failures
    would undo step 26's additive-evolution policy.
    """
    breaches: list[ContractBreach] = []
    actual = {field.name: field for field in table.schema}
    for column in contract.columns:
        field = actual.get(column.name)
        if field is None:
            breaches.append(ContractBreach(column.name, "promised but missing"))
            continue
        if str(field.type) != column.type:
            breaches.append(
                ContractBreach(column.name, f"expected {column.type}, got {field.type}")
            )
    return breaches


def _rules_for(column: ColumnContract) -> list[tuple[str, str | None, Severity]]:
    """Every rule one contracted column implies, as (type, expression, severity)."""
    rules: list[tuple[str, str | None, Severity]] = []
    if not column.nullable:
        rules.append((RuleType.NOT_NULL, None, column.null_severity))

    expect = column.expect
    if expect is None:
        return rules

    if expect.unique:
        rules.append((RuleType.UNIQUE, None, expect.severity))
    if expect.allowed_values is not None:
        rules.append(
            (
                RuleType.ALLOWED_VALUES,
                json.dumps({"values": expect.allowed_values}),
                expect.severity,
            )
        )
    if expect.minimum is not None or expect.maximum is not None:
        rules.append(
            (
                RuleType.RANGE,
                json.dumps({"minimum": expect.minimum, "maximum": expect.maximum}),
                expect.severity,
            )
        )
    if expect.pattern is not None:
        rules.append((RuleType.REGEX, json.dumps({"pattern": expect.pattern}), expect.severity))
    return rules


def resolve_object(session: Session, contract: Contract) -> SourceObject:
    """Find the source object a contract describes.

    Raises:
        ContractError: if no such object is registered. A contract for
            something that does not exist is almost always a typo.
    """
    obj = session.scalars(
        select(SourceObject)
        .join(SourceSystem, SourceObject.source_system_id == SourceSystem.id)
        .where(SourceSystem.name == contract.source_system)
        .where(SourceObject.object_name == contract.object)
    ).one_or_none()
    if obj is None:
        raise ContractError(
            f"no registered source object for {contract.source_system}.{contract.object}"
        )
    return obj


@dataclass(frozen=True)
class SyncResult:
    """What a sync changed."""

    object_name: str
    rules_written: int
    rules_removed: int


def sync_contract(session: Session, contract: Contract) -> SyncResult:
    """Compile a contract into the control plane.

    Rules generated by a previous sync are deleted first, so removing an
    expectation from the YAML removes the rule. Hand-written rules — any
    whose expression is not marked as contract-generated — are left
    alone; the contract does not own them.
    """
    obj = resolve_object(session, contract)

    existing = list(
        session.scalars(select(DataQualityRule).where(DataQualityRule.source_object_id == obj.id))
    )
    generated = [r for r in existing if _is_generated(r)]
    removed = len(generated)
    if generated:
        session.execute(
            delete(DataQualityRule).where(DataQualityRule.id.in_([r.id for r in generated]))
        )

    written = 0
    for column in contract.columns:
        for rule_type, expression, severity in _rules_for(column):
            session.add(
                DataQualityRule(
                    source_object_id=obj.id,
                    rule_type=rule_type,
                    column_name=column.name,
                    expression=_mark(expression),
                    severity=severity,
                )
            )
            written += 1

    if contract.freshness_sla_minutes is not None:
        session.add(
            DataQualityRule(
                source_object_id=obj.id,
                rule_type=RuleType.FRESHNESS,
                column_name=contract.freshness_column,
                expression=_mark(json.dumps({"max_age_minutes": contract.freshness_sla_minutes})),
                severity=Severity.WARN,
            )
        )
        written += 1
        obj.freshness_sla_minutes = contract.freshness_sla_minutes

    obj.owner = contract.owner
    session.commit()
    return SyncResult(object_name=obj.object_name, rules_written=written, rules_removed=removed)


def _mark(expression: str | None) -> str:
    """Tag an expression as contract-generated."""
    payload = json.loads(expression) if expression else {}
    payload[CONTRACT_MARKER] = True
    return json.dumps(payload)


def _is_generated(rule: DataQualityRule) -> bool:
    if not rule.expression:
        return False
    try:
        payload = json.loads(rule.expression)
    except json.JSONDecodeError:
        return False
    return isinstance(payload, dict) and payload.get(CONTRACT_MARKER) is True
