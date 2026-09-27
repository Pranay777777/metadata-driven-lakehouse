"""The data quality engine.

Rules come from `dq_rule`, which step 27's contracts compile into. This
module evaluates them and applies the consequence each one declares.

The three tiers exist because not every quality problem should stop a
pipeline and not every one should be ignored, and a system offering only
those two options gets configured entirely as "ignore" within a quarter:

- **warn** — record the failure, load everything, carry on.
- **quarantine** — divert the failing rows, load the rest.
- **fail** — abort. Nothing is written.

Two properties are deliberate:

**Every rule is evaluated before any consequence is applied.** Stopping
at the first failure would hide the other nine, and the first failure is
rarely the informative one.

**A rule that cannot be evaluated is an error, not a skip.** A silently
skipped rule is worse than no rule, because someone is relying on it.

Quarantined rows are returned, not written — persisting them is step 29.
Until then they are dropped from the load, and `rows_rejected` on the
task run is where that shows up.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import select
from sqlalchemy.orm import Session

from lakehouse.metadata.enums import RuleType, RunStatus, Severity
from lakehouse.metadata.models import DataQualityResult, DataQualityRule, SourceObject, TaskRun

_MICROS_PER_MINUTE = 60_000_000

REJECTION_REASON = "_rejection_reason"
"""Column added to diverted rows naming every rule they breached."""


class DataQualityError(Exception):
    """Raised when a fail-severity rule is breached. Nothing is written."""


class RuleEvaluationError(Exception):
    """Raised when a configured rule cannot be evaluated by this engine."""


@dataclass(frozen=True)
class RuleOutcome:
    """The result of one rule against one batch."""

    rule_id: int
    rule_type: str
    column: str | None
    severity: str
    passed: bool
    failed_rows: int
    detail: str

    def __str__(self) -> str:
        target = self.column or "<table>"
        return f"{self.rule_type} on {target}: {self.detail}"


@dataclass(frozen=True)
class QualityOutcome:
    """Everything one evaluation produced."""

    results: list[RuleOutcome]
    kept: pa.Table
    rejected: pa.Table
    status: str

    @property
    def failures(self) -> list[RuleOutcome]:
        return [r for r in self.results if not r.passed]

    @property
    def rows_rejected(self) -> int:
        return int(self.rejected.num_rows)


def _parameters(rule: DataQualityRule) -> dict[str, object]:
    if not rule.expression:
        return {}
    try:
        payload = json.loads(rule.expression)
    except json.JSONDecodeError as exc:
        raise RuleEvaluationError(
            f"rule {rule.id} ({rule.rule_type}) has an unparseable expression"
        ) from exc
    if not isinstance(payload, dict):
        raise RuleEvaluationError(f"rule {rule.id} ({rule.rule_type}) must be a JSON object")
    return payload


def _column(rule: DataQualityRule, table: pa.Table) -> pa.ChunkedArray:
    if not rule.column_name:
        raise RuleEvaluationError(f"rule {rule.id} ({rule.rule_type}) needs a column")
    if rule.column_name not in table.column_names:
        raise RuleEvaluationError(
            f"rule {rule.id} names column '{rule.column_name}', which is not in the batch"
        )
    return table.column(rule.column_name)


def _failing_mask(rule: DataQualityRule, table: pa.Table) -> pa.Array:
    """Per-row mask: true where this rule is breached.

    Nulls pass every rule except NOT_NULL. A null is an absent value, not
    a wrong one, and double-reporting it as both missing and
    out-of-range buries the actual finding.
    """
    params = _parameters(rule)

    if rule.rule_type == RuleType.NOT_NULL:
        return pc.is_null(_column(rule, table)).combine_chunks()

    if rule.rule_type == RuleType.UNIQUE:
        values = _column(rule, table).to_pylist()
        counts: dict[object, int] = {}
        for value in values:
            counts[value] = counts.get(value, 0) + 1
        return pa.array([counts[v] > 1 for v in values], type=pa.bool_())

    if rule.rule_type == RuleType.RANGE:
        column = _column(rule, table)
        minimum, maximum = params.get("minimum"), params.get("maximum")
        if minimum is None and maximum is None:
            raise RuleEvaluationError(f"rule {rule.id} (range) states neither bound")
        below = pc.less(column, minimum) if minimum is not None else None
        above = pc.greater(column, maximum) if maximum is not None else None
        if below is not None and above is not None:
            mask = pc.or_(below, above)
        else:
            mask = below if below is not None else above
        return pc.fill_null(mask, False).combine_chunks()

    if rule.rule_type == RuleType.ALLOWED_VALUES:
        values = params.get("values")
        if not isinstance(values, list):
            raise RuleEvaluationError(f"rule {rule.id} (allowed_values) needs a values list")
        mask = pc.invert(pc.is_in(_column(rule, table), value_set=pa.array(values)))
        return pc.fill_null(mask, False).combine_chunks()

    if rule.rule_type == RuleType.REGEX:
        pattern = params.get("pattern")
        if not isinstance(pattern, str):
            raise RuleEvaluationError(f"rule {rule.id} (regex) needs a pattern")
        mask = pc.invert(pc.match_substring_regex(_column(rule, table), pattern))
        return pc.fill_null(mask, False).combine_chunks()

    raise RuleEvaluationError(
        f"rule {rule.id} has type '{rule.rule_type}', which this engine cannot evaluate"
    )


def _table_level(rule: DataQualityRule, table: pa.Table, now: datetime) -> tuple[bool, str]:
    """Evaluate a rule about the batch as a whole."""
    params = _parameters(rule)

    if rule.rule_type == RuleType.ROW_COUNT:
        minimum, maximum = params.get("minimum"), params.get("maximum")
        count = table.num_rows
        if minimum is not None and count < float(minimum):  # type: ignore[arg-type]
            return False, f"{count} rows, minimum {minimum}"
        if maximum is not None and count > float(maximum):  # type: ignore[arg-type]
            return False, f"{count} rows, maximum {maximum}"
        return True, f"{count} rows"

    limit = params.get("max_age_minutes")
    if not isinstance(limit, int | float):
        raise RuleEvaluationError(f"rule {rule.id} (freshness) needs max_age_minutes")
    column = _column(rule, table)
    if table.num_rows == 0:
        return False, "no rows, so freshness cannot be established"

    newest = pc.max(column.cast(pa.int64()) if pa.types.is_timestamp(column.type) else column)
    value = newest.as_py()
    if value is None:
        return False, "every value is null"
    micros = int(value) if pa.types.is_timestamp(column.type) else int(value) * 1_000_000
    age_minutes = (int(now.timestamp() * 1_000_000) - micros) / _MICROS_PER_MINUTE
    if age_minutes > limit:
        return False, f"newest row is {age_minutes:.0f} minutes old, SLA {limit}"
    return True, f"newest row is {max(age_minutes, 0):.0f} minutes old"


def active_rules(session: Session, obj: SourceObject) -> list[DataQualityRule]:
    """Rules configured for an object, in a stable order."""
    return list(
        session.scalars(
            select(DataQualityRule)
            .where(DataQualityRule.source_object_id == obj.id)
            .where(DataQualityRule.active)
            .order_by(DataQualityRule.id)
        )
    )


def evaluate(
    session: Session,
    task: TaskRun,
    obj: SourceObject,
    table: pa.Table,
    now: datetime | None = None,
) -> QualityOutcome:
    """Run every active rule and apply the strictest consequence earned.

    Raises:
        DataQualityError: if any fail-severity rule was breached. Results
            are recorded first, so the audit shows what happened.
        RuleEvaluationError: if a rule is misconfigured for this engine.
    """
    moment = now or datetime.now(UTC)
    rules = active_rules(session, obj)

    results: list[RuleOutcome] = []
    quarantine_mask: pa.Array | None = None
    quarantine_reasons: list[tuple[str, pa.Array]] = []
    fatal: list[RuleOutcome] = []

    for rule in rules:
        if rule.rule_type in (RuleType.ROW_COUNT, RuleType.FRESHNESS):
            ok, detail = _table_level(rule, table, moment)
            failed = 0 if ok else table.num_rows
            mask = None
        else:
            mask = _failing_mask(rule, table)
            failed = int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)
            ok = failed == 0
            detail = "all rows pass" if ok else f"{failed} row(s) breach the rule"

        outcome = RuleOutcome(
            rule_id=rule.id,
            rule_type=rule.rule_type,
            column=rule.column_name,
            severity=rule.severity,
            passed=ok,
            failed_rows=failed,
            detail=detail,
        )
        results.append(outcome)
        session.add(
            DataQualityResult(
                task_run_id=task.id,
                dq_rule_id=rule.id,
                passed=ok,
                failed_row_count=failed,
                detail=detail,
            )
        )

        if ok:
            continue
        if rule.severity == Severity.FAIL:
            fatal.append(outcome)
        elif rule.severity == Severity.QUARANTINE and mask is not None:
            quarantine_mask = mask if quarantine_mask is None else pc.or_(quarantine_mask, mask)
            label = f"{rule.rule_type}:{rule.column_name}" if rule.column_name else rule.rule_type
            quarantine_reasons.append((label, mask))

    session.commit()

    if fatal:
        raise DataQualityError(
            f"'{obj.object_name}' breached {len(fatal)} fail-severity rule(s): "
            + "; ".join(str(f) for f in fatal)
        )

    if quarantine_mask is None:
        return QualityOutcome(results, table, table.slice(0, 0), RunStatus.SUCCEEDED)

    rejected = _with_reasons(table.filter(quarantine_mask), quarantine_mask, quarantine_reasons)
    kept = table.filter(pc.invert(quarantine_mask))
    status = RunStatus.QUARANTINED if rejected.num_rows else RunStatus.SUCCEEDED
    return QualityOutcome(results, kept, rejected, status)


def _with_reasons(
    rejected: pa.Table, combined: pa.Array, reasons: list[tuple[str, pa.Array]]
) -> pa.Table:
    """Label each diverted row with every rule it breached.

    A row diverted for two reasons carries both. Recording only the
    first turns "why was this rejected" into a guessing game, which is
    the question quarantine exists to answer.
    """
    if rejected.num_rows == 0:
        return rejected.append_column(REJECTION_REASON, pa.array([], type=pa.string()))

    kept_positions = [i for i, flag in enumerate(combined.to_pylist()) if flag]
    per_rule = [(label, mask.to_pylist()) for label, mask in reasons]
    labels = [", ".join(label for label, mask in per_rule if mask[i]) for i in kept_positions]
    return rejected.append_column(REJECTION_REASON, pa.array(labels, type=pa.string()))
