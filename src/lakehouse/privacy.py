"""PII classification and masking.

Two halves that deliberately do not trust each other.

**Classification** is a *proposal*. `scan` reads a Bronze table, looks at
column names and at a sample of their values, and prints what it thinks
each column is. Nothing is masked as a result. `--apply` writes the
proposals into `column_metadata`, and even then it refuses to overwrite a
classification a human already made. Automatic detection that silently
changes what a pipeline publishes is worse than no detection, because the
first time it is wrong nobody finds out.

**Masking** is driven only by what is in `column_metadata`. Silver reads
the rows, applies the configured strategy, and writes. Reclassifying a
column is an UPDATE, not a deploy.

Where masking sits in the pipeline matters more than how it hashes:

- **After data quality, before any write.** Rules run on real values —
  a `regex` rule against a hashed email would pass forever — but nothing
  leaves Silver unmasked, including the rows diverted to quarantine.
  Quarantine is the easiest place in a lakehouse to leak PII, because it
  is the one table nobody thinks of as a table.
- **Before SCD2.** Tombstones keep a deleted member's row forever
  (ADR-005), so a value that reached history unmasked stays there. Masking
  first means the historical versions were never unmasked to begin with.
- **Bronze is left raw.** Bronze is the faithful record of what the source
  sent; masking it would make drift detection and replay lie. That is a
  real limitation, and ADR-017 says so rather than hiding it.

The hash is a keyed HMAC, so equal inputs give equal outputs. A masked
`customer_id` still joins to a masked `customer_id` in another table, and
still deduplicates. Without that property, masking a key column breaks
the star schema and everyone turns it off.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.credentials import (
    SecretNotFoundError,
    SecretStore,
    database_url,
    get_store,
    resolve,
)
from lakehouse.ingest.bronze import read_bronze
from lakehouse.logging import configure_logging
from lakehouse.metadata.enums import MaskingStrategy, Sensitivity
from lakehouse.metadata.models import ColumnMetadata, SourceObject
from lakehouse.migrate import ensure_schema
from lakehouse.tables import to_snake_case

logger = logging.getLogger(__name__)

DEV_MASKING_KEY = "lakehouse-development-masking-key"
"""Used when no masking key is set locally. Published, so it protects nothing."""

_HASH_LENGTH = 32
"""Hex characters kept from the HMAC. 128 bits — collisions are not the
threat model, and a full 64-character digest doubles the column width."""

PARTIAL_KEEP = 4
_PARTIAL_FILL = "*"

SAMPLE_ROWS = 1_000
"""Rows the scanner looks at. Detection is a proposal, not an audit, and
scanning eight million rows to learn what a column is would be theatre."""

_VALUE_MATCH_RATIO = 0.5
"""Fraction of non-null sampled values that must match a pattern before
the scanner claims the column holds that kind of data."""


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Proposal:
    """One column, what the scanner thinks it is, and why."""

    column: str
    sensitivity: Sensitivity
    strategy: MaskingStrategy
    evidence: str

    @property
    def masks(self) -> bool:
        return self.strategy is not MaskingStrategy.NONE


@dataclass(frozen=True)
class _Rule:
    """A pattern and what matching it implies. Used for both tables:
    `NAME_RULES` matches the column name, `VALUE_RULES` its contents."""

    pattern: re.Pattern[str]
    sensitivity: Sensitivity
    strategy: MaskingStrategy
    label: str


def _name(expr: str, sensitivity: Sensitivity, strategy: MaskingStrategy, label: str) -> _Rule:
    return _Rule(re.compile(expr), sensitivity, strategy, label)


NAME_RULES: tuple[_Rule, ...] = (
    # Government identifiers first — the most specific and the most
    # damaging, and 'tax_id' would otherwise be caught by a looser rule.
    _name(
        r"(ssn|social_security|passport|national_id|tax_id|cpf|cnpj|aadhaar|pan_number)",
        Sensitivity.SENSITIVE_PII,
        MaskingStrategy.HASH,
        "a government identifier",
    ),
    _name(
        r"(date_of_birth|birth_date|dob)",
        Sensitivity.SENSITIVE_PII,
        MaskingStrategy.REDACT,
        "a date of birth",
    ),
    _name(
        r"(document|identity)",
        Sensitivity.SENSITIVE_PII,
        MaskingStrategy.HASH,
        "an identity document",
    ),
    _name(r"(email|e_mail)", Sensitivity.PII, MaskingStrategy.HASH, "an email address"),
    _name(
        r"(phone|mobile|msisdn|telephone)",
        Sensitivity.PII,
        MaskingStrategy.PARTIAL,
        "a phone number",
    ),
    _name(
        r"(full_name|first_name|last_name|surname|_name$|^name$)",
        Sensitivity.PII,
        MaskingStrategy.HASH,
        "a person name",
    ),
    _name(
        r"(street|address_line|addr_line|house_no)",
        Sensitivity.PII,
        MaskingStrategy.HASH,
        "a street address",
    ),
    _name(
        r"(latitude|longitude|geo_lat|geo_lon)",
        Sensitivity.PII,
        MaskingStrategy.REDACT,
        "a precise location",
    ),
    _name(
        r"(ip_address|device_id|user_agent)",
        Sensitivity.PII,
        MaskingStrategy.HASH,
        "a device identifier",
    ),
    # Quasi-identifiers: labelled so they show up in an access review,
    # not masked. Masking a postcode destroys most of the analysis the
    # warehouse exists for, and on its own it identifies nobody.
    _name(
        r"(zip|postcode|postal_code)",
        Sensitivity.INTERNAL,
        MaskingStrategy.NONE,
        "a postal area",
    ),
    _name(
        r"(city|state|province|region|country)",
        Sensitivity.INTERNAL,
        MaskingStrategy.NONE,
        "coarse geography",
    ),
)

# Ordered most specific first. A CPF satisfies the phone pattern too, so
# a looser rule placed above it would claim the column and stop the scan.
VALUE_RULES: tuple[_Rule, ...] = (
    _Rule(
        re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"),
        Sensitivity.SENSITIVE_PII,
        MaskingStrategy.HASH,
        "a government identifier",
    ),
    _Rule(
        re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"),
        Sensitivity.PII,
        MaskingStrategy.HASH,
        "an email address",
    ),
    _Rule(
        re.compile(r"^\+?\d[\d\s().-]{7,}\d$"),
        Sensitivity.PII,
        MaskingStrategy.PARTIAL,
        "a phone number",
    ),
)

_RANK = {
    Sensitivity.NONE: 0,
    Sensitivity.INTERNAL: 1,
    Sensitivity.PII: 2,
    Sensitivity.SENSITIVE_PII: 3,
}


def _sample(column: pa.ChunkedArray | pa.Array) -> list[str]:
    """Up to `SAMPLE_ROWS` non-null values, as strings."""
    if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
        return []
    head = column.slice(0, SAMPLE_ROWS).to_pylist()
    return [v for v in head if v is not None and v != ""]


def classify_column(name: str, column: pa.ChunkedArray | pa.Array | None = None) -> Proposal | None:
    """Propose a classification for one column, or None for no evidence.

    The column name is checked first because it is what an analyst would
    read. Values are checked second and can only *raise* the proposal: a
    column called `notes` holding a million email addresses is PII no
    matter what it was named.
    """
    normalised = to_snake_case(name)
    proposal: Proposal | None = None
    for rule in NAME_RULES:
        if rule.pattern.search(normalised):
            proposal = Proposal(
                name, rule.sensitivity, rule.strategy, f"name suggests {rule.label}"
            )
            break

    values = _sample(column) if column is not None else []
    if values:
        for rule in VALUE_RULES:
            matches = sum(1 for v in values if rule.pattern.match(v))
            if matches / len(values) < _VALUE_MATCH_RATIO:
                continue
            share = f"{matches}/{len(values)} sampled values look like {rule.label}"
            if proposal is None or _RANK[rule.sensitivity] > _RANK[proposal.sensitivity]:
                proposal = Proposal(name, rule.sensitivity, rule.strategy, share)
            elif rule.sensitivity == proposal.sensitivity:
                proposal = Proposal(
                    name, proposal.sensitivity, proposal.strategy, f"{proposal.evidence}; {share}"
                )
            break

    if proposal is None:
        return None
    return _demote_unmaskable(proposal, column)


def _demote_unmaskable(proposal: Proposal, column: pa.ChunkedArray | pa.Array | None) -> Proposal:
    """Fall back to redaction where hashing cannot apply.

    `hash` and `partial` are string operations. Proposing them for an
    integer column would produce a config the pipeline then refuses at
    runtime — the wrong place to find out.
    """
    if column is None or not proposal.masks:
        return proposal
    if proposal.strategy is MaskingStrategy.REDACT:
        return proposal
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        return proposal
    return Proposal(
        proposal.column,
        proposal.sensitivity,
        MaskingStrategy.REDACT,
        f"{proposal.evidence}; not a string column, so redacted rather than hashed",
    )


def scan(table: pa.Table) -> list[Proposal]:
    """Propose classifications for every column that shows evidence."""
    proposals = []
    for field in table.schema:
        if field.name.startswith("_"):
            continue  # provenance columns the platform added itself
        proposal = classify_column(field.name, table.column(field.name))
        if proposal is not None:
            proposals.append(proposal)
    return proposals


def apply_proposals(session: Session, obj: SourceObject, proposals: Iterable[Proposal]) -> int:
    """Write proposals into `column_metadata`. Returns rows changed.

    An existing classification other than `none` is left alone. The
    scanner is allowed to fill a gap; it is not allowed to overrule the
    person who owns the data.
    """
    changed = 0
    for proposal in proposals:
        row = session.scalars(
            select(ColumnMetadata)
            .where(ColumnMetadata.source_object_id == obj.id)
            .where(ColumnMetadata.column_name == proposal.column)
        ).one_or_none()
        if row is None:
            session.add(
                ColumnMetadata(
                    source_object_id=obj.id,
                    column_name=proposal.column,
                    sensitivity=proposal.sensitivity,
                    masking_strategy=proposal.strategy,
                    business_description=f"classified by scan: {proposal.evidence}",
                )
            )
            changed += 1
        elif row.sensitivity == Sensitivity.NONE:
            row.sensitivity = proposal.sensitivity
            row.masking_strategy = proposal.strategy
            changed += 1
    session.commit()
    return changed


# --------------------------------------------------------------------------
# Masking
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnPolicy:
    """The masking decision for one column, as stored in the control plane."""

    column: str
    """Conformed (snake_case) name, so it matches the Silver table."""

    sensitivity: Sensitivity
    strategy: MaskingStrategy
    allow_in_gold: bool

    @property
    def restricted(self) -> bool:
        """Dropped on the way into Gold unless explicitly allow-listed."""
        return self.sensitivity is Sensitivity.SENSITIVE_PII and not self.allow_in_gold


def load_policies(session: Session, obj: SourceObject) -> dict[str, ColumnPolicy]:
    """Read one object's column policies, keyed by conformed column name."""
    rows = session.scalars(select(ColumnMetadata).where(ColumnMetadata.source_object_id == obj.id))
    return {
        to_snake_case(row.column_name): ColumnPolicy(
            column=to_snake_case(row.column_name),
            sensitivity=Sensitivity(row.sensitivity),
            strategy=MaskingStrategy(row.masking_strategy),
            allow_in_gold=row.allow_in_gold,
        )
        for row in rows
    }


def masking_key(settings: Settings | None = None, store: SecretStore | None = None) -> bytes:
    """The HMAC key, resolved by name through the secret store.

    Missing on a laptop, the published development key is used with a
    warning, so the demo runs out of the box. Missing from Key Vault is a
    hard failure: a production run silently masking with a public key
    would be worse than not masking, because it looks like protection.
    """
    resolved = settings or Settings()
    source = store or get_store(resolved)
    try:
        return resolve(resolved.masking_key_secret, source).get_secret_value().encode("utf-8")
    except SecretNotFoundError:
        if source.backend != "env":
            raise
    logger.warning(
        "masking key '%s' is not set — using the development key, which is public. "
        "Set MASKING_KEY before masking anything that matters.",
        resolved.masking_key_secret,
    )
    return DEV_MASKING_KEY.encode("utf-8")


def hash_value(value: str, key: bytes) -> str:
    """Keyed, deterministic, one-way. The same value always masks alike."""
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()[:_HASH_LENGTH]


def partial_value(value: str) -> str:
    """Keep the tail, mask the head. Short values are masked entirely."""
    if len(value) <= PARTIAL_KEEP:
        return _PARTIAL_FILL * len(value)
    return _PARTIAL_FILL * (len(value) - PARTIAL_KEEP) + value[-PARTIAL_KEEP:]


def _mask_column(
    column: pa.ChunkedArray, field: pa.Field, strategy: MaskingStrategy, key: bytes
) -> pa.ChunkedArray | pa.Array:
    if strategy is MaskingStrategy.REDACT:
        return pa.nulls(len(column), field.type)
    if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
        raise TypeError(
            f"column '{field.name}' is {field.type}, which the '{strategy}' strategy "
            f"cannot mask — use '{MaskingStrategy.REDACT}' for non-string columns"
        )
    transform = hash_value if strategy is MaskingStrategy.HASH else None
    values = column.to_pylist()
    if transform is not None:
        masked = [None if v is None else transform(v, key) for v in values]
    else:
        masked = [None if v is None else partial_value(v) for v in values]
    return pa.array(masked, type=field.type)


def mask(
    table: pa.Table, policies: dict[str, ColumnPolicy], key: bytes | None = None
) -> tuple[pa.Table, tuple[str, ...]]:
    """Apply every configured strategy. Returns the table and what changed.

    Nulls stay null: a masked null is still a null, and turning it into a
    hash of the empty string would invent a value that was never there.
    Columns with no policy, and policies set to `none`, pass through
    untouched — masking is opt-in per column, always.
    """
    resolved = key if key is not None else masking_key()
    masked_columns: list[str] = []
    for index, field in enumerate(table.schema):
        policy = policies.get(field.name)
        if policy is None or policy.strategy is MaskingStrategy.NONE:
            continue
        table = table.set_column(
            index, field, _mask_column(table.column(index), field, policy.strategy, resolved)
        )
        masked_columns.append(field.name)
    return table, tuple(masked_columns)


def restricted_columns(policies: dict[str, ColumnPolicy], keep: Sequence[str] = ()) -> list[str]:
    """Columns Gold must not publish.

    `keep` protects the natural keys. A dimension without its business
    key is not a dimension, so a restricted key column is reported
    through its masked value rather than removed — which is exactly what
    masking it was for.
    """
    protected = {to_snake_case(k) for k in keep}
    return [
        name for name, policy in policies.items() if policy.restricted and name not in protected
    ]


def drop_restricted(
    table: pa.Table, policies: dict[str, ColumnPolicy], keep: Sequence[str] = ()
) -> tuple[pa.Table, tuple[str, ...]]:
    """Remove restricted columns from a table bound for Gold."""
    dropped = [c for c in restricted_columns(policies, keep) if c in table.column_names]
    if not dropped:
        return table, ()
    return table.drop_columns(dropped), tuple(dropped)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _report(obj: SourceObject, proposals: list[Proposal]) -> None:
    print(f"\n{obj.object_name}")
    if not proposals:
        print("  no columns matched a classification rule")
        return
    for proposal in proposals:
        flag = "→" if proposal.masks else " "
        print(
            f"  {flag} {proposal.column:<24} {proposal.sensitivity:<14}"
            f" {proposal.strategy:<8} {proposal.evidence}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.privacy", description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the proposals into column_metadata (never overwrites a human classification)",
    )
    parser.add_argument("--object", help="scan one object instead of all active ones")
    parser.add_argument("--lake-root", type=Path, default=None)
    args = parser.parse_args(argv)

    settings = Settings()
    configure_logging(settings.log_level)
    lake_root = args.lake_root or Path(settings.lake_root)

    engine = create_engine(database_url(settings))
    ensure_schema(engine)

    scanned = 0
    written = 0
    with Session(engine) as session:
        query = select(SourceObject).where(SourceObject.active.is_(True))
        if args.object:
            query = query.where(SourceObject.object_name == args.object)
        objects = list(session.scalars(query.order_by(SourceObject.load_order)))
        if not objects:
            print("no active objects — run 'python -m lakehouse.pipeline --register' first")
            return 2

        for obj in objects:
            try:
                bronze = read_bronze(lake_root, obj)
            except Exception as exc:
                print(f"\n{obj.object_name}\n  skipped: {type(exc).__name__}")
                continue
            scanned += 1
            proposals = scan(bronze)
            _report(obj, proposals)
            if args.apply:
                written += apply_proposals(session, obj, proposals)

    print(f"\nscanned {scanned} object(s)")
    if args.apply:
        print(f"wrote {written} classification(s) into column_metadata")
    else:
        print("nothing written — re-run with --apply to store these classifications")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
