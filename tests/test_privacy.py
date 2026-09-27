"""Tests for PII classification and masking."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.credentials import SecretNotFoundError
from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import (
    LoadStrategy,
    MaskingStrategy,
    RuleType,
    Sensitivity,
    Severity,
    SourceKind,
)
from lakehouse.metadata.models import Base, ColumnMetadata, SourceObject, SourceSystem
from lakehouse.privacy import (
    DEV_MASKING_KEY,
    ColumnPolicy,
    Proposal,
    apply_proposals,
    classify_column,
    drop_restricted,
    hash_value,
    load_policies,
    main,
    mask,
    masking_key,
    partial_value,
    restricted_columns,
    scan,
)
from lakehouse.transform.silver import build_silver, read_silver

KEY = b"test-key"


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
        incremental_column="updated_at",
    )
    session.add(o)
    session.commit()
    return o


def people(n: int = 3) -> pa.Table:
    return pa.table(
        {
            "customer_id": [f"cust_{i}" for i in range(n)],
            "customer_email": [f"person{i}@example.com" for i in range(n)],
            "customer_phone": ["+55 11 912345678", None, "+55 21 998765432"][:n],
            "customer_document": [f"123.456.78{i}-01" for i in range(n)],
            "customer_city": ["sao paulo"] * n,
            "updated_at": list(range(1, n + 1)),
        }
    )


# --- classification -------------------------------------------------------


def test_a_column_name_alone_is_enough_to_classify() -> None:
    proposal = classify_column("customer_email")
    assert proposal is not None
    assert proposal.sensitivity is Sensitivity.PII
    assert proposal.strategy is MaskingStrategy.HASH


def test_an_unrecognised_column_is_left_alone() -> None:
    assert classify_column("freight_value") is None


def test_values_classify_a_column_the_name_hides() -> None:
    column = pa.chunked_array([[f"user{i}@example.com" for i in range(20)]])
    proposal = classify_column("notes", column)
    assert proposal is not None
    assert proposal.sensitivity is Sensitivity.PII
    assert "sampled values" in proposal.evidence


def test_values_never_lower_a_name_based_classification() -> None:
    # The name says government identifier; the values also match the
    # looser phone pattern. The stronger claim has to win.
    column = pa.chunked_array([[f"123.456.78{i}-90" for i in range(10)]])
    proposal = classify_column("customer_document", column)
    assert proposal is not None
    assert proposal.sensitivity is Sensitivity.SENSITIVE_PII


def test_a_minority_of_matches_is_not_evidence() -> None:
    values = [f"user{i}@example.com" for i in range(2)] + ["plain text"] * 18
    assert classify_column("notes", pa.chunked_array([values])) is None


def test_a_non_string_column_is_redacted_rather_than_hashed() -> None:
    proposal = classify_column("national_id", pa.chunked_array([[1, 2, 3]]))
    assert proposal is not None
    assert proposal.strategy is MaskingStrategy.REDACT
    assert "not a string column" in proposal.evidence


def test_quasi_identifiers_are_labelled_but_not_masked() -> None:
    proposal = classify_column("customer_city")
    assert proposal is not None
    assert proposal.sensitivity is Sensitivity.INTERNAL
    assert not proposal.masks


def test_scan_skips_provenance_columns() -> None:
    table = people().append_column("_ingested_at", pa.array([1, 2, 3]))
    assert all(not p.column.startswith("_") for p in scan(table))


def test_apply_writes_classifications(session: Session, obj: SourceObject) -> None:
    changed = apply_proposals(session, obj, scan(people()))
    assert changed == 4
    policies = load_policies(session, obj)
    assert policies["customer_email"].strategy is MaskingStrategy.HASH
    assert policies["customer_document"].sensitivity is Sensitivity.SENSITIVE_PII


def test_apply_never_overrules_a_human(session: Session, obj: SourceObject) -> None:
    session.add(
        ColumnMetadata(
            source_object_id=obj.id,
            column_name="customer_email",
            sensitivity=Sensitivity.INTERNAL,
            masking_strategy=MaskingStrategy.NONE,
        )
    )
    session.commit()

    apply_proposals(session, obj, scan(people()))

    assert load_policies(session, obj)["customer_email"].sensitivity is Sensitivity.INTERNAL


def test_apply_fills_an_unclassified_row(session: Session, obj: SourceObject) -> None:
    session.add(
        ColumnMetadata(
            source_object_id=obj.id,
            column_name="customer_email",
            sensitivity=Sensitivity.NONE,
        )
    )
    session.commit()

    apply_proposals(session, obj, scan(people()))

    assert load_policies(session, obj)["customer_email"].strategy is MaskingStrategy.HASH


def test_apply_is_idempotent(session: Session, obj: SourceObject) -> None:
    apply_proposals(session, obj, scan(people()))
    assert apply_proposals(session, obj, scan(people())) == 0


# --- masking --------------------------------------------------------------


def policies(**strategies: MaskingStrategy) -> dict[str, ColumnPolicy]:
    return {
        column: ColumnPolicy(column, Sensitivity.PII, strategy, allow_in_gold=False)
        for column, strategy in strategies.items()
    }


def test_the_same_value_always_masks_the_same_way() -> None:
    assert hash_value("ana@example.com", KEY) == hash_value("ana@example.com", KEY)
    assert hash_value("ana@example.com", KEY) != hash_value("bruno@example.com", KEY)


def test_a_different_key_gives_a_different_mask() -> None:
    assert hash_value("ana@example.com", KEY) != hash_value("ana@example.com", b"other")


def test_masked_keys_still_join() -> None:
    left = pa.table({"customer_id": ["cust_1", "cust_2"]})
    right = pa.table({"customer_id": ["cust_2", "cust_3"]})
    policy = policies(customer_id=MaskingStrategy.HASH)

    masked_left, _ = mask(left, policy, KEY)
    masked_right, _ = mask(right, policy, KEY)

    overlap = set(masked_left.column("customer_id").to_pylist()) & set(
        masked_right.column("customer_id").to_pylist()
    )
    assert len(overlap) == 1


def test_masking_reports_what_it_touched() -> None:
    _, touched = mask(people(), policies(customer_email=MaskingStrategy.HASH), KEY)
    assert touched == ("customer_email",)


def test_an_unclassified_column_passes_through() -> None:
    masked, _ = mask(people(), policies(customer_email=MaskingStrategy.HASH), KEY)
    assert (
        masked.column("customer_city").to_pylist() == people().column("customer_city").to_pylist()
    )


def test_nulls_survive_masking() -> None:
    masked, _ = mask(people(), policies(customer_phone=MaskingStrategy.PARTIAL), KEY)
    assert masked.column("customer_phone").to_pylist()[1] is None


def test_partial_keeps_the_tail() -> None:
    assert partial_value("+55 11 912345678") == "************5678"
    assert partial_value("abc") == "***"


def test_redact_nulls_the_column() -> None:
    masked, _ = mask(people(), policies(customer_document=MaskingStrategy.REDACT), KEY)
    assert masked.column("customer_document").null_count == 3


def test_redact_works_on_a_non_string_column() -> None:
    masked, _ = mask(people(), policies(updated_at=MaskingStrategy.REDACT), KEY)
    assert masked.column("updated_at").null_count == 3


def test_hashing_a_non_string_column_says_what_to_do_instead() -> None:
    with pytest.raises(TypeError, match="redact"):
        mask(people(), policies(updated_at=MaskingStrategy.HASH), KEY)


def test_masking_preserves_the_schema() -> None:
    masked, _ = mask(people(), policies(customer_email=MaskingStrategy.HASH), KEY)
    assert masked.schema == people().schema


class _Store:
    """A secret store holding whatever the test gives it."""

    def __init__(self, backend: str = "env", **secrets: str) -> None:
        self.backend = backend
        self._secrets = secrets

    def get(self, name: str) -> SecretStr | None:
        value = self._secrets.get(name)
        return SecretStr(value) if value else None


def test_the_default_key_warns(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        assert masking_key(Settings(), _Store()) == DEV_MASKING_KEY.encode()
    assert "development key" in caplog.text


def test_a_configured_key_is_used_quietly(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        assert masking_key(Settings(), _Store(**{"masking-key": "real"})) == b"real"
    assert "development key" not in caplog.text


def test_the_key_name_is_configuration() -> None:
    settings = Settings(masking_key_secret="pii-hmac")
    assert masking_key(settings, _Store(**{"pii-hmac": "other"})) == b"other"


def test_a_missing_key_in_key_vault_stops_the_run() -> None:
    # A production run masking with a public key looks like protection
    # and is not — failing is the honest outcome (ADR-017, ADR-018).
    with pytest.raises(SecretNotFoundError, match="masking-key"):
        masking_key(Settings(), _Store(backend="keyvault"))


# --- gold restriction -----------------------------------------------------


def restriction(**pairs: Sensitivity) -> dict[str, ColumnPolicy]:
    return {
        column: ColumnPolicy(column, sensitivity, MaskingStrategy.HASH, allow_in_gold=False)
        for column, sensitivity in pairs.items()
    }


def test_sensitive_columns_are_withheld_from_gold() -> None:
    table, dropped = drop_restricted(
        people(), restriction(customer_document=Sensitivity.SENSITIVE_PII)
    )
    assert dropped == ("customer_document",)
    assert "customer_document" not in table.column_names


def test_plain_pii_still_reaches_gold_masked() -> None:
    _, dropped = drop_restricted(people(), restriction(customer_email=Sensitivity.PII))
    assert dropped == ()


def test_an_allow_listed_column_reaches_gold() -> None:
    allowed = {
        "customer_document": ColumnPolicy(
            "customer_document",
            Sensitivity.SENSITIVE_PII,
            MaskingStrategy.HASH,
            allow_in_gold=True,
        )
    }
    assert drop_restricted(people(), allowed)[1] == ()


def test_natural_keys_are_never_dropped() -> None:
    policy = restriction(customer_id=Sensitivity.SENSITIVE_PII)
    assert restricted_columns(policy, keep=["customer_id"]) == []


# --- end to end through Silver -------------------------------------------


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def test_silver_masks_what_the_control_plane_says(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, Fixed(people()), tmp_path)
    apply_proposals(session, obj, scan(people()))

    silver_run = start_pipeline_run(session, "silver")
    result = build_silver(session, silver_run, obj, tmp_path)

    assert "customer_email" in result.masked_columns
    emails = read_silver(tmp_path, obj).column("customer_email").to_pylist()
    assert all("@" not in value for value in emails)


def test_bronze_keeps_the_raw_value(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    # Bronze is the faithful record of what the source sent (ADR-017).
    from lakehouse.ingest.bronze import read_bronze

    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, Fixed(people()), tmp_path)
    apply_proposals(session, obj, scan(people()))
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert "@" in read_bronze(tmp_path, obj).column("customer_email").to_pylist()[0]


def test_masking_does_not_break_deduplication(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    doubled = pa.concat_tables([people(), people()])
    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, Fixed(doubled), tmp_path)
    apply_proposals(session, obj, scan(people()))

    result = build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert result.rows_written == 3


# --- CLI ------------------------------------------------------------------


@pytest.fixture
def database_url(tmp_path: Path) -> str:
    """A file-backed control plane, so the CLI's engine sees the same rows."""
    return f"sqlite:///{tmp_path / 'control.db'}"


@pytest.fixture
def cli_session(database_url: str) -> Iterator[Session]:
    eng: Engine = create_engine(database_url)
    Base.metadata.create_all(eng)
    with Session(eng) as s:
        yield s


@pytest.fixture
def cli_obj(cli_session: Session) -> SourceObject:
    system = SourceSystem(name="seed", kind=SourceKind.FILE)
    cli_session.add(system)
    cli_session.commit()
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name="customers",
        target_path="bronze/seed/customers",
        load_strategy=LoadStrategy.FULL,
        primary_key_columns="customer_id",
        incremental_column="updated_at",
    )
    cli_session.add(o)
    cli_session.commit()
    return o


def test_the_cli_reports_without_writing(
    cli_session: Session,
    cli_obj: SourceObject,
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run = start_pipeline_run(cli_session, "bronze")
    load_full(cli_session, run, cli_obj, Fixed(people()), tmp_path)
    monkeypatch.setenv("DATABASE_URL", database_url)

    assert main(["--lake-root", str(tmp_path)]) == 0

    out = capsys.readouterr().out
    assert "customer_email" in out
    assert "nothing written" in out
    cli_session.expire_all()
    assert load_policies(cli_session, cli_obj) == {}


def test_the_cli_applies_when_asked(
    cli_session: Session,
    cli_obj: SourceObject,
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run = start_pipeline_run(cli_session, "bronze")
    load_full(cli_session, run, cli_obj, Fixed(people()), tmp_path)
    monkeypatch.setenv("DATABASE_URL", database_url)

    assert main(["--apply", "--object", "customers", "--lake-root", str(tmp_path)]) == 0

    assert "classification(s)" in capsys.readouterr().out
    cli_session.expire_all()
    assert load_policies(cli_session, cli_obj)["customer_email"].strategy is MaskingStrategy.HASH


def test_the_cli_skips_an_object_with_no_bronze_table(
    cli_obj: SourceObject,
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABASE_URL", database_url)

    assert main(["--lake-root", str(tmp_path)]) == 0
    assert "skipped" in capsys.readouterr().out


def test_the_cli_says_when_the_control_plane_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'empty.db'}")

    assert main(["--lake-root", str(tmp_path)]) == 2
    assert "--register" in capsys.readouterr().out


def test_a_proposal_knows_whether_it_masks() -> None:
    assert Proposal("a", Sensitivity.PII, MaskingStrategy.HASH, "").masks
    assert not Proposal("a", Sensitivity.INTERNAL, MaskingStrategy.NONE, "").masks


def test_quarantined_rows_are_masked_too(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    # Quarantine is the easiest place in a lakehouse to leak PII, because
    # it is the one table nobody thinks of as published (ADR-017).
    from lakehouse.metadata.models import DataQualityRule
    from lakehouse.quality import read_quarantine

    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.NOT_NULL,
            column_name="customer_phone",
            severity=Severity.QUARANTINE,
        )
    )
    session.commit()

    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(people()), tmp_path)
    apply_proposals(session, obj, scan(people()))
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    rejected = read_quarantine(tmp_path, obj)
    assert rejected.num_rows == 1
    assert "@" not in rejected.column("customer_email").to_pylist()[0]


def test_classifying_after_a_load_does_not_rewrite_history(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """A late classification leaves existing SCD2 versions unmasked.

    The rows carry the same `updated_at`, so SCD2 refuses to open a
    version that is not newer — correct behaviour, and a trap. Masking a
    table that has already loaded means a replay, not a re-run.
    """
    obj.scd2_enabled = True
    session.commit()
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(people()), tmp_path)
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    apply_proposals(session, obj, scan(people()))
    result = build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert result.scd2 is not None
    assert result.scd2.opened == 0
    assert "@" in read_silver(tmp_path, obj).column("customer_email").to_pylist()[0]
