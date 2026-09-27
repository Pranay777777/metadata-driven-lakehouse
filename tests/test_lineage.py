"""Tests for lineage emission.

Nothing here touches the network: a capturing emitter stands in for the
HTTP client, which is also how the pipeline behaves in CI.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from openlineage.client.event_v2 import RunEvent, RunState
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

import lakehouse.lineage as lineage_module
from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.lineage import (
    DatasetRef,
    Lineage,
    identity_lineage,
    lake_dataset,
    renamed_lineage,
    reset_default,
    source_dataset,
)
from lakehouse.metadata.enums import GoldRole, LoadStrategy, SourceKind
from lakehouse.metadata.models import Base, SourceObject, SourceSystem
from lakehouse.transform.gold import build_gold
from lakehouse.transform.silver import build_silver


def error_message(event: RunEvent) -> str:
    """The error text off a FAIL event, narrowed for the type checker."""
    facets = event.run.facets or {}
    facet: Any = facets["errorMessage"]
    return str(facet.message)


def output_facets(event: RunEvent) -> dict[str, Any]:
    outputs = event.outputs or []
    assert outputs, "expected the event to carry an output dataset"
    return dict(outputs[0].facets or {})


def column_fields(event: RunEvent) -> dict[str, Any]:
    return dict(output_facets(event)["columnLineage"].fields)


def schema_fields(event: RunEvent) -> list[Any]:
    return list(output_facets(event)["schema"].fields)


class Capture:
    """Stands in for the HTTP client."""

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)

    def of_type(self, state: RunState) -> list[RunEvent]:
        return [e for e in self.events if e.eventType == state]


@pytest.fixture
def captured() -> Iterator[Capture]:
    """Install a capturing emitter as the process-wide default."""
    capture = Capture()
    lineage_module._default = Lineage(capture, "lakehouse")
    yield capture
    reset_default()


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
        primary_key_columns="CustomerID",
        incremental_column="updated_at",
        gold_role=GoldRole.DIMENSION,
    )
    session.add(o)
    session.commit()
    return o


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


class Broken:
    def read(self, obj: SourceObject) -> pa.Table:
        raise RuntimeError("source unavailable")


TABLE = pa.table({"CustomerID": ["c1"], "City": ["rio"], "updated_at": [1]})


# --------------------------------------------------------------------------
# The emitter
# --------------------------------------------------------------------------


def test_disabled_lineage_creates_no_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """CI must never need Marquez."""
    monkeypatch.setenv("OPENLINEAGE_ENABLED", "false")
    reset_default()
    assert Lineage.from_settings().enabled is False


def test_a_disabled_emitter_sends_nothing() -> None:
    quiet = Lineage(None)
    with quiet.track("job", "run"):
        pass  # no emitter, so nothing to assert beyond not raising


def test_a_tracked_job_emits_start_then_complete() -> None:
    capture = Capture()
    with Lineage(capture).track("job", "run"):
        pass

    assert [e.eventType for e in capture.events] == [RunState.START, RunState.COMPLETE]


def test_both_events_share_one_run_id() -> None:
    capture = Capture()
    with Lineage(capture).track("job", "run"):
        pass
    assert capture.events[0].run.runId == capture.events[1].run.runId


def test_a_failure_emits_fail_and_re_raises() -> None:
    capture = Capture()
    with pytest.raises(ValueError, match="boom"), Lineage(capture).track("job", "run"):
        raise ValueError("boom")

    assert capture.events[-1].eventType == RunState.FAIL
    assert "boom" in error_message(capture.events[-1])


def test_emission_failure_never_breaks_the_caller() -> None:
    """Observability that can take down the pipeline is a liability."""

    class Exploding:
        def emit(self, event: RunEvent) -> None:
            raise ConnectionError("marquez is down")

    with Lineage(Exploding()).track("job", "run") as rec:
        rec.writes(lake_dataset("bronze/seed/customers"))
    # reaching here without raising is the assertion


def test_a_bad_url_degrades_to_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENLINEAGE_ENABLED", "true")
    monkeypatch.setenv("OPENLINEAGE_URL", "not-a-url")
    reset_default()
    assert Lineage.from_settings().enabled is False


def test_the_default_emitter_is_built_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENLINEAGE_ENABLED", "false")
    reset_default()
    assert lineage_module.default_lineage() is lineage_module.default_lineage()
    reset_default()


# --------------------------------------------------------------------------
# Facets
# --------------------------------------------------------------------------


def test_the_schema_facet_carries_every_column() -> None:
    capture = Capture()
    with Lineage(capture).track("job", "run") as rec:
        rec.writes(lake_dataset("bronze/seed/customers", TABLE.schema))

    fields = schema_fields(capture.events[-1])
    assert [f.name for f in fields] == ["CustomerID", "City", "updated_at"]
    assert fields[0].type == "string"


def test_identity_lineage_maps_like_to_like() -> None:
    src = DatasetRef("source://seed", "public.customers")
    mapping = identity_lineage(src, ["a", "b"])
    assert mapping["a"] == [("source://seed", "public.customers", "a")]


def test_renamed_lineage_maps_across_names() -> None:
    src = lake_dataset("bronze/seed/customers")
    mapping = renamed_lineage(src, {"customer_id": "CustomerID"})
    assert mapping["customer_id"] == [("delta", "bronze/seed/customers", "CustomerID")]


def test_a_dataset_without_a_schema_emits_no_schema_facet() -> None:
    capture = Capture()
    with Lineage(capture).track("job", "run") as rec:
        rec.writes(lake_dataset("bronze/seed/customers"))
    assert output_facets(capture.events[-1]) == {}


def test_source_datasets_are_namespaced_by_system(session: Session, obj: SourceObject) -> None:
    ref = source_dataset(obj)
    assert ref.namespace == "source://seed"
    assert ref.name == "public.customers"


# --------------------------------------------------------------------------
# Wired into the pipeline
# --------------------------------------------------------------------------


def test_a_bronze_load_emits_source_to_delta(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)

    complete = captured.of_type(RunState.COMPLETE)[-1]
    inputs = complete.inputs or []
    outputs = complete.outputs or []
    assert complete.job.name == "bronze.customers"
    assert inputs[0].namespace == "source://seed"
    assert outputs[0].name == "bronze/seed/customers"


def test_bronze_records_column_level_lineage(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)

    fields = column_fields(captured.of_type(RunState.COMPLETE)[-1])
    assert set(fields) == {"CustomerID", "City", "updated_at"}
    assert fields["City"].inputFields[0].field == "City"


def test_provenance_columns_claim_no_upstream_field(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    """Bronze generates them, so attributing them upstream would be a lie."""
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)

    fields = column_fields(captured.of_type(RunState.COMPLETE)[-1])
    assert "_ingested_at" not in fields


def test_a_failed_load_emits_fail(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    with pytest.raises(RuntimeError):
        load_full(session, start_pipeline_run(session, "b"), obj, Broken(), tmp_path)

    assert captured.of_type(RunState.FAIL)
    assert not captured.of_type(RunState.COMPLETE)


def test_silver_records_the_conforming_rename(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)
    build_silver(session, start_pipeline_run(session, "s"), obj, tmp_path)

    silver = [e for e in captured.of_type(RunState.COMPLETE) if e.job.name.startswith("silver")]
    fields = column_fields(silver[-1])
    assert fields["customer_id"].inputFields[0].field == "CustomerID", (
        "the conformed name must point back at the original"
    )


def test_gold_attributes_the_surrogate_to_the_natural_key(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)
    build_silver(session, start_pipeline_run(session, "s"), obj, tmp_path)
    build_gold(session, start_pipeline_run(session, "g"), obj, tmp_path)

    gold = [e for e in captured.of_type(RunState.COMPLETE) if e.job.name.startswith("gold")]
    fields = column_fields(gold[-1])
    assert fields["customers_key"].inputFields[0].field == "customer_id"


def test_the_whole_pipeline_produces_a_connected_graph(
    session: Session, obj: SourceObject, tmp_path: Path, captured: Capture
) -> None:
    """Every layer's output must be the next layer's input, by name."""
    load_full(session, start_pipeline_run(session, "b"), obj, Fixed(TABLE), tmp_path)
    build_silver(session, start_pipeline_run(session, "s"), obj, tmp_path)
    build_gold(session, start_pipeline_run(session, "g"), obj, tmp_path)

    edges = {
        ((e.inputs or [])[0].name, (e.outputs or [])[0].name)
        for e in captured.of_type(RunState.COMPLETE)
        if e.inputs and e.outputs
    }
    assert ("public.customers", "bronze/seed/customers") in edges
    assert ("bronze/seed/customers", "silver/seed/customers") in edges
    assert ("silver/seed/customers", "gold/dim_customers") in edges
