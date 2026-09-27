"""Dagster assets, generated from the catalog rather than written out.

`lakehouse.pipeline` runs the layers in a fixed order and stops at the
first thing it cannot do. That is fine for a demo and wrong for
operations, which needs three things it cannot provide: a dependency
graph the scheduler understands, retries and partial reruns of only
what failed, and a schedule that does not depend on someone being at a
terminal.

The assets here are **built from the same catalog the control plane is
registered from**, not hand-written. Thirty assets typed out by hand
would be thirty places to forget an edge when a source is added; here,
adding a `CatalogEntry` produces its Bronze, Silver and Gold assets
with their dependencies already correct.

The dependency edges matter and are not decoration:

- Silver depends on its own Bronze.
- A Gold **dimension** depends on its own Silver.
- A Gold **fact** depends on its own Silver *and on every dimension it
  references*, because resolving a surrogate reads the published
  dimension. Getting this wrong produces facts full of unknown-member
  keys, which looks like a data problem rather than an ordering bug.

That last edge is the reason `gold_reference` exists in the control
plane, and the reason this is generated code rather than a list.
"""

# No `from __future__ import annotations` here, deliberately: Dagster
# inspects the real annotation objects on an asset's parameters to work
# out which are resources, and string annotations defeat that.
from collections.abc import Iterator
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.ingest.bronze import ParquetSource, start_pipeline_run
from lakehouse.ingest.runner import run_pipeline
from lakehouse.metadata.models import SourceObject
from lakehouse.pipeline import CATALOG, CatalogEntry
from lakehouse.transform.gold import build_gold
from lakehouse.transform.silver import build_silver

BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"


class LakehouseResource:
    """Everything an asset needs to touch the outside world.

    A plain class rather than a Dagster `ConfigurableResource`: the only
    configuration is already in `Settings`, and a second configuration
    system layered on the first is how the two end up disagreeing.
    Assets therefore declare `required_resource_keys` and read it off
    the context, which is how Dagster passes a resource it does not own
    the type of.
    """

    def __init__(self, settings: Settings | None = None, data_dir: Path | None = None) -> None:
        self.settings = settings or Settings()
        self.data_dir = data_dir or Path("data/generated")
        self.lake_root = Path(self.settings.lake_root)
        self._engine = create_engine(self.settings.database_url)

    def session(self) -> Session:
        return Session(self._engine)

    def source(self) -> ParquetSource:
        return ParquetSource(self.data_dir)


def bronze_key(name: str) -> AssetKey:
    return AssetKey([BRONZE_PREFIX, name])


def silver_key(name: str) -> AssetKey:
    return AssetKey([SILVER_PREFIX, name])


def gold_key(name: str) -> AssetKey:
    return AssetKey([GOLD_PREFIX, name])


def gold_dependencies(entry: CatalogEntry) -> list[AssetKey]:
    """What a Gold asset must wait for.

    A dimension needs its own Silver. A fact needs its own Silver and
    every dimension it resolves surrogates against — read straight off
    the catalog's references, so a new edge in the star is a new edge
    in the schedule.
    """
    deps = [silver_key(entry.name)]
    deps.extend(gold_key(dimension) for dimension in sorted(set(entry.references.values())))
    return deps


def _find(name: str) -> CatalogEntry:
    return next(e for e in CATALOG if e.name == name)


def build_bronze_asset(entry: CatalogEntry) -> AssetsDefinition:
    @asset(
        key=bronze_key(entry.name),
        group_name=BRONZE_PREFIX,
        description=f"Raw {entry.name}, loaded {entry.strategy} with provenance.",
        compute_kind="delta",
        required_resource_keys={"lakehouse"},
    )
    def _bronze(context: AssetExecutionContext) -> None:
        lakehouse: LakehouseResource = context.resources.lakehouse
        with lakehouse.session() as session:
            summary = run_pipeline(
                session,
                lakehouse.source(),
                lakehouse.lake_root,
                object_names=[entry.name],
            )
            failed = [o for o in summary.outcomes if o.status == "failed"]
            if failed:
                raise RuntimeError(f"{entry.name}: {failed[0].error}")
            context.log.info("loaded %s in run %s", entry.name, summary.run_id)

    return _bronze


def build_silver_asset(entry: CatalogEntry) -> AssetsDefinition:
    @asset(
        key=silver_key(entry.name),
        deps=[bronze_key(entry.name)],
        group_name=SILVER_PREFIX,
        description=f"Deduplicated, conformed {entry.name}"
        + (" with SCD2 history." if entry.scd2 else "."),
        compute_kind="delta",
        required_resource_keys={"lakehouse"},
    )
    def _silver(context: AssetExecutionContext) -> None:
        lakehouse: LakehouseResource = context.resources.lakehouse
        with lakehouse.session() as session:
            obj = _object(session, entry.name)
            run = start_pipeline_run(session, SILVER_PREFIX, triggered_by="dagster")
            result = build_silver(session, run, obj, lakehouse.lake_root)
            context.log.info("%s: %s rows written", entry.name, result.rows_written)

    return _silver


def build_gold_asset(entry: CatalogEntry) -> AssetsDefinition:
    @asset(
        key=gold_key(entry.name),
        deps=gold_dependencies(entry),
        group_name=GOLD_PREFIX,
        description=f"Published {entry.gold_role} for {entry.name}.",
        compute_kind="delta",
        required_resource_keys={"lakehouse"},
    )
    def _gold(context: AssetExecutionContext) -> None:
        lakehouse: LakehouseResource = context.resources.lakehouse
        with lakehouse.session() as session:
            obj = _object(session, entry.name)
            run = start_pipeline_run(session, GOLD_PREFIX, triggered_by="dagster")
            result = build_gold(session, run, obj, lakehouse.lake_root)
            context.log.info("%s: %s rows published", entry.name, result.rows_written)

    return _gold


def _object(session: Session, name: str) -> SourceObject:
    obj = session.scalars(select(SourceObject).where(SourceObject.object_name == name)).first()
    if obj is None:
        raise ValueError(
            f"'{name}' is not registered — run 'python -m lakehouse.pipeline --register'"
        )
    return obj


def all_assets() -> Iterator[AssetsDefinition]:
    """Every asset the catalog implies, in layer order."""
    for entry in CATALOG:
        yield build_bronze_asset(entry)
    for entry in CATALOG:
        yield build_silver_asset(entry)
    for entry in CATALOG:
        if entry.gold_role:
            yield build_gold_asset(entry)


# No explicit selection: the default is every asset, and passing "*"
# routes through a beta selection parser for no gain.
daily_refresh = define_asset_job("daily_refresh")

schedule = ScheduleDefinition(
    job=daily_refresh,
    cron_schedule="0 2 * * *",
    # Off unless deliberately switched on. A schedule that starts itself
    # the moment someone opens the UI is a surprise, not a feature.
    default_status=DefaultScheduleStatus.STOPPED,
)


def build_definitions(resource: LakehouseResource | None = None) -> Definitions:
    """Assemble the Dagster definitions, optionally with a test resource."""
    return Definitions(
        assets=list(all_assets()),
        jobs=[daily_refresh],
        schedules=[schedule],
        resources={"lakehouse": resource or LakehouseResource()},
    )


defs = build_definitions()
"""What `dagster dev -m lakehouse.orchestration` loads."""
