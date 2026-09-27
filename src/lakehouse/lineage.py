"""Emitting OpenLineage events.

The control plane already records what ran and how many rows moved.
What it cannot answer is the question anyone actually asks during an
incident: *this number is wrong, where did it come from?* That needs
edges between datasets, not rows in a task table, and it needs them in
a form a tool can draw.

OpenLineage is that form, and Marquez draws it.

Three rules shape this module.

**Lineage never breaks the pipeline.** Every emission is wrapped and
failures are logged, not raised. Observability that can take down the
thing it observes is a liability: a Marquez container being down at 3am
must not stop the load.

**Disabled means silent, not buffered.** With `OPENLINEAGE_ENABLED`
false there is no client, no socket and no queue. The test suite and CI
run with lineage off and never touch the network.

**Column lineage is recorded where it is known.** A graph of tables
tells you a column came from somewhere in `orders`. A graph of columns
tells you which field, which is the difference between narrowing an
incident to a table and narrowing it to a line of code. Each layer
knows its own mapping — Bronze copies, Silver renames, Gold derives a
surrogate from a natural key — so each layer declares it.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

import pyarrow as pa
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    RunFacet,
    column_lineage_dataset,
    error_message_run,
    schema_dataset,
)
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.uuid import generate_new_uuid

from lakehouse.config import Settings
from lakehouse.metadata.models import SourceObject

logger = logging.getLogger(__name__)

LAKE_NAMESPACE = "delta"
"""Namespace for datasets this platform owns."""

PRODUCER = "https://github.com/Pranay777777/metadata-driven-lakehouse"


class Emitter(Protocol):
    """Anything that can accept a RunEvent. Lets tests capture instead of send."""

    def emit(self, event: RunEvent) -> None: ...


@dataclass(frozen=True)
class DatasetRef:
    """One dataset in the graph, with whatever is known about it."""

    namespace: str
    name: str
    schema: pa.Schema | None = None
    column_lineage: dict[str, list[tuple[str, str, str]]] | None = None
    """Output column -> [(namespace, dataset, input column)]."""


def source_dataset(obj: SourceObject) -> DatasetRef:
    """The upstream system's object, outside this platform."""
    system = obj.system.name if obj.system else "unknown"
    return DatasetRef(namespace=f"source://{system}", name=f"{obj.schema_name}.{obj.object_name}")


def lake_dataset(path: str, schema: pa.Schema | None = None, **kwargs: object) -> DatasetRef:
    """A Delta table this platform writes."""
    return DatasetRef(namespace=LAKE_NAMESPACE, name=path, schema=schema, **kwargs)  # type: ignore[arg-type]


def identity_lineage(
    source: DatasetRef, columns: list[str]
) -> dict[str, list[tuple[str, str, str]]]:
    """Each column comes from the identically named column upstream.

    Correct for Bronze, which copies faithfully and adds provenance.
    Provenance columns are generated here and have no upstream field,
    so they are simply absent from the mapping rather than mapped to
    something invented.
    """
    return {c: [(source.namespace, source.name, c)] for c in columns}


def renamed_lineage(
    source: DatasetRef, mapping: dict[str, str]
) -> dict[str, list[tuple[str, str, str]]]:
    """Output column -> single input column under a different name."""
    return {out: [(source.namespace, source.name, src)] for out, src in mapping.items()}


def _schema_facet(schema: pa.Schema) -> schema_dataset.SchemaDatasetFacet:
    return schema_dataset.SchemaDatasetFacet(
        fields=[
            schema_dataset.SchemaDatasetFacetFields(name=f.name, type=str(f.type)) for f in schema
        ]
    )


def _column_lineage_facet(
    mapping: dict[str, list[tuple[str, str, str]]],
) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    return column_lineage_dataset.ColumnLineageDatasetFacet(
        fields={
            output: column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(namespace=ns, name=name, field=col)
                    for ns, name, col in inputs
                ]
            )
            for output, inputs in mapping.items()
        }
    )


def _facets(ref: DatasetRef) -> dict[str, object]:
    facets: dict[str, object] = {}
    if ref.schema is not None:
        facets["schema"] = _schema_facet(ref.schema)
    if ref.column_lineage:
        facets["columnLineage"] = _column_lineage_facet(ref.column_lineage)
    return facets


@dataclass
class Recording:
    """Collects the datasets a job touched, for emission when it ends."""

    inputs: list[DatasetRef] = field(default_factory=list)
    outputs: list[DatasetRef] = field(default_factory=list)

    def reads(self, ref: DatasetRef) -> None:
        self.inputs.append(ref)

    def writes(self, ref: DatasetRef) -> None:
        self.outputs.append(ref)


class Lineage:
    """Emits OpenLineage events, or does nothing at all."""

    def __init__(self, emitter: Emitter | None, namespace: str = "lakehouse") -> None:
        self.emitter = emitter
        self.namespace = namespace

    @property
    def enabled(self) -> bool:
        return self.emitter is not None

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> Lineage:
        """Build from configuration. Disabled produces no client at all."""
        resolved = settings or Settings()
        if not resolved.openlineage_enabled:
            return cls(None, resolved.openlineage_namespace)
        try:
            # Explicit transport rather than OpenLineageClient(url=...),
            # which is deprecated. The short timeout matters: a hung
            # Marquez must not become a hung pipeline.
            client = OpenLineageClient(
                transport=HttpTransport(HttpConfig(url=resolved.openlineage_url, timeout=5.0))
            )
        except Exception:
            logger.warning("could not create the lineage client; continuing without lineage")
            return cls(None, resolved.openlineage_namespace)
        return cls(client, resolved.openlineage_namespace)

    def _send(self, event: RunEvent) -> None:
        """Emission is best-effort by design. See the module docstring."""
        if self.emitter is None:
            return
        try:
            self.emitter.emit(event)
        except Exception as exc:
            logger.warning("lineage emission failed: %s: %s", type(exc).__name__, exc)

    def _event(
        self,
        state: RunState,
        job_name: str,
        run_uuid: str,
        inputs: list[DatasetRef],
        outputs: list[DatasetRef],
        error: str | None = None,
    ) -> RunEvent:
        run_facets: dict[str, RunFacet] = {}
        if error is not None:
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error, programmingLanguage="PYTHON"
            )
        return RunEvent(
            eventType=state,
            eventTime=datetime.now(UTC).isoformat(),
            run=Run(runId=run_uuid, facets=run_facets),
            job=Job(namespace=self.namespace, name=job_name),
            inputs=[
                InputDataset(namespace=d.namespace, name=d.name, facets=_facets(d))  # type: ignore[arg-type]
                for d in inputs
            ],
            outputs=[
                OutputDataset(namespace=d.namespace, name=d.name, facets=_facets(d))  # type: ignore[arg-type]
                for d in outputs
            ],
            producer=PRODUCER,
        )

    def begin(self, job_name: str) -> str:
        """Emit START and return the run UUID that FAIL or COMPLETE must reuse."""
        run_uuid = str(generate_new_uuid())
        self._send(self._event(RunState.START, job_name, run_uuid, [], []))
        return run_uuid

    def finish(
        self,
        job_name: str,
        run_uuid: str,
        recording: Recording | None = None,
        error: str | None = None,
    ) -> None:
        """Emit COMPLETE, or FAIL when `error` is given."""
        done = recording or Recording()
        state = RunState.FAIL if error else RunState.COMPLETE
        self._send(self._event(state, job_name, run_uuid, done.inputs, done.outputs, error=error))

    @contextmanager
    def track(self, job_name: str, run_id: str) -> Iterator[Recording]:
        """Emit START on entry and COMPLETE or FAIL on exit.

        The caller records datasets on the yielded object as it learns
        them, which is why START carries none: a job does not know its
        output schema until it has produced one.

        The run UUID is derived from the pipeline run id and the job
        name, so the two events of one task share an id and a rerun of
        the same task produces a new one.
        """
        recording = Recording()
        run_uuid = self.begin(job_name)
        try:
            yield recording
        except Exception as exc:
            self.finish(job_name, run_uuid, recording, error=f"{type(exc).__name__}: {exc}")
            raise
        self.finish(job_name, run_uuid, recording)


_default: Lineage | None = None


def default_lineage() -> Lineage:
    """The process-wide emitter, built once from settings."""
    global _default
    if _default is None:
        _default = Lineage.from_settings()
    return _default


def reset_default() -> None:
    """Drop the cached emitter. For tests and for settings changes."""
    global _default
    _default = None
