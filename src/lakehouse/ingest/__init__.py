"""Ingestion into the medallion layers."""

from lakehouse.ingest.bronze import (
    LoadResult,
    ParquetSource,
    Source,
    finish_pipeline_run,
    load_full,
    read_bronze,
    start_pipeline_run,
)
from lakehouse.ingest.cdc import MergeResult, collapse_changes, load_cdc
from lakehouse.ingest.incremental import (
    FilteringSource,
    IncrementalResult,
    IncrementalSource,
    current_watermark,
    load_incremental,
)
from lakehouse.ingest.runner import ObjectOutcome, RunSummary, run_pipeline

__all__ = [
    "FilteringSource",
    "IncrementalResult",
    "IncrementalSource",
    "LoadResult",
    "MergeResult",
    "ObjectOutcome",
    "ParquetSource",
    "RunSummary",
    "Source",
    "collapse_changes",
    "current_watermark",
    "finish_pipeline_run",
    "load_cdc",
    "load_full",
    "load_incremental",
    "read_bronze",
    "run_pipeline",
    "start_pipeline_run",
]
