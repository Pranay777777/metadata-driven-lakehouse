"""Tests for table maintenance."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.maintenance import (
    MIN_RETENTION_HOURS,
    MaintenanceError,
    compact,
    delta_tables,
    file_stats,
    is_quarantine,
    main,
    vacuum,
    zorder,
)


def fragmented(path: Path, batches: int = 12) -> Path:
    """A table built from many small appends, as incremental loads leave it."""
    for batch in range(batches):
        write_deltalake(
            str(path),
            pa.table(
                {
                    "customer_id": pa.array([batch * 10 + i for i in range(10)], type=pa.int64()),
                    "amount": pa.array([float(i) for i in range(10)]),
                }
            ),
            mode="append",
        )
    return path


def rows(path: Path) -> list[tuple[object, ...]]:
    table = DeltaTable(str(path)).to_pyarrow_table()
    columns = [table.column(c).to_pylist() for c in sorted(table.column_names)]
    return sorted(tuple(c[i] for c in columns) for i in range(table.num_rows))


# --------------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------------


def test_file_stats_counts_every_append(tmp_path: Path) -> None:
    stats = file_stats(fragmented(tmp_path / "t", batches=5))
    assert stats.files == 5
    assert stats.total_bytes > 0
    assert stats.small_files == 5


def test_stats_describe_themselves(tmp_path: Path) -> None:
    assert "5 files" in file_stats(fragmented(tmp_path / "t", batches=5)).describe()


def test_an_empty_stats_average_is_zero() -> None:
    from lakehouse.maintenance import FileStats

    assert FileStats(files=0, total_bytes=0, small_files=0).average_bytes == 0.0


# --------------------------------------------------------------------------
# Compaction
# --------------------------------------------------------------------------


def test_compaction_merges_small_files(tmp_path: Path) -> None:
    result = compact(fragmented(tmp_path / "t"))
    assert result.before.files == 12
    assert result.after.files < result.before.files
    assert result.files_removed > 0


def test_compaction_changes_layout_not_data(tmp_path: Path) -> None:
    """The one property that makes it always safe to run."""
    path = fragmented(tmp_path / "t")
    before = rows(path)
    compact(path)
    assert rows(path) == before


def test_compaction_is_idempotent(tmp_path: Path) -> None:
    path = fragmented(tmp_path / "t")
    compact(path)
    settled = file_stats(path).files
    compact(path)
    assert file_stats(path).files == settled


# --------------------------------------------------------------------------
# Z-order
# --------------------------------------------------------------------------


def test_zorder_preserves_data(tmp_path: Path) -> None:
    path = fragmented(tmp_path / "t")
    before = rows(path)
    zorder(path, ["customer_id"])
    assert rows(path) == before


def test_zorder_refuses_an_unknown_column(tmp_path: Path) -> None:
    """A typo would otherwise rewrite every file for no benefit."""
    with pytest.raises(MaintenanceError, match="not in the table"):
        zorder(fragmented(tmp_path / "t"), ["custmer_id"])


def test_zorder_records_the_operation(tmp_path: Path) -> None:
    result = zorder(fragmented(tmp_path / "t"), ["customer_id"])
    assert result.operation == "zorder"


# --------------------------------------------------------------------------
# Vacuum
# --------------------------------------------------------------------------


def test_vacuum_defaults_to_a_dry_run(tmp_path: Path) -> None:
    """The only destructive operation must not be destructive by default."""
    path = fragmented(tmp_path / "t")
    compact(path)
    files_on_disk = sorted(path.glob("*.parquet"))

    vacuum(path)
    assert sorted(path.glob("*.parquet")) == files_on_disk


def test_vacuum_refuses_a_quarantine_table(tmp_path: Path) -> None:
    path = fragmented(tmp_path / "quarantine" / "seed" / "customers")
    with pytest.raises(MaintenanceError, match="append-only evidence"):
        vacuum(path)


def test_vacuum_refuses_a_retention_below_the_floor(tmp_path: Path) -> None:
    """Below a week breaks time travel exactly when replay needs it."""
    with pytest.raises(MaintenanceError, match="floor"):
        vacuum(fragmented(tmp_path / "t"), retention_hours=MIN_RETENTION_HOURS - 1)


def test_vacuum_keeps_files_inside_the_retention_window(tmp_path: Path) -> None:
    """Freshly superseded files are younger than a week, so they stay."""
    path = fragmented(tmp_path / "t")
    compact(path)
    assert vacuum(path, dry_run=False) == []

    # The point of the retention floor: history is still readable.
    table = DeltaTable(str(path))
    table.load_as_version(0)
    assert table.version() == 0
    assert table.to_pyarrow_table().num_rows == 10


def test_quarantine_is_recognised_anywhere_in_the_path(tmp_path: Path) -> None:
    assert is_quarantine(tmp_path / "lake" / "quarantine" / "seed" / "x")
    assert not is_quarantine(tmp_path / "lake" / "silver" / "seed" / "x")


# --------------------------------------------------------------------------
# Discovery and CLI
# --------------------------------------------------------------------------


def test_delta_tables_finds_every_table(tmp_path: Path) -> None:
    fragmented(tmp_path / "silver" / "a", batches=1)
    fragmented(tmp_path / "silver" / "b", batches=1)
    assert len(delta_tables(tmp_path)) == 2


def test_the_cli_requires_an_operation(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        main(["--lake-root", str(tmp_path)])


def test_the_cli_reports_a_missing_lake(tmp_path: Path) -> None:
    assert main(["--lake-root", str(tmp_path / "nope"), "--compact"]) == 2


def test_the_cli_compacts_a_layer(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    fragmented(tmp_path / "silver" / "seed" / "customers")
    assert main(["--lake-root", str(tmp_path), "--layer", "silver", "--compact"]) == 0
    assert "compact" in capsys.readouterr().out


def test_the_cli_skips_zorder_where_the_column_is_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fragmented(tmp_path / "silver" / "seed" / "customers")
    main(["--lake-root", str(tmp_path), "--zorder-by", "nope"])
    assert "skipped" in capsys.readouterr().out


def test_the_cli_zorders_where_the_column_exists(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fragmented(tmp_path / "silver" / "seed" / "customers")
    main(["--lake-root", str(tmp_path), "--zorder-by", "customer_id"])
    assert "by customer_id" in capsys.readouterr().out


def test_the_cli_vacuum_is_a_dry_run_unless_applied(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fragmented(tmp_path / "silver" / "seed" / "customers")
    main(["--lake-root", str(tmp_path), "--vacuum"])
    assert "dry run" in capsys.readouterr().out


def test_the_cli_never_touches_quarantine(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fragmented(tmp_path / "quarantine" / "seed" / "customers")
    fragmented(tmp_path / "silver" / "seed" / "customers")
    main(["--lake-root", str(tmp_path), "--vacuum", "--apply"])
    assert "quarantine" not in capsys.readouterr().out
