"""The ops dashboard: freshness, volume, quality, quarantine, privacy.

This file renders and nothing else. Every number on the page comes from
`lakehouse.audit.snapshot()`, which is tested without Streamlit, so the
only thing that can be wrong here is layout.

    streamlit run src/lakehouse/dashboard/app.py

It reads the control plane through `credentials.database_url()`, so it
honours `DATABASE_URL_SECRET` exactly like the pipeline does.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import streamlit as st
from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from lakehouse.audit import Snapshot, snapshot
from lakehouse.config import Settings
from lakehouse.credentials import database_url

PAGE_TITLE = "Lakehouse operations"


@st.cache_resource  # type: ignore[untyped-decorator, unused-ignore]
def _engine(url: str) -> Engine:
    """One engine per URL for the life of the server, not per rerun."""
    return create_engine(url)


def _table(records: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(records)


def _minutes(value: float | None) -> str:
    if value is None:
        return "never"
    if value < 90:
        return f"{value:.0f} min"
    if value < 48 * 60:
        return f"{value / 60:.1f} h"
    return f"{value / 1440:.1f} d"


def describe(url: str) -> str:
    """The control-plane URL as it may be shown: never with its password."""
    return make_url(url).render_as_string(hide_password=True)


def render_headline(snap: Snapshot) -> None:
    last = snap.runs[0] if snap.runs else None
    cols = st.columns(5)
    cols[0].metric("Last run", last.status if last else "none")
    cols[1].metric("Stale runs", len(snap.stale))
    cols[2].metric("Freshness breaches", snap.freshness_breaches)
    cols[3].metric("Rows quarantined", f"{snap.rows_quarantined:,}")
    cols[4].metric("Unknown-member rows", f"{snap.unknown_total:,}")


def render_freshness(snap: Snapshot) -> None:
    st.subheader("Freshness")
    st.caption(
        "Time since each object's last completed Silver build, against its SLA. "
        "This is when the platform last loaded the object — not the age of the "
        "newest source row."
    )
    st.dataframe(
        _table(
            [
                {
                    "object": f.object_name,
                    "last loaded": f.last_loaded_at.isoformat(timespec="minutes")
                    if f.last_loaded_at
                    else "never",
                    "age": _minutes(f.age_minutes),
                    "SLA": f"{f.sla_minutes} min" if f.sla_minutes else "—",
                    "breached": "⚠️" if f.breached else "",
                    "owner": f.owner or "unowned",
                }
                for f in snap.freshness
            ]
        ),
        hide_index=True,
    )


def render_volume(snap: Snapshot) -> None:
    st.subheader("Volume and compute")
    if not snap.volume:
        st.info("No task runs in the window yet.")
        return
    points = _table(
        [
            {
                "day": v.day,
                "layer": v.layer,
                "rows written": v.rows_written,
                "compute seconds": v.compute_seconds,
            }
            for v in snap.volume
        ]
    )
    left, right = st.columns(2)
    with left:
        st.caption("Rows written per day, by layer")
        st.bar_chart(points, x="day", y="rows written", color="layer")
    with right:
        st.caption(
            "Compute seconds per day, by layer — the cost proxy. There is no "
            "billing data behind this platform, so no currency figure (ADR-019)."
        )
        st.bar_chart(points, x="day", y="compute seconds", color="layer")


def render_quality(snap: Snapshot) -> None:
    st.subheader("Data quality")
    left, right = st.columns(2)
    with left:
        st.caption("Pass rate per rule, across every evaluation")
        if snap.rules:
            st.dataframe(
                _table(
                    [
                        {
                            "rule": r.rule_id,
                            "type": r.rule_type,
                            "column": r.column or "—",
                            "severity": r.severity,
                            "evaluations": r.evaluations,
                            "pass rate": f"{r.pass_rate:.0%}",
                        }
                        for r in snap.rules
                    ]
                ),
                hide_index=True,
            )
        else:
            st.info("No rules configured — sync a contract with `python -m lakehouse.contracts`.")
    with right:
        st.caption(
            "Rows diverted by quarantine-severity rules. Duplicates and late "
            "rows are not counted here — they are not quarantine."
        )
        if snap.quarantine:
            st.dataframe(
                _table(
                    [
                        {
                            "object": q.object_name,
                            "rows quarantined": q.rows_quarantined,
                            "breaches": q.breaches,
                        }
                        for q in snap.quarantine
                    ]
                ),
                hide_index=True,
            )
        else:
            st.info("Nothing has been quarantined.")


def render_gold(snap: Snapshot) -> None:
    st.subheader("Gold integrity")
    st.caption(
        "Fact rows pointing at a dimension's unknown member (key 0) in the latest "
        "build. The rows are kept so totals stay right; this count is the gap."
    )
    if snap.unknown:
        st.dataframe(
            _table(
                [
                    {"fact": u.object_name, "unknown-member rows": u.rows, "run": u.run_id}
                    for u in snap.unknown
                ]
            ),
            hide_index=True,
        )
    else:
        st.info("No fact has been published to Gold yet.")


def render_privacy(snap: Snapshot) -> None:
    st.subheader("Privacy posture")
    st.caption(
        "Classified columns, how Silver masks them, and whether they reach Gold "
        "(ADR-017). Bronze holds raw values by design."
    )
    if snap.privacy:
        st.dataframe(
            _table(
                [
                    {
                        "object": c.object_name,
                        "column": c.column_name,
                        "sensitivity": c.sensitivity,
                        "masking": c.strategy,
                        "in Gold": "yes" if c.reaches_gold else "withheld",
                    }
                    for c in snap.privacy
                ]
            ),
            hide_index=True,
        )
    else:
        st.warning(
            "No columns are classified. Run `python -m lakehouse.privacy --apply` "
            "to propose classifications."
        )


def render_runs(snap: Snapshot) -> None:
    st.subheader("Recent runs")
    if snap.stale:
        st.warning(
            f"{len(snap.stale)} run(s) still marked running — a crashed process, or one in flight: "
            + ", ".join(r.run_id for r in snap.stale)
        )
    for run in snap.runs:
        duration = f"{run.duration_seconds:.0f}s" if run.duration_seconds is not None else "open"
        with st.expander(f"{run.pipeline_name} · {run.run_id} · {run.status} · {duration}"):
            st.dataframe(
                _table(
                    [
                        {
                            "layer": s.layer,
                            "tasks": s.tasks,
                            "read": s.rows_read,
                            "written": s.rows_written,
                            "rejected": s.rows_rejected,
                            "failed": s.failed,
                        }
                        for s in run.stages
                    ]
                ),
                hide_index=True,
            )


def render(snap: Snapshot, source: str) -> None:
    """Lay out the whole page from one snapshot."""
    st.title(PAGE_TITLE)
    st.caption(f"Control plane: {source} · read at {snap.taken_at.isoformat(timespec='seconds')}")
    if snap.empty:
        st.info(
            "The control plane is empty. Run `python -m lakehouse.pipeline --register`, "
            "then `python -m lakehouse.pipeline`."
        )
        return
    render_headline(snap)
    render_freshness(snap)
    render_volume(snap)
    render_quality(snap)
    render_gold(snap)
    render_privacy(snap)
    render_runs(snap)


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, page_icon="🏞️", layout="wide")
    url = database_url(Settings())
    with Session(_engine(url)) as session:
        snap = snapshot(session)
    # Never the password, even on a local page: screenshots travel.
    render(snap, describe(url))
    st.button("Refresh")


if __name__ == "__main__":
    main()
