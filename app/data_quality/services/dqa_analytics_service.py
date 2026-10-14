"""
DQA Analytics Cache Service

Stores the computed results of all four DQA indicators (RRS, ICS, ICI, AID)
as a single snapshot document in `dqa_analytics` so the frontend can load
them from a pre-computed cache instead of running live AQL on every page visit.
"""

from datetime import datetime, timedelta
from typing import Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_groups

_SNAPSHOT_KEY = "snapshot"
_CONFIG_KEY = "dqa_analytics_schedule"

# A run is marked 'running' before the work starts and 'completed'/'failed'
# after it. Nothing writes that final state if the worker dies mid-run - an
# OOM kill, a container restart, a redeploy - so the document stays 'running'
# for ever. The UI polls while it says running and refuses to start a new run
# while it does, so a single dead worker leaves the page spinning with no way
# out of it from the browser.
#
# Anything still 'running' after this long is therefore treated as abandoned.
# A healthy recompute measures 13-25 seconds now that ICS is chunked, so ten
# minutes is roughly thirty times a normal run - long enough to absorb a much
# larger dataset, short enough that a stuck page recovers while someone is
# still looking at it. It was thirty minutes, which is a long time to sit in
# front of a spinner.
_STALE_RUN_AFTER = timedelta(minutes=10)


# ── Snapshot helpers ──────────────────────────────────────────────────────────

def _upsert_doc_sync(db: StandardDatabase, collection: str, doc: dict) -> None:
    col = db.collection(collection)
    if col.has(doc["_key"]):
        col.update(doc, merge=True)
    else:
        col.insert(doc)


def _is_stale_run(snapshot: dict) -> bool:
    """True when a 'running' snapshot has been running implausibly long."""
    if not snapshot or snapshot.get("status") != "running":
        return False
    started = snapshot.get("computed_at")
    if not started:
        return True
    try:
        # Stored as an ISO string with a trailing Z.
        began = datetime.fromisoformat(str(started).rstrip("Z"))
    except ValueError:
        return True
    return datetime.utcnow() - began > _STALE_RUN_AFTER


def _mark_stale_as_failed(snapshot: Optional[dict]) -> Optional[dict]:
    if _is_stale_run(snapshot):
        # Report it as failed rather than leaving the caller to poll for ever.
        # Reported, not rewritten: if the worker is somehow still alive it will
        # finish and overwrite this itself, and a read should not destroy state.
        snapshot = dict(snapshot)
        snapshot["status"] = "failed"
        snapshot["error"] = (
            "The previous computation did not finish - the worker most likely "
            "stopped part-way. Start it again."
        )
    return snapshot


def _pool_stat_blocks(blocks: list) -> dict:
    """Combines several per-location _stat_block dicts (general_dqa.py) into
    one, exactly for avg/stddev/count/min/max (standard pooled-mean/pooled-
    variance formula for disjoint groups), approximately for p50 (a median
    cannot be exactly recombined from per-group medians without the raw
    values - a count-weighted average of the group medians is used instead,
    flagged via `p50_approximate`). The common case - a user restricted to a
    single location value - never reaches the approximate branch.
    """
    present = [b for b in blocks if b and b.get("count")]
    if not present:
        return {"avg": None, "min_v": None, "max_v": None, "stddev": None, "p50": None, "count": 0, "p50_approximate": False}

    combined_n = sum(b["count"] for b in present)
    combined_mean = sum(b["count"] * b["avg"] for b in present) / combined_n
    combined_var = sum(
        b["count"] * ((b["stddev"] ** 2) + (b["avg"] - combined_mean) ** 2)
        for b in present
    ) / combined_n

    result = {
        "avg": combined_mean,
        "min_v": min(b["min_v"] for b in present),
        "max_v": max(b["max_v"] for b in present),
        "stddev": combined_var ** 0.5,
        "count": combined_n,
    }
    if len(present) == 1:
        result["p50"] = present[0]["p50"]
        result["p50_approximate"] = False
    else:
        result["p50"] = sum(b["count"] * b["p50"] for b in present) / combined_n
        result["p50_approximate"] = True
    return result


def _pool_breakdowns(docs: list, indicator: str) -> dict:
    """Pools one indicator's (rrs/ics/aid) overall/by_age_group/
    by_gender_adult breakdown across several location docs."""
    def cell(path):
        blocks = []
        for doc in docs:
            block = doc.get(indicator) or {}
            for key in path:
                block = (block or {}).get(key, {})
            blocks.append(block)
        return _pool_stat_blocks(blocks)

    return {
        "overall": cell(["overall"]),
        "by_age_group": {
            "adults": cell(["by_age_group", "adults"]),
            "children": cell(["by_age_group", "children"]),
            "neonates": cell(["by_age_group", "neonates"]),
        },
        "by_gender_adult": {
            "male_adults": cell(["by_gender_adult", "male_adults"]),
            "female_adults": cell(["by_gender_adult", "female_adults"]),
        },
    }


def _pool_ici(docs: list) -> dict:
    """Pools the ICI block across several location docs - exact (count
    sums + re-derived percentage), no median involved."""
    totals: dict = {}
    checks_applied = []
    for doc in docs:
        ici = doc.get("ici") or {}
        checks_applied = checks_applied or ici.get("checks_applied", [])
        for row in ici.get("interviewers", []):
            entry = totals.setdefault(row["interviewer"], {"total": 0, "passed": 0})
            entry["total"] += row["total"]
            entry["passed"] += row["passed"]

    overall_total = sum(e["total"] for e in totals.values())
    overall_passed = sum(e["passed"] for e in totals.values())
    interviewers = sorted(
        (
            {
                "interviewer": name,
                "total": e["total"],
                "passed": e["passed"],
                "errors": e["total"] - e["passed"],
                "ici": (e["passed"] * 100.0 / e["total"]) if e["total"] else 0.0,
            }
            for name, e in totals.items()
        ),
        key=lambda r: r["ici"],
        reverse=True,
    )
    return {
        "overall_ici": (overall_passed * 100.0 / overall_total) if overall_total else None,
        "overall_total": overall_total,
        "overall_passed": overall_passed,
        "interviewers": interviewers,
        "checks_applied": checks_applied,
    }


def _pool_location_docs(docs: list) -> dict:
    completed = [d for d in docs if d.get("status") == "completed"]
    if not completed:
        return {
            "status": "failed",
            "computed_at": None,
            "error": "DQA analytics have not been computed yet for this location.",
            "rrs": None, "ics": None, "aid": None, "ici": None,
        }
    return {
        "status": "completed",
        "computed_at": max(d.get("computed_at") or "" for d in completed) or None,
        "rrs": _pool_breakdowns(completed, "rrs"),
        "ics": _pool_breakdowns(completed, "ics"),
        "aid": _pool_breakdowns(completed, "aid"),
        "ici": _pool_ici(completed),
    }


async def fetch_dqa_analytics_snapshot(db: StandardDatabase, current_user: dict) -> Optional[dict]:
    from app.data_quality.services.general_dqa import _location_snapshot_key

    groups = get_location_limit_groups(current_user)

    if not groups:
        def _read_global():
            try:
                col = db.collection(db_collections.DQA_ANALYTICS)
                return col.get(_SNAPSHOT_KEY)
            except Exception:
                return None

        snapshot = await run_in_threadpool(_read_global)
        return _mark_stale_as_failed(snapshot)

    def _read_location_docs():
        col = db.collection(db_collections.DQA_ANALYTICS)
        keys = [_location_snapshot_key(field, value) for field, values in groups for value in values]
        return [d for d in col.get_many(keys) if d]

    docs = await run_in_threadpool(_read_location_docs)
    if not docs:
        # No pre-aggregated row for this user's location(s) yet - e.g. a
        # value with zero VA records, or analytics never computed since
        # this value first appeared. A well-shaped empty snapshot rather
        # than a crash.
        return {"status": "completed", "computed_at": None, "rrs": None, "ics": None, "aid": None, "ici": None}

    return _pool_location_docs(docs)


# ── Full recompute ────────────────────────────────────────────────────────────

async def compute_and_store_dqa_analytics(db: StandardDatabase) -> dict:
    """
    Run all four DQA indicator queries and persist a single snapshot document.
    Marks the snapshot as 'running' before starting so the UI can show progress.
    """
    from app.data_quality.services.general_dqa import (
        fetch_rrs_stats,
        fetch_ics_stats,
        fetch_interview_duration_stats,
        fetch_ici_stats,
        compute_and_store_dqa_map_points,
        compute_and_store_dqa_trend_points,
        compute_and_store_dqa_location_snapshots,
    )

    computed_at = datetime.utcnow().isoformat() + "Z"

    # Mark as in-progress so the frontend can show a spinner while polling.
    await run_in_threadpool(
        _upsert_doc_sync, db, db_collections.DQA_ANALYTICS,
        {"_key": _SNAPSHOT_KEY, "status": "running", "computed_at": computed_at},
    )

    try:
        # One load, shared by all four indicators. Each used to fetch the whole
        # VA table itself, so a recompute pulled 38,000 documents four times
        # over - four peaks instead of one, and on an 8 GB host the worker was
        # SIGKILLed by the OOM killer partway through.
        from app.data_quality.services.general_dqa import _fetch_va_dataframe, _compute_ics_chunked
        from vman_dq import compute_rrs, compute_aid, compute_ici
        from app.settings.services.odk_configs import fetch_odk_config

        df = await _fetch_va_dataframe(db)

        # Computed ONCE here and threaded through everything below -
        # previously each of these was computed independently inside
        # fetch_*_stats AND again inside compute_and_store_dqa_map_points
        # AND again inside compute_and_store_dqa_trend_points: three
        # redundant full recomputes of the OOM-prone chunked ICS step per
        # nightly run.
        rrs_series = await run_in_threadpool(compute_rrs, df)
        ics_series = await run_in_threadpool(_compute_ics_chunked, df)
        aid_series = await run_in_threadpool(compute_aid, df)

        config = await fetch_odk_config(db, True)
        gender_field = config.field_mapping.deceased_gender

        def _compute_ici():
            return compute_ici(df, gender_field=gender_field)

        ici_series, ici_flags, _rule_computable = await run_in_threadpool(_compute_ici)

        rrs_result = await fetch_rrs_stats(db, df, rrs_series)
        ics_result = await fetch_ics_stats(db, df, ics_series)
        aid_result = await fetch_interview_duration_stats(db, df, aid_series)
        ici_result = await fetch_ici_stats(db, df, ici_series, ici_flags)

        # Per-record map points for the Data Map's DQA-indicator coloring -
        # piggybacks on this same batch run/df rather than a separate
        # on-demand recompute (see that function's docstring for why).
        # Isolated in its own try/except: a bug here should not regress the
        # long-standing aggregate-stats snapshot the dashboard depends on.
        try:
            await compute_and_store_dqa_map_points(db, df, rrs_series, ics_series, aid_series, ici_series)
        except Exception as map_exc:
            import logging
            logging.getLogger(__name__).error(f"DQA map points computation failed: {map_exc}")

        # Per-record trend points for the General DQA page's Trend Analysis
        # chart - same reasoning/isolation as the map points above.
        try:
            await compute_and_store_dqa_trend_points(db, df, rrs_series, ics_series, aid_series, ici_series)
        except Exception as trend_exc:
            import logging
            logging.getLogger(__name__).error(f"DQA trend points computation failed: {trend_exc}")

        # Per-location indicator snapshots, for a restricted user's own
        # Data Quality overview - see fetch_dqa_analytics_snapshot for the
        # read-side lookup/pooling. Same isolation reasoning as above.
        try:
            await compute_and_store_dqa_location_snapshots(
                db, df, rrs_series, ics_series, aid_series, ici_series, ici_flags
            )
        except Exception as loc_exc:
            import logging
            logging.getLogger(__name__).error(f"DQA location snapshots computation failed: {loc_exc}")

        snapshot = {
            "_key": _SNAPSHOT_KEY,
            "status": "completed",
            "computed_at": computed_at,
            "rrs": rrs_result.data,
            "ics": ics_result.data,
            "aid": aid_result.data,
            "ici": ici_result.data,
        }
        await run_in_threadpool(
            _upsert_doc_sync, db, db_collections.DQA_ANALYTICS, snapshot,
        )
        return snapshot

    except Exception as exc:
        await run_in_threadpool(
            _upsert_doc_sync, db, db_collections.DQA_ANALYTICS,
            {"_key": _SNAPSHOT_KEY, "status": "failed",
             "error": str(exc), "computed_at": computed_at},
        )
        raise


# ── Schedule config ───────────────────────────────────────────────────────────

_DEFAULT_CONFIG = {"run_hour": 2, "enabled": True}


async def get_dqa_analytics_config(db: StandardDatabase) -> dict:
    def _read():
        try:
            col = db.collection(db_collections.SYSTEM_CONFIGS)
            doc = col.get(_CONFIG_KEY)
            if doc:
                doc.pop("_id", None)
                doc.pop("_rev", None)
                return doc
            return {**_DEFAULT_CONFIG}
        except Exception:
            return {**_DEFAULT_CONFIG}
    return await run_in_threadpool(_read)


async def save_dqa_analytics_config(db: StandardDatabase, config: dict) -> dict:
    def _write():
        doc = {"_key": _CONFIG_KEY, **config}
        col = db.collection(db_collections.SYSTEM_CONFIGS)
        if col.has(_CONFIG_KEY):
            col.update(doc, merge=False)
        else:
            col.insert(doc)
        saved = col.get(_CONFIG_KEY)
        saved.pop("_id", None)
        saved.pop("_rev", None)
        return saved
    return await run_in_threadpool(_write)
