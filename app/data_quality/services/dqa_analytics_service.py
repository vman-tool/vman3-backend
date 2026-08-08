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
# The DQA compute takes about 100 seconds on a modest server, so half an hour
# is far beyond a slow run and still short enough to recover the same day.
_STALE_RUN_AFTER = timedelta(minutes=30)


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


async def fetch_dqa_analytics_snapshot(db: StandardDatabase) -> Optional[dict]:
    def _read():
        try:
            col = db.collection(db_collections.DQA_ANALYTICS)
            return col.get(_SNAPSHOT_KEY)
        except Exception:
            return None

    snapshot = await run_in_threadpool(_read)

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
    )

    computed_at = datetime.utcnow().isoformat() + "Z"

    # Mark as in-progress so the frontend can show a spinner while polling.
    await run_in_threadpool(
        _upsert_doc_sync, db, db_collections.DQA_ANALYTICS,
        {"_key": _SNAPSHOT_KEY, "status": "running", "computed_at": computed_at},
    )

    try:
        rrs_result = await fetch_rrs_stats(db)
        ics_result = await fetch_ics_stats(db)
        aid_result = await fetch_interview_duration_stats(db)
        ici_result = await fetch_ici_stats(db)

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
