"""
DQA Analytics Cache Service

Stores the computed results of all four DQA indicators (RRS, ICS, ICI, AID)
as a single snapshot document in `dqa_analytics` so the frontend can load
them from a pre-computed cache instead of running live AQL on every page visit.
"""

from datetime import datetime
from typing import Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel

_SNAPSHOT_KEY = "snapshot"
_CONFIG_KEY = "dqa_analytics_schedule"


# ── Snapshot helpers ──────────────────────────────────────────────────────────

def _upsert_doc_sync(db: StandardDatabase, collection: str, doc: dict) -> None:
    col = db.collection(collection)
    if col.has(doc["_key"]):
        col.update(doc, merge=True)
    else:
        col.insert(doc)


async def fetch_dqa_analytics_snapshot(db: StandardDatabase) -> Optional[dict]:
    def _read():
        try:
            col = db.collection(db_collections.DQA_ANALYTICS)
            return col.get(_SNAPSHOT_KEY)
        except Exception:
            return None
    return await run_in_threadpool(_read)


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
