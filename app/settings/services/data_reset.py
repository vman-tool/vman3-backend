"""
Delete VA submissions by origin, together with everything derived from them.

The use case is resetting a deployment between training runs: import practice
data, work through it, then put the system back to a known state without
touching what came from the other source.

Deleting submissions alone is not enough. A VA record is referenced from five
other collections, and this database already shows what happens when that is
ignored - 40 of its 50 `assigned_va` rows point at submissions that no longer
exist. So the delete cascades:

    form_submissions.instanceid
      ├─ ccva_results.ID
      ├─ assigned_va.vaId ─── pcva_results.assigned_va
      ├─ pcva_messages.va
      └─ ccva_errors_corrections.form_id   (stored without the 'uuid:' prefix)

CCVA runs (`ccva_graph_results`, `ccva_errors`) are keyed by task, not by
submission, so they are removed only when a run loses every one of its results
- and only for runs this delete actually touched, never for orphans that were
already lying around.

Order matters: derived rows go first and the submissions themselves go last.
The steps are separate statements rather than one transaction, so if the
process dies midway the leftovers are submissions whose derived data is gone,
which the application recomputes - never derived rows pointing at submissions
that have vanished.
"""

from typing import Dict, List

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import data_sources, db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException

VALID_SOURCES = {
    data_sources.ODK_API: "API Synchronization",
    data_sources.CSV_UPLOAD: "File Upload",
}


def _validate_sources(sources: List[str]) -> List[str]:
    if not sources:
        raise BadRequestException(
            "Choose at least one data source to delete."
        )
    unknown = [s for s in sources if s not in VALID_SOURCES]
    if unknown:
        raise BadRequestException(
            f"Unknown data source(s): {', '.join(unknown)}. "
            f"Valid values are {', '.join(VALID_SOURCES)}."
        )
    # de-duplicate while keeping the caller's order
    return list(dict.fromkeys(sources))


# Shared preamble: the submissions in scope, and the ids other collections use
# to point at them. Recomputed by each step, which is why the submissions
# themselves must be deleted last.
_TARGETS = f"""
    LET targets = (
        FOR s IN {db_collections.VA_TABLE}
            FILTER s.vman_data_source IN @sources
            RETURN s.instanceid
    )
"""


def _count_scope(db: StandardDatabase, sources: List[str]) -> Dict[str, int]:
    """What a reset would remove, without removing anything."""
    query = f"""
        {_TARGETS}
        LET bare = (FOR t IN targets RETURN SUBSTITUTE(t, 'uuid:', ''))
        LET assigned = (
            FOR a IN {db_collections.ASSIGNED_VA}
                FILTER a.vaId IN targets
                RETURN a.uuid
        )
        RETURN {{
            submissions: LENGTH(targets),
            ccva_results: LENGTH(
                FOR c IN {db_collections.CCVA_RESULTS} FILTER c.ID IN targets RETURN 1),
            assigned_va: LENGTH(assigned),
            pcva_results: LENGTH(
                FOR p IN {db_collections.PCVA_RESULTS} FILTER p.assigned_va IN assigned RETURN 1),
            pcva_messages: LENGTH(
                FOR m IN {db_collections.PCVA_MESSAGES} FILTER m.va IN targets RETURN 1),
            ccva_corrections: LENGTH(
                FOR x IN {db_collections.CCVA_ERRORS_CORRECTIONS} FILTER x.form_id IN bare RETURN 1)
        }}
    """
    return db.aql.execute(query, bind_vars={"sources": sources}).next()


async def preview_reset(sources: List[str], db: StandardDatabase) -> ResponseMainModel:
    """Report what a reset would delete. Changes nothing.

    The confirmation dialog shows these numbers, so the user is agreeing to a
    specific amount of destruction rather than to the idea of it.
    """
    sources = _validate_sources(sources)
    counts = await run_in_threadpool(_count_scope, db, sources)

    total = sum(counts.values())
    labels = ", ".join(VALID_SOURCES[s] for s in sources)

    return ResponseMainModel(
        data={"sources": sources, "counts": counts, "total": total},
        message=(
            f"{counts['submissions']} VA record(s) from {labels}, "
            f"and {total - counts['submissions']} related record(s), would be deleted."
        ),
        total=total,
    )


def _run_reset(db: StandardDatabase, sources: List[str]) -> Dict[str, int]:
    """Perform the cascade. Returns the number removed from each collection."""
    bind = {"sources": sources}
    removed: Dict[str, int] = {}

    def run(query: str, bind_vars: Dict = None) -> int:
        # Pass only the parameters the query actually references: ArangoDB
        # rejects a declared-but-unused bind parameter outright (ERR 1552),
        # so the source list cannot simply be merged into every call.
        return db.aql.execute(query, bind_vars=bind_vars or {}).next()

    # Which CCVA runs are about to lose results. Captured before anything is
    # deleted, so the cleanup below can tell "this run was emptied by us" from
    # "this run was already empty".
    affected_tasks = db.aql.execute(
        f"""
        {_TARGETS}
        RETURN UNIQUE(
            FOR c IN {db_collections.CCVA_RESULTS}
                FILTER c.ID IN targets
                RETURN c.task_id
        )
        """,
        bind_vars=bind,
    ).next() or []

    # ── Derived records first ────────────────────────────────────────────────
    removed["pcva_results"] = run(f"""
        {_TARGETS}
        LET assigned = (
            FOR a IN {db_collections.ASSIGNED_VA} FILTER a.vaId IN targets RETURN a.uuid)
        RETURN LENGTH(
            FOR p IN {db_collections.PCVA_RESULTS}
                FILTER p.assigned_va IN assigned
                REMOVE p IN {db_collections.PCVA_RESULTS}
                RETURN 1)
    """, bind)

    removed["pcva_messages"] = run(f"""
        {_TARGETS}
        RETURN LENGTH(
            FOR m IN {db_collections.PCVA_MESSAGES}
                FILTER m.va IN targets
                REMOVE m IN {db_collections.PCVA_MESSAGES}
                RETURN 1)
    """, bind)

    removed["assigned_va"] = run(f"""
        {_TARGETS}
        RETURN LENGTH(
            FOR a IN {db_collections.ASSIGNED_VA}
                FILTER a.vaId IN targets
                REMOVE a IN {db_collections.ASSIGNED_VA}
                RETURN 1)
    """, bind)

    removed["ccva_results"] = run(f"""
        {_TARGETS}
        RETURN LENGTH(
            FOR c IN {db_collections.CCVA_RESULTS}
                FILTER c.ID IN targets
                REMOVE c IN {db_collections.CCVA_RESULTS}
                RETURN 1)
    """, bind)

    removed["ccva_corrections"] = run(f"""
        {_TARGETS}
        LET bare = (FOR t IN targets RETURN SUBSTITUTE(t, 'uuid:', ''))
        RETURN LENGTH(
            FOR x IN {db_collections.CCVA_ERRORS_CORRECTIONS}
                FILTER x.form_id IN bare
                REMOVE x IN {db_collections.CCVA_ERRORS_CORRECTIONS}
                RETURN 1)
    """, bind)

    # ── The submissions themselves ───────────────────────────────────────────
    removed["submissions"] = run(f"""
        RETURN LENGTH(
            FOR s IN {db_collections.VA_TABLE}
                FILTER s.vman_data_source IN @sources
                REMOVE s IN {db_collections.VA_TABLE}
                RETURN 1)
    """, bind)

    # ── CCVA runs left with nothing ──────────────────────────────────────────
    emptied = []
    for task_id in affected_tasks:
        if not task_id:
            continue
        still_has = db.aql.execute(
            f"RETURN LENGTH(FOR c IN {db_collections.CCVA_RESULTS} "
            f"FILTER c.task_id == @task LIMIT 1 RETURN 1)",
            bind_vars={"task": task_id},
        ).next()
        if not still_has:
            emptied.append(task_id)

    if emptied:
        removed["ccva_graph_results"] = run(
            f"""RETURN LENGTH(
                FOR g IN {db_collections.CCVA_GRAPH_RESULTS}
                    FILTER g.task_id IN @tasks
                    REMOVE g IN {db_collections.CCVA_GRAPH_RESULTS}
                    RETURN 1)""",
            {"tasks": emptied},
        )
        removed["ccva_errors"] = run(
            f"""RETURN LENGTH(
                FOR e IN {db_collections.CCVA_ERRORS}
                    FILTER e.task_id IN @tasks
                    REMOVE e IN {db_collections.CCVA_ERRORS}
                    RETURN 1)""",
            {"tasks": emptied},
        )
    else:
        removed["ccva_graph_results"] = 0
        removed["ccva_errors"] = 0

    # ── Aggregates that no longer describe the data ──────────────────────────
    # The DQA snapshot is computed across every submission, so any deletion
    # invalidates it wholesale. It is rebuilt on the analytics schedule.
    if removed["submissions"]:
        removed["dqa_analytics"] = run(
            f"""RETURN LENGTH(
                FOR d IN {db_collections.DQA_ANALYTICS}
                    REMOVE d IN {db_collections.DQA_ANALYTICS}
                    RETURN 1)"""
        )

        # With nothing left, a "last synchronized" timestamp describes data
        # that is gone; clear it so the status card is not quietly wrong.
        remaining = db.aql.execute(
            f"RETURN LENGTH(FOR s IN {db_collections.VA_TABLE} LIMIT 1 RETURN 1)"
        ).next()
        if not remaining:
            config = db.collection(db_collections.SYSTEM_CONFIGS).get("vman_config")
            if config and config.get("sync_status"):
                config["sync_status"]["last_sync_date"] = None
                config["sync_status"]["last_sync_data_count"] = 0
                config["sync_status"]["total_synced_data"] = 0
                db.collection(db_collections.SYSTEM_CONFIGS).update(config)
    else:
        removed["dqa_analytics"] = 0

    return removed


async def reset_va_data(sources: List[str], db: StandardDatabase) -> ResponseMainModel:
    """Delete every submission from the given sources, and its dependants."""
    sources = _validate_sources(sources)

    # Skipping collections a deployment does not have keeps this usable on a
    # fresh install, where PCVA and CCVA may never have run.
    for name in (
        db_collections.ASSIGNED_VA,
        db_collections.PCVA_RESULTS,
        db_collections.PCVA_MESSAGES,
        db_collections.CCVA_RESULTS,
        db_collections.CCVA_ERRORS,
        db_collections.CCVA_ERRORS_CORRECTIONS,
        db_collections.CCVA_GRAPH_RESULTS,
        db_collections.DQA_ANALYTICS,
    ):
        if not db.has_collection(name):
            db.create_collection(name)

    removed = await run_in_threadpool(_run_reset, db, sources)

    labels = ", ".join(VALID_SOURCES[s] for s in sources)
    related = sum(v for k, v in removed.items() if k != "submissions")

    print(
        f"DATA RESET: sources={sources} removed={removed}"
    )

    return ResponseMainModel(
        data={"sources": sources, "removed": removed},
        message=(
            f"Deleted {removed['submissions']} VA record(s) from {labels}, "
            f"and {related} related record(s)."
        ),
        total=removed["submissions"],
    )
