from typing import Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.pcva.utilities.pcva_utils import fetch_pcva_settings
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def _fetch_ccva_cod(va_id: str, db: StandardDatabase, task_id: Optional[str] = None) -> Optional[dict]:
    """CoD for this VA from a specific CCVA run when `task_id` is given (used
    by CCVA > Display Data, where the run being viewed is unambiguous and
    may not be the one marked default - showing the default run's cause
    there would be inconsistent with the row the user actually clicked),
    otherwise from whichever run is currently marked default (InterVA5 or
    VManML10 - the VA Records menu and everywhere else with no specific run
    in context). Returns None when the relevant run doesn't exist, or has no
    result for this particular VA.
    """
    if task_id:
        # "graph" is a reserved AQL keyword (GRAPH traversal syntax) and
        # can't be used as a LET variable name - caught live, where the real
        # ArangoDB parser rejects it; the FakeDB-backed unit tests below
        # don't parse AQL so they couldn't catch this.
        query = f"""
            LET run_graph = FIRST(
                FOR g IN {db_collections.CCVA_GRAPH_RESULTS}
                    FILTER g.task_id == @task_id
                    RETURN g
            )
            LET record = FIRST(
                FOR r IN {db_collections.CCVA_RESULTS}
                    FILTER r.task_id == @task_id AND (r.ID == @va_id OR r.uid == @va_id)
                    RETURN r
            )
            FILTER record != null
            RETURN {{
                algorithm: run_graph != null && run_graph.algorithm != null ? run_graph.algorithm : "InterVA5",
                cause1: record.CAUSE1,
                probability: record.LIK1
            }}
        """
        bind_vars = {"va_id": va_id, "task_id": task_id}
    else:
        query = f"""
            LET default_run = FIRST(
                FOR g IN {db_collections.CCVA_GRAPH_RESULTS}
                    FILTER g.isDefault == true
                    RETURN g
            )
            FILTER default_run != null
            LET record = FIRST(
                FOR r IN {db_collections.CCVA_RESULTS}
                    FILTER r.task_id == default_run.task_id AND (r.ID == @va_id OR r.uid == @va_id)
                    RETURN r
            )
            FILTER record != null
            RETURN {{
                algorithm: default_run.algorithm != null ? default_run.algorithm : "InterVA5",
                cause1: record.CAUSE1,
                probability: record.LIK1
            }}
        """
        bind_vars = {"va_id": va_id}

    def execute():
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        results = list(cursor)
        return results[0] if results else None

    return await run_in_threadpool(execute)


async def _fetch_pcva_cod(va_id: str, db: StandardDatabase) -> Optional[dict]:
    """Every coder's latest coding for this VA, plus whether they've reached
    concordance (per the configured concordanceLevel) on the underlying
    cause. Returns None when no coder has coded this VA yet.
    """
    query = f"""
        FOR result IN {db_collections.PCVA_RESULTS}
            FILTER result.assigned_va == @va_id AND result.is_deleted == false
            SORT result.datetime DESC
            COLLECT coder = result.created_by INTO codings = result
            LET latest = FIRST(codings)
            LET cause_uuid = latest.frameA.d != null ? latest.frameA.d :
                        latest.frameA.c != null ? latest.frameA.c :
                        latest.frameA.b != null ? latest.frameA.b :
                        latest.frameA.a
            LET cause_name = cause_uuid != null ? FIRST(
                FOR icd IN {db_collections.ICD10}
                    FILTER icd.uuid == cause_uuid
                    RETURN CONCAT(icd.code, " - ", icd.name)
            ) : null
            LET coder_name = FIRST(
                FOR u IN {db_collections.USERS}
                    FILTER u._key == coder OR u.uuid == coder
                    RETURN u.name
            )
            RETURN {{
                coder: coder_name != null ? coder_name : coder,
                coded_at: latest.datetime,
                underlying_cause: cause_name
            }}
    """

    def execute():
        cursor = db.aql.execute(query, bind_vars={"va_id": va_id})
        return list(cursor)

    coders = await run_in_threadpool(execute)
    if not coders:
        return None

    pcva_config = await fetch_pcva_settings(db)
    concordance_level = pcva_config.concordanceLevel

    tallies: dict = {}
    for c in coders:
        cause = c.get("underlying_cause")
        if cause:
            tallies[cause] = tallies.get(cause, 0) + 1

    top_cause, agreeing = max(tallies.items(), key=lambda kv: kv[1]) if tallies else (None, 0)
    reached = agreeing >= concordance_level and top_cause is not None

    return {
        "coders": coders,
        "concordance": {
            "reached": reached,
            "underlying_cause": top_cause if reached else None,
            "agreeing_coders": agreeing,
            "total_coders": len(coders),
            "concordance_level": concordance_level,
        },
    }


async def get_va_cause_of_death(
    va_id: str, include_ccva: bool, include_pcva: bool, db: StandardDatabase, task_id: Optional[str] = None
) -> ResponseMainModel:
    ccva = await _fetch_ccva_cod(va_id, db, task_id) if include_ccva else None
    pcva = await _fetch_pcva_cod(va_id, db) if include_pcva else None

    return ResponseMainModel(
        data={"ccva": ccva, "pcva": pcva},
        message="Cause of death fetched successfully"
    )
