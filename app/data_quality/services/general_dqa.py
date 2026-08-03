import pandas as pd
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from vman_dq import compute_ics, compute_rrs, compute_ici, compute_aid
from vman_dq.dqa import ICI_RULE_DESCRIPTIONS

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel

# ---------------------------------------------------------------------------
# Shared helper: fetch every VA record into a DataFrame for vman_dq
# ---------------------------------------------------------------------------
async def _fetch_va_dataframe(db: StandardDatabase) -> pd.DataFrame:
    """Fetch all VA records (no filtering/pagination - DQA assesses the whole
    dataset) into a DataFrame. Column names are whatever the raw documents
    use; vman_dq resolves WHO-VA field names (id10xxx) case-insensitively.
    """
    col = db_collections.VA_TABLE

    def run():
        cursor = db.aql.execute(f"FOR doc IN {col} RETURN doc")
        return list(cursor)

    docs = await run_in_threadpool(run)
    return pd.DataFrame.from_records(docs) if docs else pd.DataFrame()


def _stat_block(series: pd.Series) -> dict:
    """Same shape as the previous AQL _stat_block: avg/min/max/stddev/p50/count."""
    s = series.dropna()
    if s.empty:
        return {"avg": None, "min_v": None, "max_v": None, "stddev": None, "p50": None, "count": 0}
    return {
        "avg": float(s.mean()),
        "min_v": float(s.min()),
        "max_v": float(s.max()),
        "stddev": float(s.std(ddof=0)),
        "p50": float(s.median()),
        "count": int(s.shape[0]),
    }


async def _breakdowns(db: StandardDatabase, df: pd.DataFrame, series: pd.Series) -> dict:
    """overall / by_age_group / by_gender_adult breakdowns for a per-record
    indicator series, grouped using this deployment's configured field
    mapping (is_adult/is_child/is_neonate/deceased_gender) - unlike the
    indicator formulas themselves, these grouping fields remain configurable
    per deployment, matching the previous AQL implementation.
    """
    config = await fetch_odk_config(db, True)
    fm = config.field_mapping

    def col(name):
        if name and name in df.columns:
            return df[name].astype(str)
        return pd.Series("", index=df.index)

    adult = col(fm.is_adult)
    child = col(fm.is_child)
    neonate = col(fm.is_neonate)
    gender = col(fm.deceased_gender).str.lower()

    return {
        "overall": _stat_block(series),
        "by_age_group": {
            "adults": _stat_block(series[adult == "1"]),
            "children": _stat_block(series[child == "1"]),
            "neonates": _stat_block(series[neonate == "1"]),
        },
        "by_gender_adult": {
            "male_adults": _stat_block(series[(adult == "1") & (gender == "male")]),
            "female_adults": _stat_block(series[(adult == "1") & (gender == "female")]),
        },
    }


# ---------------------------------------------------------------------------
# Informative Completeness Score (ICS) Stats
# ---------------------------------------------------------------------------
async def fetch_ics_stats(db: StandardDatabase) -> ResponseMainModel:
    try:
        df = await _fetch_va_dataframe(db)
        if df.empty:
            return ResponseMainModel(data=None, message="No VA records found")

        ics = await run_in_threadpool(compute_ics, df)
        data = await _breakdowns(db, df, ics)
        return ResponseMainModel(data=data, message="ICS statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch ICS statistics", error=str(e))


# ---------------------------------------------------------------------------
# Respondent Reliability Score (RRS) Stats
# ---------------------------------------------------------------------------
async def fetch_rrs_stats(db: StandardDatabase) -> ResponseMainModel:
    try:
        df = await _fetch_va_dataframe(db)
        if df.empty:
            return ResponseMainModel(data=None, message="No VA records found")

        rrs = await run_in_threadpool(compute_rrs, df)
        data = await _breakdowns(db, df, rrs)
        return ResponseMainModel(data=data, message="RRS statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch RRS statistics", error=str(e))


# ---------------------------------------------------------------------------
# Diagnostic: sample what short string values actually exist in the dataset
# Call GET /data-quality/ics-value-sample to see the real yes/no encoding
# ---------------------------------------------------------------------------
async def fetch_ics_value_sample(db: StandardDatabase) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        fm     = config.field_mapping
        col    = db_collections.VA_TABLE

        # Reuse the same exclusion list so we only inspect response-type fields
        excluded: set = {
            fm.instance_id, fm.va_id, fm.consent_id, fm.date,
            fm.location_level1, fm.location_level2, fm.deceased_gender,
            fm.is_adult, fm.is_child, fm.is_neonate,
            fm.interviewer_name, fm.interviewer_phone, fm.interviewer_sex,
        }
        for f in [fm.submitted_date, fm.birth_date, fm.death_date, fm.interview_date, fm.table_name]:
            if f:
                excluded.add(f)
        excluded.update([
            'instanceid', 'today', 'submissiondate', 'start', 'end',
            'deviceid', 'username', 'phonenumber', 'audit', 'duration',
            'vman_data_source', 'vman_data_name', '__id',
            'id10011', 'id10481', 'id10012', 'id10023',
        ])

        query = f"""
        FOR doc IN {col}
        LIMIT 2000
            FOR attr IN ATTRIBUTES(doc, true)
            FILTER attr NOT IN @excluded_fields
            LET v = doc[attr]
            FILTER IS_STRING(v)
            LET norm = LOWER(TRIM(v))
            FILTER LENGTH(norm) <= 5
            COLLECT val = norm WITH COUNT INTO cnt
            SORT cnt DESC
            LIMIT 30
            RETURN {{ val: val, count: cnt }}
        """

        bind_vars = {"excluded_fields": list(excluded)}

        def run():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return list(cursor)

        data = await run_in_threadpool(run)
        return ResponseMainModel(
            data=data,
            message="Sampled top short-string values from first 2000 records"
        )

    except Exception as e:
        return ResponseMainModel(data=None, message="Diagnostic failed", error=str(e))


# ---------------------------------------------------------------------------
# Interview Duration Stats (AID)
# ---------------------------------------------------------------------------
async def fetch_interview_duration_stats(db: StandardDatabase) -> ResponseMainModel:
    try:
        df = await _fetch_va_dataframe(db)
        if df.empty:
            return ResponseMainModel(data=None, message="No VA records found")

        aid = await run_in_threadpool(compute_aid, df)
        data = await _breakdowns(db, df, aid)
        return ResponseMainModel(data=data, message="Interview duration statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch interview duration statistics", error=str(e))


# ---------------------------------------------------------------------------
# Internal Consistency Index (ICI) Stats
# ---------------------------------------------------------------------------
async def fetch_ici_stats(db: StandardDatabase) -> ResponseMainModel:
    """
    ICI = (records with ZERO logical contradictions / total records) x 100

    Rule definitions and per-record computation come from vman_dq
    (compute_ici); this function only reshapes that output into the
    overall/by-interviewer breakdown the frontend expects, grouped by
    interviewer (id10010) and sorted ICI descending.
    """
    try:
        config = await fetch_odk_config(db, True)
        gender_field = config.field_mapping.deceased_gender

        df = await _fetch_va_dataframe(db)
        if df.empty:
            return ResponseMainModel(data=None, message="No VA records found")

        def _compute():
            return compute_ici(df, gender_field=gender_field)

        ici, flags, _rule_computable = await run_in_threadpool(_compute)

        interviewer_col = next((c for c in df.columns if c.lower() == "id10010"), None)
        if interviewer_col:
            interviewer = df[interviewer_col].astype(str).str.strip()
            interviewer = interviewer.mask(interviewer.eq(""), "Unknown")
        else:
            interviewer = pd.Series("Unknown", index=df.index)

        total_recs = len(df)
        n_errors = flags.sum(axis=1) if not flags.empty else pd.Series(0, index=df.index)
        passed_mask = n_errors == 0
        passed_recs = int(passed_mask.sum())
        overall_ici = (passed_recs * 100.0 / total_recs) if total_recs else None

        grouped = (
            pd.DataFrame({"interviewer": interviewer, "errors": n_errors, "passed": passed_mask.astype(int)})
            .groupby("interviewer")
            .agg(total=("interviewer", "count"), errors=("errors", "sum"), passed=("passed", "sum"))
            .reset_index()
        )
        grouped["ici"] = grouped["passed"] * 100.0 / grouped["total"]
        grouped = grouped.sort_values("ici", ascending=False)

        by_interviewer = [
            {
                "interviewer": row["interviewer"],
                "total": int(row["total"]),
                "errors": int(row["errors"]),
                "passed": int(row["passed"]),
                "ici": float(row["ici"]),
            }
            for _, row in grouped.iterrows()
        ]

        data = {
            "overall_ici": overall_ici,
            "overall_total": total_recs,
            "overall_passed": passed_recs,
            "interviewers": by_interviewer,
            "checks_applied": [ICI_RULE_DESCRIPTIONS[r] for r in flags.columns],
        }

        return ResponseMainModel(data=data, message="ICI statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch ICI statistics", error=str(e))