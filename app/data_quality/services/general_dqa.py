from typing import Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from vman_dq import compute_ics, compute_rrs, compute_ici, compute_aid
from vman_dq.dqa import ICI_RULE_DESCRIPTIONS

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import build_location_limit_filter, build_locations_query_filter

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
        # Streamed into the frame rather than list(cursor) first. The whole VA
        # table is loaded here - 38,000 documents of several hundred columns on
        # a real deployment - and materialising it as Python dicts *and* as a
        # DataFrame at the same time doubles the peak for no benefit. That peak
        # is what the OOM killer was reacting to.
        cursor = db.aql.execute(f"FOR doc IN {col} RETURN doc", batch_size=1000)
        frame = pd.DataFrame.from_records(iter(cursor))
        return frame

    return await run_in_threadpool(run)


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

# ICS is the one indicator that touches the whole record rather than a handful
# of columns: compute_ics selects every id* column and does
# `.astype(str).apply(str.strip/lower)` over it, which makes several full
# copies of a 38,000 x ~500 string frame. Measured on a real deployment it
# added 2.6 GB on top of the 1.4 GB the loaded frame already costs, and that
# spike is what the OOM killer reacted to - the worker was SIGKILLed ten
# seconds into a recompute.
#
# It is a per-record score, so slicing rows and concatenating gives exactly the
# same answer (verified against the full-frame result, means equal to six
# decimal places) while capping the copies at chunk size. compute_rrs and
# compute_aid touch four columns and two columns respectively, so they are
# left alone.
_ICS_CHUNK_ROWS = 5000


def _compute_ics_chunked(df: pd.DataFrame) -> pd.Series:
    """compute_ics over row slices, so peak memory does not scale with rows."""
    if len(df) <= _ICS_CHUNK_ROWS:
        return compute_ics(df)

    parts = [
        compute_ics(df.iloc[start:start + _ICS_CHUNK_ROWS])
        for start in range(0, len(df), _ICS_CHUNK_ROWS)
    ]
    return pd.concat(parts)
async def fetch_ics_stats(db: StandardDatabase, df: pd.DataFrame = None) -> ResponseMainModel:
    try:
        df = df if df is not None else await _fetch_va_dataframe(db)
        if df.empty:
            return ResponseMainModel(data=None, message="No VA records found")

        ics = await run_in_threadpool(_compute_ics_chunked, df)
        data = await _breakdowns(db, df, ics)
        return ResponseMainModel(data=data, message="ICS statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch ICS statistics", error=str(e))


# ---------------------------------------------------------------------------
# Respondent Reliability Score (RRS) Stats
# ---------------------------------------------------------------------------
async def fetch_rrs_stats(db: StandardDatabase, df: pd.DataFrame = None) -> ResponseMainModel:
    try:
        df = df if df is not None else await _fetch_va_dataframe(db)
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
            fm.instance_id, fm.va_id, fm.consent_id,
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
async def fetch_interview_duration_stats(db: StandardDatabase, df: pd.DataFrame = None) -> ResponseMainModel:
    try:
        df = df if df is not None else await _fetch_va_dataframe(db)
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
async def fetch_ici_stats(db: StandardDatabase, df: pd.DataFrame = None) -> ResponseMainModel:
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

        df = df if df is not None else await _fetch_va_dataframe(db)
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


# ---------------------------------------------------------------------------
# Per-record DQA map points (Data Map's DQA-indicator coloring)
# ---------------------------------------------------------------------------

def _clean(v):
    """None-safe scalar cleanup: NaN/None -> None, everything else passed
    through as-is (caller casts to float where a number is expected)."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


async def compute_and_store_dqa_map_points(db: StandardDatabase, df: pd.DataFrame) -> int:
    """Persist one row per VA record with GPS + admin location + each
    indicator's raw score, for the Data Map's DQA-indicator coloring.

    Tiering (High/Medium/Low equivalents) deliberately happens on the
    frontend via the already-existing, admin-configurable
    DqaThresholdService - this only stores raw scores, so a threshold
    change recolors the map without needing a recompute here.

    Reuses the exact per-record Series fetch_rrs_stats/fetch_ics_stats/
    fetch_interview_duration_stats/fetch_ici_stats already produce
    internally from this same `df` - the only new cost is zipping them with
    coordinates/location (already columns in `df`) and writing rows.
    """
    if df.empty:
        await run_in_threadpool(
            lambda: db.collection(db_collections.DQA_MAP_POINTS).truncate()
        )
        return 0

    config = await fetch_odk_config(db, True)
    fm = config.field_mapping

    def col(name):
        if name and name in df.columns:
            return df[name]
        return pd.Series(None, index=df.index)

    coordinates = col("coordinates")
    region = col(fm.location_level1)
    district = col(fm.location_level2)
    ward = col(fm.location_level3)
    date_col = col(fm.interview_date)
    va_id_col = col("_key")

    rrs = await run_in_threadpool(compute_rrs, df)
    ics = await run_in_threadpool(_compute_ics_chunked, df)
    aid = await run_in_threadpool(compute_aid, df)

    def _compute_ici_series():
        ici_series, _flags, _computable = compute_ici(df, gender_field=fm.deceased_gender)
        return ici_series

    ici = await run_in_threadpool(_compute_ici_series)

    def run():
        rows = []
        for i in df.index:
            coord = _clean(coordinates.loc[i])
            if not isinstance(coord, (list, tuple)) or len(coord) < 2:
                continue
            lng, lat = _clean(coord[0]), _clean(coord[1])
            if lat is None or lng is None:
                continue

            raw_date = _clean(date_col.loc[i])

            def _num(series):
                v = _clean(series.loc[i])
                return float(v) if v is not None else None

            row = {
                "_key": str(va_id_col.loc[i]),
                "lat": float(lat),
                "lng": float(lng),
                "date": str(raw_date) if raw_date is not None else None,
                "rrs": _num(rrs),
                "ics": _num(ics),
                "ici": _num(ici),
                "aid": _num(aid),
            }
            # Stored under the deployment's *actual* field names (e.g.
            # "id10005r"), not the fixed labels "region"/"district"/"ward" -
            # fetch_dqa_map_points' location filter (build_locations_query_
            # filter/build_location_limit_filter, shared with every other
            # location-filterable view) interpolates doc.<that same raw
            # field name>, so storing it under a different key meant the
            # filter always compared against a field that did not exist and
            # silently matched nothing. A level with no field configured
            # for this deployment (fm.location_levelN falsy) is omitted
            # entirely, same as map_data.py's own district_line handling.
            for field_name, series in (
                (fm.location_level1, region),
                (fm.location_level2, district),
                (fm.location_level3, ward),
            ):
                if field_name:
                    row[field_name] = _clean(series.loc[i])
            rows.append(row)

        collection = db.collection(db_collections.DQA_MAP_POINTS)
        collection.truncate()
        if rows:
            collection.insert_many(rows)
        return len(rows)

    return await run_in_threadpool(run)


async def fetch_dqa_map_points(
    current_user: dict,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    locations: Optional[str] = None,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """Reads the cached per-record snapshot built by
    compute_and_store_dqa_map_points - a plain AQL scan over a small
    pre-computed collection, not a pandas recompute, so this is cheap to
    call on every Data Map page load/filter change.
    """
    try:
        collection = db_collections.DQA_MAP_POINTS
        bind_vars: dict = {}
        filters = []

        location_limit_filter = build_location_limit_filter(current_user, bind_vars)
        if location_limit_filter:
            filters.append(location_limit_filter)

        if start_date:
            filters.append("doc.date >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append("doc.date <= @end_date")
            bind_vars["end_date"] = str(end_date)

        locations_filter = build_locations_query_filter(locations, bind_vars)
        if locations_filter:
            filters.append(locations_filter)

        query = f"FOR doc IN {collection}"
        if filters:
            query += " FILTER " + " AND ".join(filters)
        query += """
            RETURN {
                va_id: doc._key,
                lat: doc.lat,
                lng: doc.lng,
                rrs: doc.rrs,
                ics: doc.ics,
                ici: doc.ici,
                aid: doc.aid
            }
        """

        def run():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return list(cursor)

        data = await run_in_threadpool(run)
        return ResponseMainModel(data=data, message="DQA map points fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch DQA map points", error=str(e))