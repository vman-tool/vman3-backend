import asyncio
from datetime import date
from io import BytesIO
from typing import List, Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel
from arango.database import StandardDatabase as _ArangoDB
from fastapi.concurrency import run_in_threadpool


async def _run_export_query(query: str, bind_vars: dict, db) -> list:
    """Execute an AQL query with streaming batch_size and a generous max_runtime."""
    def _exec():
        cursor = db.aql.execute(
            query=query,
            bind_vars=bind_vars,
            batch_size=5000,   # stream 5k rows at a time instead of all at once
            max_runtime=300,   # 5-minute ceiling (default is 60s)
            count=False,
        )
        return list(cursor)    # exhaust the cursor
    return await run_in_threadpool(_exec)


async def export_merged_va_records(
    current_user: dict,
    db: StandardDatabase,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[List[str]] = None,
    date_type: Optional[str] = None,
    results_filter: str = "all",  # "all", "both", "ccva_only", "pcva_only"
    file_format: str = "excel"
) -> StreamingResponse:
    """
    Export VA records merged with CCVA and PCVA results.
    Uses separate queries and in-memory merging for performance.
    """
    try:
        print(f"Export request received: start_date={start_date}, end_date={end_date}, "
              f"locations={locations}, date_type={date_type}, results_filter={results_filter}")

        # ── 1. Fetch ODK config ──────────────────────────────────────────────
        config = await fetch_odk_config(db, True)

        instance_id      = config.field_mapping.instance_id or 'instanceid'
        region_field     = config.field_mapping.location_level1 or 'region'
        district_field   = config.field_mapping.location_level2 or 'district'
        interview_date_f = config.field_mapping.interview_date or 'id10012'
        death_date_f     = config.field_mapping.death_date or 'id10023'
        submission_date_f = config.field_mapping.submitted_date or 'submissiondate'
        interviewer_f    = 'id10019'

        date_field = death_date_f
        if date_type == 'interview_date':
            date_field = interview_date_f
        elif date_type == 'submission_date':
            date_field = submission_date_f

        # ── 2. Build VA filter ───────────────────────────────────────────────
        filter_conditions = []
        bind_vars: dict = {}

        if start_date:
            filter_conditions.append(f"va.{date_field} >= @start_date")
            bind_vars['start_date'] = start_date.isoformat()
        if end_date:
            filter_conditions.append(f"va.{date_field} <= @end_date")
            bind_vars['end_date'] = end_date.isoformat()
        if locations:
            filter_conditions.append(f"va.{region_field} IN @locations")
            bind_vars['locations'] = locations

        filter_clause = " AND ".join(filter_conditions) if filter_conditions else "true"

        # ── 3. Query VA records ──────────────────────────────────────────────
        va_query = f"""
            FOR va IN {db_collections.VA_TABLE}
                FILTER {filter_clause}
                RETURN {{
                    va_id:           va.{instance_id},
                    region:          va.{region_field},
                    district:        va.{district_field},
                    interview_date:  va.{interview_date_f},
                    death_date:      va.{death_date_f},
                    submission_date: va.{submission_date_f},
                    interviewer_name: va.{interviewer_f}
                }}
        """
        va_cursor = await _run_export_query(query=va_query, bind_vars=bind_vars, db=db)
        va_records = va_cursor

        if not va_records:
            raise HTTPException(status_code=404, detail="No records found matching the specified filters")

        # Extract the list of VA IDs for subsequent queries
        va_ids = [r['va_id'] for r in va_records if r.get('va_id')]

        print(f"Fetched {len(va_records)} VA records. Fetching CCVA/PCVA/ICD10 in parallel...")

        # ── 4-6. Run CCVA, PCVA, ICD10 queries concurrently (asyncio.gather = Promise.all) ──
        ccva_query = f"""
            FOR ccva IN {db_collections.CCVA_RESULTS}
                FILTER ccva.ID IN @va_ids OR ccva.uid IN @va_ids
                FILTER ccva.CAUSE1 != ""
                COLLECT va_id = (ccva.ID != null ? ccva.ID : ccva.uid)
                AGGREGATE
                    ccva_top_cause  = MAX(ccva.CAUSE1),
                    ccva_cause2     = MAX(ccva.CAUSE2),
                    ccva_cause3     = MAX(ccva.CAUSE3),
                    ccva_likelihood = MAX(ccva.LIK1),
                    ccva_age_group  = MAX(ccva.age_group),
                    ccva_gender     = MAX(ccva.gender)
                RETURN {{
                    va_id:           va_id,
                    ccva_top_cause:  ccva_top_cause,
                    ccva_cause2:     ccva_cause2,
                    ccva_cause3:     ccva_cause3,
                    ccva_likelihood: ccva_likelihood,
                    ccva_age_group:  ccva_age_group,
                    ccva_gender:     ccva_gender
                }}
        """

        pcva_query = f"""
            FOR pcva IN {db_collections.PCVA_RESULTS}
                FILTER pcva.assigned_va IN @va_ids
                SORT pcva.assigned_va, pcva.datetime DESC
                COLLECT va_id = pcva.assigned_va INTO grouped = pcva
                LET latest = FIRST(grouped)
                RETURN {{
                    va_id:               va_id,
                    pcva_coder:          latest.created_by,
                    pcva_cause_a:        latest.frameA.a,
                    pcva_cause_b:        latest.frameA.b,
                    pcva_cause_c:        latest.frameA.c,
                    pcva_cause_d:        latest.frameA.d,
                    pcva_contributories: latest.frameA.contributories,
                    pcva_coded_at:       latest.datetime
                }}
        """

        icd10_query = f"""
            FOR code IN {db_collections.ICD10}
                RETURN {{ uuid: code.uuid, code: code.code, name: code.name }}
        """

        # Fire all three queries at the same time
        ccva_records, pcva_records, icd10_raw = await asyncio.gather(
            _run_export_query(query=ccva_query, bind_vars={'va_ids': va_ids}, db=db),
            _run_export_query(query=pcva_query, bind_vars={'va_ids': va_ids}, db=db),
            _run_export_query(query=icd10_query, bind_vars={}, db=db),
        )

        icd10_lookup: dict = {
            r['uuid']: f"({r['code']}) {r['name']}"
            for r in icd10_raw
            if r.get('uuid')
        }

        print(f"CCVA: {len(ccva_records)}, PCVA: {len(pcva_records)}, ICD10 codes: {len(icd10_lookup)}")

        # ── 7. Resolve ICD10 UUIDs in PCVA records ───────────────────────────
        def resolve(uuid):
            return icd10_lookup.get(uuid, '') if uuid else ''

        def resolve_list(uuids):
            if not uuids:
                return ''
            return ', '.join(filter(None, [resolve(u) for u in uuids]))

        for p in pcva_records:
            a = resolve(p.get('pcva_cause_a'))
            b = resolve(p.get('pcva_cause_b'))
            c = resolve(p.get('pcva_cause_c'))
            d = resolve(p.get('pcva_cause_d'))
            # Underlying cause: deepest non-null cause
            underlying = next((x for x in [d, c, b, a] if x), '')
            p['pcva_cause_a']           = a
            p['pcva_cause_b']           = b
            p['pcva_cause_c']           = c
            p['pcva_cause_d']           = d
            p['pcva_underlying_cause']  = underlying
            p['pcva_contributory_causes'] = resolve_list(p.get('pcva_contributories'))
            del p['pcva_contributories']

        # ── 8. Build DataFrames and merge ─────────────────────────────────────
        df_va   = pd.DataFrame(va_records)
        df_ccva = pd.DataFrame(ccva_records) if ccva_records else pd.DataFrame(columns=['va_id'])
        df_pcva = pd.DataFrame(pcva_records) if pcva_records else pd.DataFrame(columns=['va_id'])

        df = df_va.merge(df_ccva, on='va_id', how='left')
        df = df.merge(df_pcva,  on='va_id', how='left')

        # ── 9. Apply results_filter ───────────────────────────────────────────
        has_ccva = df['ccva_top_cause'].notna() & (df['ccva_top_cause'] != '')
        has_pcva = df['pcva_coder'].notna()     & (df['pcva_coder'] != '')

        if results_filter == 'both':
            df = df[has_ccva & has_pcva]
        elif results_filter == 'ccva_only':
            df = df[has_ccva]
        elif results_filter == 'pcva_only':
            df = df[has_pcva]
        # 'all' → no additional filter

        if df.empty:
            raise HTTPException(status_code=404, detail="No records found matching the specified filters")

        # ── 10. Rename columns ────────────────────────────────────────────────
        column_names = {
            'va_id':                    'VA ID',
            'region':                   'Region',
            'district':                 'District',
            'interview_date':           'Interview Date',
            'death_date':               'Death Date',
            'submission_date':          'Submission Date',
            'interviewer_name':         'Interviewer Name',
            'ccva_top_cause':           'CCVA - Top Cause',
            'ccva_cause2':              'CCVA - Cause 2',
            'ccva_cause3':              'CCVA - Cause 3',
            'ccva_likelihood':          'CCVA - Likelihood',
            'ccva_age_group':           'CCVA - Age Group',
            'ccva_gender':              'CCVA - Gender',
            'pcva_coder':               'PCVA - Coder',
            'pcva_cause_a':             'PCVA - Cause A',
            'pcva_cause_b':             'PCVA - Cause B',
            'pcva_cause_c':             'PCVA - Cause C',
            'pcva_cause_d':             'PCVA - Cause D',
            'pcva_underlying_cause':    'PCVA - Underlying Cause',
            'pcva_contributory_causes': 'PCVA - Contributory Causes',
            'pcva_coded_at':            'PCVA - Coded At',
        }
        df.rename(columns=column_names, inplace=True)
        df.fillna('', inplace=True)

        # ── 11. Export ────────────────────────────────────────────────────────
        output = BytesIO()
        df.index = range(1, len(df) + 1)
        df.index.name = 'No.'

        if file_format.lower() == "csv":
            df.to_csv(output, index=True)
            output.seek(0)
            media_type = "text/csv"
            filename   = f"va_records_export_{date.today().isoformat()}.csv"
        else:
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=True, sheet_name="VA Records")
            output.seek(0)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename   = f"va_records_export_{date.today().isoformat()}.xlsx"

        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(output, media_type=media_type, headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        print(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to export VA records: {str(e)}")
