from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel

# ---------------------------------------------------------------------------
# Shared helper: build per-group AQL stat blocks
# ---------------------------------------------------------------------------
def _stat_block(arr_name: str, value_field: str) -> str:
    """Return an AQL inline object that computes all distribution stats for an array."""
    a = f"{arr_name}[*].{value_field}"
    return (
        f"{{ avg: AVERAGE({a}), min_v: MIN({a}), max_v: MAX({a}), "
        f"stddev: STDDEV_POPULATION({a}), p50: PERCENTILE({a}, 50), count: LENGTH({a}) }}"
    )


# ---------------------------------------------------------------------------
# Internal Consistency Index (ICI) Stats
# ---------------------------------------------------------------------------
async def fetch_ici_stats(db: StandardDatabase) -> ResponseMainModel:
    """
    Computes the Internal Consistency Index (ICI) per interviewer.

    ICI = (records with ZERO logical contradictions / total records) × 100

    Six logical checks applied per record:
      1. Pregnancy in male deceased        (id10019 vs id10305)
      2. Coughed blood without cough       (id10153 vs id10157)
      3. Fever duration > illness duration  (id10148 vs id10120)
      4. Cough duration > illness duration  (id10154 vs id10120)
      5. Diarrhoea duration > illness       (id10182 vs id10120)
      6. Breathlessness duration > illness  (id10161 vs id10120)

    Results are grouped by interviewer (id10010) and sorted ICI descending.
    """
    try:
        config = await fetch_odk_config(db, True)
        fm     = config.field_mapping
        gender = fm.deceased_gender
        col    = db_collections.VA_TABLE

        query = f"""
        LET records = (
            FOR doc IN {col}

            // ── Per-record component checks ──────────────────────────────────
            LET gender_val = LOWER(TRIM(doc.{gender}))
            LET pregnancy  = LOWER(TRIM(doc.id10305))
            LET had_cough  = LOWER(TRIM(doc.id10153))
            LET blood_cgh  = LOWER(TRIM(doc.id10157))
            LET had_fever  = LOWER(TRIM(doc.id10147))
            LET had_diarr  = LOWER(TRIM(doc.id10181))
            LET had_breath = LOWER(TRIM(doc.id10159))

            // Calculated duration fields (stored as numbers in ArangoDB)
            LET ill_days    = TO_NUMBER(doc.id10120)
            LET fever_days  = TO_NUMBER(doc.id10148)
            LET cough_days  = TO_NUMBER(doc.id10154)
            LET diarr_days  = TO_NUMBER(doc.id10182)
            LET breath_days = TO_NUMBER(doc.id10161)

            LET chk_preg        = (gender_val == "male"  AND pregnancy == "yes"     ? 1 : 0)
            LET chk_blood       = (had_cough  == "no"    AND blood_cgh == "yes"     ? 1 : 0)
            LET chk_fever_dur   = (had_fever  == "yes"   AND ill_days > 0 AND fever_days  > ill_days ? 1 : 0)
            LET chk_cough_dur   = (had_cough  == "yes"   AND ill_days > 0 AND cough_days  > ill_days ? 1 : 0)
            LET chk_diarr_dur   = (had_diarr  == "yes"   AND ill_days > 0 AND diarr_days  > ill_days ? 1 : 0)
            LET chk_breath_dur  = (had_breath == "yes"   AND ill_days > 0 AND breath_days > ill_days ? 1 : 0)

            LET errors = chk_preg + chk_blood + chk_fever_dur + chk_cough_dur + chk_diarr_dur + chk_breath_dur

            RETURN {{
                interviewer: (doc.id10010 != null AND doc.id10010 != "" ? TRIM(doc.id10010) : "Unknown"),
                errors:  errors,
                passed:  (errors == 0 ? 1 : 0)
            }}
        )

        LET total_recs   = LENGTH(records)
        LET passed_recs  = SUM(records[*].passed)
        LET overall_ici  = (total_recs > 0 ? passed_recs * 100.0 / total_recs : null)

        LET by_interviewer = (
            FOR rec IN records
            COLLECT iname = rec.interviewer
            AGGREGATE
                n    = SUM(1),
                errs = SUM(rec.errors),
                psd  = SUM(rec.passed)
            LET ici_score = psd * 100.0 / n
            SORT ici_score DESC
            RETURN {{
                interviewer: iname,
                total:   n,
                errors:  errs,
                passed:  psd,
                ici:     ici_score
            }}
        )

        RETURN {{
            overall_ici:    overall_ici,
            overall_total:  total_recs,
            overall_passed: passed_recs,
            interviewers:   by_interviewer,
            checks_applied: [
                "Pregnancy in male deceased (id10019 vs id10305)",
                "Coughed blood without prior cough (id10153 vs id10157)",
                "Fever duration exceeds total illness duration (id10148 vs id10120)",
                "Cough duration exceeds total illness duration (id10154 vs id10120)",
                "Diarrhoea duration exceeds total illness duration (id10182 vs id10120)",
                "Breathlessness duration exceeds total illness duration (id10161 vs id10120)"
            ]
        }}
        """

        def run():
            cursor = db.aql.execute(query)
            results = list(cursor)
            return results[0] if results else None

        data = await run_in_threadpool(run)
        return ResponseMainModel(data=data, message="ICI statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch ICI statistics", error=str(e))


# ---------------------------------------------------------------------------
# Respondent Reliability Score (RRS) Stats
# ---------------------------------------------------------------------------
async def fetch_rrs_stats(db: StandardDatabase) -> ResponseMainModel:
    """
    Computes the Respondent Reliability Score (RRS, 0-100) per VA record.

    Components
    ----------
    Wrel  (max 40): relationship to deceased          — Id10008
    Wprox (max 30): co-residence during final illness — Id10009
                    (best proxy available; the form has no explicit
                     "present at death" field)
    Wrec  (max 20): interview recall period           — DATE_DIFF(id10023, id10011)
    Wedu  (max 10): literacy proxy                    — Id10064 / Id10063
                    (fields capture deceased's literacy; respondent
                     literacy is not separately recorded in the form)
    """
    try:
        config = await fetch_odk_config(db, True)
        fm         = config.field_mapping
        is_adult   = fm.is_adult
        is_child   = fm.is_child
        is_neonate = fm.is_neonate
        gender     = fm.deceased_gender
        death_date = fm.death_date or 'id10023'
        col        = db_collections.VA_TABLE

        query = f"""
        LET records = (
            FOR doc IN {col}

            // ── Wrel: Relationship to deceased (Id10008) ──────────────────
            LET wrel = (
                LOWER(doc.id10008) == "spouse"                                     ? 40 :
                LOWER(doc.id10008) IN ["parent", "child"]                          ? 40 :
                LOWER(doc.id10008) == "family_member"                              ? 20 :
                LOWER(doc.id10008) IN ["friend","health_worker",
                                       "public_official","another_relationship"]   ? 10 :
                10
            )

            // ── Wprox: Co-residence / proximity (Id10009) ──────────────────
            LET wprox = (
                LOWER(doc.id10009) == "yes" ? 30 :
                LOWER(doc.id10009) == "no"  ? 15 :
                0
            )

            // ── Wrec: Recall period — interview start vs death date ─────────
            LET recall_days = (
                doc.id10011       != null AND doc.id10011       != "" AND
                doc.{death_date}  != null AND doc.{death_date}  != "" ?
                DATE_DIFF(doc.{death_date}, doc.id10011, "d") : null
            )
            LET wrec = (
                recall_days == null ? null :
                recall_days <  0   ?  0   :
                recall_days <  90  ? 20   :
                recall_days < 180  ? 15   :
                recall_days < 365  ? 10   :
                0
            )
            FILTER wrec != null

            // ── Wedu: Literacy proxy via deceased's literacy (Id10064/Id10063)
            LET wedu = (
                LOWER(doc.id10064) == "yes" ? 10 :
                LOWER(doc.id10064) == "no"  ?  5 :
                LOWER(doc.id10063) IN ["primary_school",
                                       "secondary_school",
                                       "higher_than_secondary_school"] ? 10 :
                LOWER(doc.id10063) == "no_formal_education"            ?  5 :
                7
            )

            RETURN {{
                v:       wrel + wprox + wrec + wedu,
                adult:   doc.{is_adult},
                child:   doc.{is_child},
                neonate: doc.{is_neonate},
                gender:  doc.{gender}
            }}
        )

        LET all_r     = records
        LET adult_r   = (FOR r IN records FILTER r.adult   == "1" RETURN r)
        LET child_r   = (FOR r IN records FILTER r.child   == "1" RETURN r)
        LET neonate_r = (FOR r IN records FILTER r.neonate == "1" RETURN r)
        LET m_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "male"   RETURN r)
        LET f_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "female" RETURN r)

        LET s_all     = {_stat_block('all_r',     'v')}
        LET s_adults  = {_stat_block('adult_r',   'v')}
        LET s_child   = {_stat_block('child_r',   'v')}
        LET s_neonate = {_stat_block('neonate_r', 'v')}
        LET s_m_adult = {_stat_block('m_adult_r', 'v')}
        LET s_f_adult = {_stat_block('f_adult_r', 'v')}

        RETURN {{
            overall: s_all,
            by_age_group: {{ adults: s_adults, children: s_child, neonates: s_neonate }},
            by_gender_adult: {{ male_adults: s_m_adult, female_adults: s_f_adult }}
        }}
        """

        def run():
            cursor = db.aql.execute(query)
            results = list(cursor)
            return results[0] if results else None

        data = await run_in_threadpool(run)
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
# Interview Duration Stats
# ---------------------------------------------------------------------------
async def fetch_interview_duration_stats(db: StandardDatabase) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        fm = config.field_mapping
        is_adult   = fm.is_adult
        is_child   = fm.is_child
        is_neonate = fm.is_neonate
        gender     = fm.deceased_gender
        col        = db_collections.VA_TABLE

        query = f"""
        LET records = (
            FOR doc IN {col}
            LET start_raw = doc.id10011
            LET end_raw   = doc.id10481
            FILTER start_raw != null AND start_raw != ""
            FILTER end_raw   != null AND end_raw   != ""
            LET duration_ms = DATE_TIMESTAMP(end_raw) - DATE_TIMESTAMP(start_raw)
            FILTER duration_ms > 0
            FILTER duration_ms < 86400000
            RETURN {{
                v:       duration_ms / 60000.0,
                adult:   doc.{is_adult},
                child:   doc.{is_child},
                neonate: doc.{is_neonate},
                gender:  doc.{gender}
            }}
        )

        LET all_r     = records
        LET adult_r   = (FOR r IN records FILTER r.adult   == "1" RETURN r)
        LET child_r   = (FOR r IN records FILTER r.child   == "1" RETURN r)
        LET neonate_r = (FOR r IN records FILTER r.neonate == "1" RETURN r)
        LET m_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "male"   RETURN r)
        LET f_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "female" RETURN r)

        LET s_all     = {_stat_block('all_r',     'v')}
        LET s_adults  = {_stat_block('adult_r',   'v')}
        LET s_child   = {_stat_block('child_r',   'v')}
        LET s_neonate = {_stat_block('neonate_r', 'v')}
        LET s_m_adult = {_stat_block('m_adult_r', 'v')}
        LET s_f_adult = {_stat_block('f_adult_r', 'v')}

        RETURN {{
            overall: s_all,
            by_age_group: {{ adults: s_adults, children: s_child, neonates: s_neonate }},
            by_gender_adult: {{ male_adults: s_m_adult, female_adults: s_f_adult }}
        }}
        """

        def run():
            cursor = db.aql.execute(query)
            results = list(cursor)
            return results[0] if results else None

        data = await run_in_threadpool(run)
        return ResponseMainModel(data=data, message="Interview duration statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch interview duration statistics", error=str(e))


# ---------------------------------------------------------------------------
# Informative Completeness Score (ICS) Stats
# ---------------------------------------------------------------------------
async def fetch_ics_stats(db: StandardDatabase) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        fm = config.field_mapping
        is_adult   = fm.is_adult
        is_child   = fm.is_child
        is_neonate = fm.is_neonate
        gender     = fm.deceased_gender
        col        = db_collections.VA_TABLE

        # Build exclusion list from all known non-binary fields
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
            # Interview timing fields (datetime strings, not binary responses)
            'id10011', 'id10481', 'id10012', 'id10023',
        ])
        excluded_list = list(excluded)

        query = f"""
        LET records = (
            FOR doc IN {col}

            // Count binary-question fields per document
            LET binary_vals = (
                FOR attr IN ATTRIBUTES(doc, true)
                FILTER attr NOT IN @excluded_fields
                LET v = doc[attr]
                FILTER IS_STRING(v)
                LET norm = LOWER(TRIM(v))
                FILTER norm IN ["yes", "no", "dk", "ref"]
                RETURN norm
            )

            LET informative = LENGTH(FOR bv IN binary_vals FILTER bv IN ["yes", "no"] RETURN bv)
            LET total       = LENGTH(binary_vals)
            FILTER total > 0

            RETURN {{
                v:       TO_NUMBER(informative) / TO_NUMBER(total),
                adult:   doc.{is_adult},
                child:   doc.{is_child},
                neonate: doc.{is_neonate},
                gender:  doc.{gender}
            }}
        )

        LET all_r     = records
        LET adult_r   = (FOR r IN records FILTER r.adult   == "1" RETURN r)
        LET child_r   = (FOR r IN records FILTER r.child   == "1" RETURN r)
        LET neonate_r = (FOR r IN records FILTER r.neonate == "1" RETURN r)
        LET m_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "male"   RETURN r)
        LET f_adult_r = (FOR r IN records FILTER r.adult == "1" AND r.gender == "female" RETURN r)

        LET s_all     = {_stat_block('all_r',     'v')}
        LET s_adults  = {_stat_block('adult_r',   'v')}
        LET s_child   = {_stat_block('child_r',   'v')}
        LET s_neonate = {_stat_block('neonate_r', 'v')}
        LET s_m_adult = {_stat_block('m_adult_r', 'v')}
        LET s_f_adult = {_stat_block('f_adult_r', 'v')}

        RETURN {{
            overall: s_all,
            by_age_group: {{ adults: s_adults, children: s_child, neonates: s_neonate }},
            by_gender_adult: {{ male_adults: s_m_adult, female_adults: s_f_adult }}
        }}
        """

        bind_vars = {"excluded_fields": excluded_list}

        def run():
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            results = list(cursor)
            return results[0] if results else None

        data = await run_in_threadpool(run)
        return ResponseMainModel(data=data, message="ICS statistics fetched successfully")

    except Exception as e:
        return ResponseMainModel(data=None, message="Failed to fetch ICS statistics", error=str(e))
