from types import SimpleNamespace
from unittest.mock import patch

from app.records.services.export_va_records import _build_export_dataframes
from tests.support.fakes import FakeCursor, FakeDB


def _fake_config():
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            instance_id="instanceid",
            interview_date="id10012",
            death_date="id10023",
            submitted_date="submissiondate",
        )
    )


def _patch_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config()
    return patch("app.records.services.export_va_records.fetch_odk_config", new=fake_fetch_odk_config)


TYPES = [
    {"uuid": "type-communicable", "name": "Group I: Communicable"},
    {"uuid": "type-ncd", "name": "Group II: Non-Communicable"},
]
CATEGORIES = [
    {"uuid": "cat-infectious", "name": "Infectious and parasitic diseases", "type": "type-communicable"},
    {"uuid": "cat-circulatory", "name": "Diseases of the circulatory system", "type": "type-ncd"},
]
ICD10 = [
    {"uuid": "icd-sepsis", "code": "VAs-01.01", "name": "Sepsis", "category": "cat-infectious"},
    {"uuid": "icd-stroke", "code": "VAs-04.02", "name": "Stroke", "category": "cat-circulatory"},
]


def _seed_icd10_tables(fake_db: FakeDB):
    for doc in ICD10:
        fake_db.collection("icd10").insert(doc)
    for doc in CATEGORIES:
        fake_db.collection("icd10_category").insert(doc)
    for doc in TYPES:
        fake_db.collection("icd10_category_type").insert(doc)


def _responder(va_records=None, pcva_records=None, ccva_rows=None, icd10_rows=None, va_cause_rows=None):
    """Dispatches by a distinctive substring in each query, mirroring the
    pattern used elsewhere in this suite (see test_list_data.py)."""
    va_records = va_records or []
    pcva_records = pcva_records or []
    ccva_rows = ccva_rows if ccva_rows is not None else []
    icd10_rows = icd10_rows or []
    va_cause_rows = va_cause_rows if va_cause_rows is not None else []

    def responder(query: str, bind_vars):
        q = " ".join(query.split())
        if "FOR va IN" in q:
            return FakeCursor(va_records)
        if "FOR pcva IN" in q:
            return FakeCursor(pcva_records)
        if "RETURN { uuid: code.uuid" in q:
            return FakeCursor(icd10_rows)
        if "AGGREGATE top_cause = MAX(ccva.CAUSE1)" in q and "RETURN { va_id: va_id, top_cause: top_cause }" in q:
            return FakeCursor(va_cause_rows)
        if "FOR ccva IN" in q:
            return FakeCursor(ccva_rows)
        return FakeCursor([])

    return responder


class TestVaDataSheetGbdColumns:
    # Sheet 1 has no cause-of-death field of its own - Broad/Major must be
    # pulled from each VA's own CCVA run (CAUSE1), the same source the CCVA
    # sheet uses, so plain "VA Data" exports (no PCVA/CCVA sheets ticked)
    # still carry GBD classification like the CCVA module's own download.
    async def test_adds_broad_and_major_category_from_the_vas_ccva_run(self):
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}, {"instanceid": "va-2"}],
            va_cause_rows=[{"va_id": "va-1", "top_cause": "Stroke"}],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            df_va, _, _ = await _build_export_dataframes(current_user={}, db=fake_db)

        assert list(df_va.columns[:3]) == ['VA ID', 'Broad Category', 'Major Category']
        row1 = df_va[df_va['VA ID'] == 'va-1'].iloc[0]
        assert row1['Major Category'] == 'Diseases of the circulatory system'
        assert row1['Broad Category'] == 'Group II: Non-Communicable'

    async def test_leaves_broad_major_blank_for_a_va_with_no_completed_ccva_run(self):
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            va_cause_rows=[],  # no CCVA result for va-1
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            df_va, _, _ = await _build_export_dataframes(current_user={}, db=fake_db)

        row1 = df_va[df_va['VA ID'] == 'va-1'].iloc[0]
        assert row1['Broad Category'] == ''
        assert row1['Major Category'] == ''

    async def test_populates_va_data_sheet_even_when_ccva_sheet_is_not_requested(self):
        # The checkbox only controls whether a separate "CCVA Results" sheet
        # is added - it must not gate the VA Data sheet's own GBD columns.
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            va_cause_rows=[{"va_id": "va-1", "top_cause": "Stroke"}],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            df_va, df_pcva, df_ccva = await _build_export_dataframes(
                current_user={}, db=fake_db, include_pcva=False, include_ccva=False,
            )

        assert df_pcva is None
        assert df_ccva is None
        assert df_va.iloc[0]['Major Category'] == 'Diseases of the circulatory system'


class TestCcvaSheetGbdColumns:
    async def test_adds_broad_and_major_category_for_the_top_cause(self):
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            ccva_rows=[{
                "va_id": "va-1", "top_cause": "Stroke", "cause2": None, "cause3": None,
                "likelihood": 0.8, "age_group": "adult", "gender": "male",
            }],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            _, _, df_ccva = await _build_export_dataframes(
                current_user={}, db=fake_db, include_ccva=True,
            )

        assert list(df_ccva.columns[:4]) == [
            'VA ID', 'Top Cause (Cause 1)', 'Broad Category', 'Major Category'
        ]
        row = df_ccva.iloc[0]
        assert row['Major Category'] == 'Diseases of the circulatory system'
        assert row['Broad Category'] == 'Group II: Non-Communicable'

    async def test_blank_when_the_cause_does_not_match_the_target_list(self):
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            ccva_rows=[{
                "va_id": "va-1", "top_cause": "Completely Unmapped Cause", "cause2": None,
                "cause3": None, "likelihood": 0.5, "age_group": "adult", "gender": "male",
            }],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            _, _, df_ccva = await _build_export_dataframes(
                current_user={}, db=fake_db, include_ccva=True,
            )

        row = df_ccva.iloc[0]
        assert row['Broad Category'] == ''
        assert row['Major Category'] == ''


class TestPcvaSheetGbdColumns:
    # PCVA already stores the precise ICD10 uuid a coder picked (frameA.a/
    # b/c/d), so classification goes through the icd10 uuid -> category
    # chain directly rather than classify_cause()'s name-matching.
    async def test_adds_broad_and_major_category_for_the_underlying_cause(self):
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            pcva_records=[{
                "va_id": "va-1", "coder": "coder@example.com", "coded_at": "2026-01-01",
                "cause_a": "icd-stroke", "cause_b": None, "cause_c": None, "cause_d": None,
                "contributories": None,
            }],
            icd10_rows=[{"uuid": "icd-stroke", "code": "VAs-04.02", "name": "Stroke"}],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            _, df_pcva, _ = await _build_export_dataframes(
                current_user={}, db=fake_db, include_pcva=True,
            )

        assert 'Broad Category' in df_pcva.columns
        assert 'Major Category' in df_pcva.columns
        row = df_pcva.iloc[0]
        assert row['Underlying Cause'] == '(VAs-04.02) Stroke'
        assert row['Major Category'] == 'Diseases of the circulatory system'
        assert row['Broad Category'] == 'Group II: Non-Communicable'

    async def test_prefers_the_most_specific_cause_slot_for_classification(self):
        # underlying_cause itself already picks d > c > b > a (most specific
        # first) - the GBD classification must follow the exact same slot,
        # not always classify cause_a.
        fake_db = FakeDB(responder=_responder(
            va_records=[{"instanceid": "va-1"}],
            pcva_records=[{
                "va_id": "va-1", "coder": "coder@example.com", "coded_at": "2026-01-01",
                "cause_a": "icd-sepsis", "cause_b": None, "cause_c": None, "cause_d": "icd-stroke",
                "contributories": None,
            }],
            icd10_rows=[
                {"uuid": "icd-stroke", "code": "VAs-04.02", "name": "Stroke"},
                {"uuid": "icd-sepsis", "code": "VAs-01.01", "name": "Sepsis"},
            ],
        ))
        _seed_icd10_tables(fake_db)

        with _patch_config():
            _, df_pcva, _ = await _build_export_dataframes(
                current_user={}, db=fake_db, include_pcva=True,
            )

        row = df_pcva.iloc[0]
        assert row['Underlying Cause'] == '(VAs-04.02) Stroke'
        assert row['Major Category'] == 'Diseases of the circulatory system'
