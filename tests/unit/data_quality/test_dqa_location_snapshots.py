from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from app.data_quality.services.general_dqa import (
    compute_and_store_dqa_location_snapshots,
    _location_snapshot_key,
)
from app.data_quality.services.dqa_analytics_service import fetch_dqa_analytics_snapshot
from app.shared.configs.constants import db_collections
from tests.support.fakes import FakeDB

REGION_FIELD = "id10005r"


def _patched_odk_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=SimpleNamespace(
            location_level1=REGION_FIELD, location_level2=None, location_level3=None,
            is_adult="id10059", is_child="", is_neonate="", deceased_gender="id10019",
        ))
    return patch("app.data_quality.services.general_dqa.fetch_odk_config", new=fake_fetch_odk_config)


class TestComputeAndStoreDqaLocationSnapshots:
    async def test_writes_one_doc_per_distinct_location_value_with_correctly_sliced_stats(self):
        df = pd.DataFrame({
            "_key": ["va-1", "va-2", "va-3"],
            REGION_FIELD: ["Dodoma", "Dodoma", "Dar es Salaam"],
            "id10010": ["intA", "intB", "intA"],
            "id10059": ["1", "1", "1"],
            "id10019": ["male", "female", "male"],
        })
        rrs = pd.Series([80.0, 60.0, 40.0], index=df.index)
        ics = pd.Series([90.0, 70.0, 50.0], index=df.index)
        aid = pd.Series([30.0, 20.0, 10.0], index=df.index)
        ici_series = pd.Series([100.0, 0.0, 100.0], index=df.index)
        ici_flags = pd.DataFrame({"C1": [False, True, False]}, index=df.index)

        fake_db = FakeDB()

        with _patched_odk_config():
            written = await compute_and_store_dqa_location_snapshots(
                fake_db, df, rrs, ics, aid, ici_series, ici_flags
            )

        assert written == 2
        dodoma_key = _location_snapshot_key(REGION_FIELD, "Dodoma")
        dsm_key = _location_snapshot_key(REGION_FIELD, "Dar es Salaam")
        col = fake_db.collection(db_collections.DQA_ANALYTICS)

        dodoma_doc = col.get(dodoma_key)
        assert dodoma_doc["field"] == REGION_FIELD
        assert dodoma_doc["value"] == "Dodoma"
        # Dodoma's 2 records (rrs 80, 60) -> mean 70, matching a direct
        # slice of the already-computed series, not a recompute.
        assert dodoma_doc["rrs"]["overall"]["avg"] == 70.0
        assert dodoma_doc["rrs"]["overall"]["count"] == 2

        dsm_doc = col.get(dsm_key)
        assert dsm_doc["value"] == "Dar es Salaam"
        assert dsm_doc["rrs"]["overall"]["avg"] == 40.0
        assert dsm_doc["rrs"]["overall"]["count"] == 1

    async def test_never_recomputes_indicators_itself(self):
        # Regression test for the core design point: this function must only
        # ever slice the Series it's handed, never call vman_dq's compute_*
        # functions on its own - that would defeat the entire point of
        # computing them once in compute_and_store_dqa_analytics.
        df = pd.DataFrame({
            "_key": ["va-1"],
            REGION_FIELD: ["Dodoma"],
            "id10010": ["intA"],
            "id10059": ["1"],
            "id10019": ["male"],
        })
        series = pd.Series([80.0], index=df.index)
        flags = pd.DataFrame({"C1": [False]}, index=df.index)
        fake_db = FakeDB()

        with _patched_odk_config(), \
             patch("app.data_quality.services.general_dqa.compute_rrs") as mock_rrs, \
             patch("app.data_quality.services.general_dqa._compute_ics_chunked") as mock_ics, \
             patch("app.data_quality.services.general_dqa.compute_aid") as mock_aid, \
             patch("app.data_quality.services.general_dqa.compute_ici") as mock_ici:
            await compute_and_store_dqa_location_snapshots(fake_db, df, series, series, series, series, flags)

        mock_rrs.assert_not_called()
        mock_ics.assert_not_called()
        mock_aid.assert_not_called()
        mock_ici.assert_not_called()

    async def test_empty_dataframe_writes_nothing(self):
        fake_db = FakeDB()
        empty = pd.Series([], dtype=float)

        written = await compute_and_store_dqa_location_snapshots(
            fake_db, pd.DataFrame(), empty, empty, empty, empty, pd.DataFrame()
        )

        assert written == 0


class TestFetchDqaAnalyticsSnapshot:
    async def test_unrestricted_user_reads_the_global_snapshot_unchanged(self):
        fake_db = FakeDB()
        fake_db.collection(db_collections.DQA_ANALYTICS).insert(
            {"_key": "snapshot", "status": "completed", "computed_at": "2026-01-01T00:00:00Z", "rrs": {"x": 1}}
        )

        result = await fetch_dqa_analytics_snapshot(fake_db, {})

        assert result["rrs"] == {"x": 1}

    async def test_restricted_single_value_returns_that_locations_doc_verbatim(self):
        fake_db = FakeDB()
        key = _location_snapshot_key(REGION_FIELD, "Dodoma")
        doc = {
            "_key": key, "status": "completed", "computed_at": "2026-01-01T00:00:00Z",
            "rrs": {
                "overall": {"avg": 70.0, "min_v": 60.0, "max_v": 80.0, "stddev": 10.0, "p50": 70.0, "count": 2},
                "by_age_group": {"adults": {}, "children": {}, "neonates": {}},
                "by_gender_adult": {"male_adults": {}, "female_adults": {}},
            },
            "ics": {"overall": {}, "by_age_group": {}, "by_gender_adult": {}},
            "aid": {"overall": {}, "by_age_group": {}, "by_gender_adult": {}},
            "ici": {"overall_ici": 100.0, "overall_total": 2, "overall_passed": 2, "interviewers": [], "checks_applied": []},
        }
        fake_db.collection(db_collections.DQA_ANALYTICS).insert(doc)
        user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        result = await fetch_dqa_analytics_snapshot(fake_db, user)

        assert result["rrs"]["overall"]["avg"] == 70.0
        assert result["rrs"]["overall"]["p50_approximate"] is False

    async def test_restricted_user_with_no_matching_doc_returns_empty_shape_not_a_crash(self):
        fake_db = FakeDB()
        user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Nowhere"}]}}

        result = await fetch_dqa_analytics_snapshot(fake_db, user)

        assert result["status"] == "completed"
        assert result["rrs"] is None
