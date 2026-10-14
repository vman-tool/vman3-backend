from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from app.data_quality.services.general_dqa import fetch_rrs_stats, _filter_df_by_location
from tests.support.fakes import FakeDB

REGION_FIELD = "id10005r"

RESTRICTED_USER = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
UNRESTRICTED_USER = {}


def _patched_odk_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=SimpleNamespace(
            is_adult="id10059", is_child="", is_neonate="", deceased_gender="id10019",
        ))
    return patch("app.data_quality.services.general_dqa.fetch_odk_config", new=fake_fetch_odk_config)


class TestFilterDfByLocation:
    def test_unrestricted_user_is_a_no_op(self):
        df = pd.DataFrame({REGION_FIELD: ["Dodoma", "Dar es Salaam"]})
        assert len(_filter_df_by_location(df, UNRESTRICTED_USER)) == 2

    def test_restricted_user_keeps_only_matching_rows(self):
        df = pd.DataFrame({REGION_FIELD: ["Dodoma", "Dar es Salaam", "Dodoma"]})
        filtered = _filter_df_by_location(df, RESTRICTED_USER)
        assert list(filtered[REGION_FIELD]) == ["Dodoma", "Dodoma"]


class TestFetchRrsStatsLiveScoping:
    async def test_restricted_current_user_filters_the_live_dataframe_before_computing(self):
        df = pd.DataFrame({
            REGION_FIELD: ["Dodoma", "Dar es Salaam"],
            "id10059": ["1", "1"],
            "id10019": ["male", "male"],
        })
        fake_db = FakeDB()

        with _patched_odk_config(), patch(
            "app.data_quality.services.general_dqa.compute_rrs",
            side_effect=lambda frame: pd.Series([80.0] * len(frame), index=frame.index),
        ):
            result = await fetch_rrs_stats(fake_db, df, current_user=RESTRICTED_USER)

        # Only the Dodoma row should have survived the location filter.
        assert result.data["overall"]["count"] == 1

    async def test_unrestricted_current_user_sees_every_row(self):
        df = pd.DataFrame({
            REGION_FIELD: ["Dodoma", "Dar es Salaam"],
            "id10059": ["1", "1"],
            "id10019": ["male", "male"],
        })
        fake_db = FakeDB()

        with _patched_odk_config(), patch(
            "app.data_quality.services.general_dqa.compute_rrs",
            side_effect=lambda frame: pd.Series([80.0] * len(frame), index=frame.index),
        ):
            result = await fetch_rrs_stats(fake_db, df, current_user=UNRESTRICTED_USER)

        assert result.data["overall"]["count"] == 2

    async def test_precomputed_series_bypasses_live_filtering(self):
        # When the nightly batch job hands in its own already-computed
        # series, current_user is never passed - this asserts the
        # "series is None" guard, not a live call shape.
        df = pd.DataFrame({
            REGION_FIELD: ["Dodoma", "Dar es Salaam"],
            "id10059": ["1", "1"],
            "id10019": ["male", "male"],
        })
        series = pd.Series([80.0, 40.0], index=df.index)
        fake_db = FakeDB()

        with _patched_odk_config():
            result = await fetch_rrs_stats(fake_db, df, series=series)

        assert result.data["overall"]["count"] == 2
