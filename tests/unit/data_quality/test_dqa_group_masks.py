from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from app.data_quality.services.general_dqa import _breakdowns, _dqa_group_masks


def _fake_field_mapping():
    return SimpleNamespace(
        is_adult="isadult", is_child="ischild", is_neonate="isneonatal",
        deceased_gender="id10019",
    )


def _patched_odk_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=_fake_field_mapping())
    return patch("app.data_quality.services.general_dqa.fetch_odk_config", new=fake_fetch_odk_config)


class TestDqaGroupMasks:
    async def test_matches_clean_integer_flags(self):
        df = pd.DataFrame({
            "isadult": [1, 0, 0],
            "ischild": [0, 1, 0],
            "isneonatal": [0, 0, 1],
            "id10019": ["male", "female", "male"],
        })

        with _patched_odk_config():
            masks = await _dqa_group_masks(None, df)

        assert masks["adult"].tolist() == [True, False, False]
        assert masks["child"].tolist() == [False, True, False]
        assert masks["neonate"].tolist() == [False, False, True]

    async def test_matches_when_any_row_in_the_collection_has_a_missing_flag(self):
        # Regression test for the real bug: a single missing (NaN) value
        # anywhere in an is_adult/is_child/is_neonate column forces pandas
        # to upcast the WHOLE column from int64 to float64 (int64 can't
        # represent NaN). `.astype(str)` on that float column then yields
        # "1.0"/"0.0"/"nan" for every row, none of which equal "1" - so a
        # single missing value among thousands of records used to zero out
        # every single age-group/gender breakdown count, not just the row
        # that was actually missing data.
        df = pd.DataFrame({
            "isadult": [1, 0, None],
            "ischild": [0, 1, None],
            "isneonatal": [0, 0, None],
            "id10019": ["male", "female", "male"],
        })
        assert df["isadult"].dtype.kind == "f"  # confirms the float upcast actually happened

        with _patched_odk_config():
            masks = await _dqa_group_masks(None, df)

        assert masks["adult"].tolist() == [True, False, False]
        assert masks["child"].tolist() == [False, True, False]
        # The genuinely-missing row correctly counts as neither.
        assert masks["neonate"].tolist() == [False, False, False]

    async def test_matches_string_encoded_flags_too(self):
        df = pd.DataFrame({
            "isadult": ["1", "0"],
            "ischild": ["0", "1"],
            "isneonatal": ["0", "0"],
            "id10019": ["male", "female"],
        })

        with _patched_odk_config():
            masks = await _dqa_group_masks(None, df)

        assert masks["adult"].tolist() == [True, False]

    async def test_male_and_female_adult_masks_combine_adult_and_gender(self):
        df = pd.DataFrame({
            "isadult": [1, 1, 0],
            "ischild": [0, 0, 1],
            "isneonatal": [0, 0, 0],
            "id10019": ["male", "female", "female"],
        })

        with _patched_odk_config():
            masks = await _dqa_group_masks(None, df)

        assert masks["male_adult"].tolist() == [True, False, False]
        assert masks["female_adult"].tolist() == [False, True, False]


class TestBreakdownsEndToEnd:
    async def test_by_age_group_counts_are_not_zeroed_by_a_missing_value_elsewhere(self):
        df = pd.DataFrame({
            "isadult": [1, 0, 0, None],
            "ischild": [0, 1, 0, None],
            "isneonatal": [0, 0, 1, None],
            "id10019": ["male", "female", "male", "female"],
        })
        series = pd.Series([80.0, 60.0, 90.0, 70.0], index=df.index)

        with _patched_odk_config():
            result = await _breakdowns(None, df, series)

        assert result["overall"]["count"] == 4
        assert result["by_age_group"]["adults"]["count"] == 1
        assert result["by_age_group"]["children"]["count"] == 1
        assert result["by_age_group"]["neonates"]["count"] == 1
