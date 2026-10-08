from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from app.data_quality.services.general_dqa import (
    compute_and_store_dqa_trend_points,
    fetch_dqa_trend_points,
)
from app.shared.configs.constants import db_collections
from tests.support.fakes import FakeCursor, FakeDB

REGION_FIELD = "id10005r"
DISTRICT_FIELD = "id10005d"
WARD_FIELD = "id10005w"
SUBMITTED_FIELD = "submissiondate"


def _fake_field_mapping(location_level2=DISTRICT_FIELD, location_level3=WARD_FIELD):
    return SimpleNamespace(
        location_level1=REGION_FIELD, location_level2=location_level2, location_level3=location_level3,
        submitted_date=SUBMITTED_FIELD, deceased_gender="id10019",
    )


def _patched_odk_config(location_level2=DISTRICT_FIELD, location_level3=WARD_FIELD):
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=_fake_field_mapping(location_level2, location_level3))
    return patch(
        "app.data_quality.services.general_dqa.fetch_odk_config",
        new=fake_fetch_odk_config,
    )


def _patched_compute_functions(rrs, ics, aid, ici):
    return (
        patch("app.data_quality.services.general_dqa.compute_rrs", return_value=rrs),
        patch("app.data_quality.services.general_dqa._compute_ics_chunked", return_value=ics),
        patch("app.data_quality.services.general_dqa.compute_aid", return_value=aid),
        patch("app.data_quality.services.general_dqa.compute_ici", return_value=(ici, pd.DataFrame(), {})),
    )


async def test_stores_one_row_per_record_bucketed_to_its_submission_month():
    df = pd.DataFrame({
        "_key": ["va-1", "va-2"],
        SUBMITTED_FIELD: ["2026-01-05T08:30:00Z", "2026-02-10"],
        REGION_FIELD: ["Dodoma", "Dar es Salaam"],
        DISTRICT_FIELD: ["Kongwa", "Ilala"],
        WARD_FIELD: ["WardA", "WardB"],
    })
    rrs = pd.Series([80.0, 40.0], index=df.index)
    ics = pd.Series([95.0, np.nan], index=df.index)
    aid = pd.Series([45.0, 20.0], index=df.index)
    ici = pd.Series([100.0, 60.0], index=df.index)

    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(rrs, ics, aid, ici)
        with patches[0], patches[1], patches[2], patches[3]:
            count = await compute_and_store_dqa_trend_points(fake_db, df)

    assert count == 2
    rows = fake_db.collection(db_collections.DQA_TREND_POINTS).inserted
    assert fake_db.collection(db_collections.DQA_TREND_POINTS).truncated is True
    assert rows[0] == {
        "_key": "va-1", "month": "2026-01",
        REGION_FIELD: "Dodoma", DISTRICT_FIELD: "Kongwa", WARD_FIELD: "WardA",
        "rrs": 80.0, "ics": 95.0, "ici": 100.0, "aid": 45.0,
    }
    assert rows[1]["month"] == "2026-02"
    # NaN ICS is stored as None, not NaN (NaN is not valid JSON/AQL).
    assert rows[1]["ics"] is None


async def test_keeps_records_with_no_gps_unlike_the_map_points_cache():
    # The actual bug this guards against: compute_and_store_dqa_map_points
    # silently drops any record with no coordinates, which would skew a
    # trend toward only geo-tagged submissions. Trend points have no GPS
    # requirement at all - only a parseable submission date.
    df = pd.DataFrame({
        "_key": ["va-1"],
        "coordinates": [None],
        SUBMITTED_FIELD: ["2026-01-05"],
        REGION_FIELD: ["Dodoma"],
        DISTRICT_FIELD: ["Kongwa"],
        WARD_FIELD: ["WardA"],
    })
    series = pd.Series([80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            count = await compute_and_store_dqa_trend_points(fake_db, df)

    assert count == 1
    assert fake_db.collection(db_collections.DQA_TREND_POINTS).inserted[0]["_key"] == "va-1"


async def test_skips_records_with_no_parseable_submission_date():
    df = pd.DataFrame({
        "_key": ["va-1", "va-2", "va-3"],
        SUBMITTED_FIELD: [None, "", "2026-03-01"],
        REGION_FIELD: ["Dodoma", "Dodoma", "Dodoma"],
        DISTRICT_FIELD: ["Kongwa", "Kongwa", "Kongwa"],
        WARD_FIELD: ["WardA", "WardA", "WardA"],
    })
    series = pd.Series([80.0, 80.0, 80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            count = await compute_and_store_dqa_trend_points(fake_db, df)

    assert count == 1
    assert fake_db.collection(db_collections.DQA_TREND_POINTS).inserted[0]["_key"] == "va-3"


async def test_stores_location_under_the_deployments_actual_raw_field_name():
    df = pd.DataFrame({
        "_key": ["va-1"],
        SUBMITTED_FIELD: ["2026-01-05"],
        REGION_FIELD: ["Dar es Salaam"],
        DISTRICT_FIELD: ["Ilala"],
        WARD_FIELD: ["WardB"],
    })
    series = pd.Series([80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            await compute_and_store_dqa_trend_points(fake_db, df)

    row = fake_db.collection(db_collections.DQA_TREND_POINTS).inserted[0]
    assert row[REGION_FIELD] == "Dar es Salaam"
    assert "region" not in row


async def test_omits_a_location_level_entirely_when_its_field_is_not_configured():
    df = pd.DataFrame({
        "_key": ["va-1"],
        SUBMITTED_FIELD: ["2026-01-05"],
        REGION_FIELD: ["Dodoma"],
    })
    series = pd.Series([80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config(location_level2=None, location_level3=None):
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            await compute_and_store_dqa_trend_points(fake_db, df)

    row = fake_db.collection(db_collections.DQA_TREND_POINTS).inserted[0]
    assert DISTRICT_FIELD not in row
    assert WARD_FIELD not in row


async def test_empty_dataframe_truncates_without_inserting():
    fake_db = FakeDB()
    df = pd.DataFrame()

    count = await compute_and_store_dqa_trend_points(fake_db, df)

    assert count == 0
    assert fake_db.collection(db_collections.DQA_TREND_POINTS).truncated is True
    assert fake_db.collection(db_collections.DQA_TREND_POINTS).inserted == []


async def test_fetch_applies_the_requesting_users_location_access_limit():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))
    current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

    await fetch_dqa_trend_points(current_user=current_user, db=fake_db)

    query, bind_vars = fake_db.aql.queries[0]
    assert f"doc.{REGION_FIELD}" in query
    assert "Dodoma" in bind_vars["locationValues0"]


async def test_fetch_returns_the_expected_point_shape():
    point = {"month": "2026-01", "rrs": 80.0, "ics": 95.0, "ici": 100.0, "aid": 45.0}
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([point]))

    result = await fetch_dqa_trend_points(current_user={}, db=fake_db)

    assert result.error is None or result.error is False
    assert result.data == [point]
