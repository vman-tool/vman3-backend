from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from app.data_quality.services.general_dqa import (
    compute_and_store_dqa_map_points,
    fetch_dqa_map_points,
)
from app.shared.configs.constants import db_collections
from tests.support.fakes import FakeCursor, FakeDB

# Deliberately NOT the friendly words "region"/"district"/"ward" - a real
# deployment's field_mapping maps location levels to raw ODK field names
# like these (confirmed against the actual dev database), and storing rows
# under the wrong key here is exactly the bug this file guards against: the
# location filter (build_locations_query_filter/build_location_limit_filter,
# shared with every other location-filterable view) always interpolates
# doc.<this raw field name>, so if these tests used the same word as the
# code's own (former) hardcoded key, they would pass even with that bug
# still in place.
REGION_FIELD = "id10005r"
DISTRICT_FIELD = "id10005d"
WARD_FIELD = "id10005w"


def _fake_field_mapping(location_level2=DISTRICT_FIELD, location_level3=WARD_FIELD):
    return SimpleNamespace(
        location_level1=REGION_FIELD, location_level2=location_level2, location_level3=location_level3,
        interview_date="id10012", deceased_gender="id10019",
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


async def test_stores_one_row_per_record_with_coordinates_zipped_to_each_indicator_score():
    df = pd.DataFrame({
        "_key": ["va-1", "va-2"],
        "coordinates": [[34.8, -6.3, 0], [39.2, -6.8, 0]],
        REGION_FIELD: ["Dodoma", "Dar es Salaam"],
        DISTRICT_FIELD: ["Kongwa", "Ilala"],
        WARD_FIELD: ["WardA", "WardB"],
        "id10012": ["2026-01-05", "2026-02-10"],
    })
    rrs = pd.Series([80.0, 40.0], index=df.index)
    ics = pd.Series([95.0, np.nan], index=df.index)
    aid = pd.Series([45.0, 20.0], index=df.index)
    ici = pd.Series([100.0, 60.0], index=df.index)

    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(rrs, ics, aid, ici)
        with patches[0], patches[1], patches[2], patches[3]:
            count = await compute_and_store_dqa_map_points(fake_db, df)

    assert count == 2
    rows = fake_db.collection(db_collections.DQA_MAP_POINTS).inserted
    assert fake_db.collection(db_collections.DQA_MAP_POINTS).truncated is True
    assert rows[0] == {
        "_key": "va-1", "lat": -6.3, "lng": 34.8,
        REGION_FIELD: "Dodoma", DISTRICT_FIELD: "Kongwa", WARD_FIELD: "WardA",
        "date": "2026-01-05", "rrs": 80.0, "ics": 95.0, "ici": 100.0, "aid": 45.0,
    }
    # NaN ICS is stored as None, not NaN (NaN is not valid JSON/AQL).
    assert rows[1]["ics"] is None
    assert rows[1]["rrs"] == 40.0


async def test_stores_location_under_the_deployments_actual_raw_field_name_not_a_fixed_label():
    # Regression test for the actual bug reported live: filtering the Data
    # Map by any location made every DQA-colored point vanish, because the
    # stored key ("region") never matched what the filter looked for
    # (doc.id10005r) - passes only once rows are keyed by fm.location_level1
    # itself, whatever that raw field happens to be for this deployment.
    df = pd.DataFrame({
        "_key": ["va-1"],
        "coordinates": [[34.8, -6.3, 0]],
        REGION_FIELD: ["Dar es Salaam"],
        DISTRICT_FIELD: ["Ilala"],
        WARD_FIELD: ["WardB"],
        "id10012": ["2026-01-05"],
    })
    series = pd.Series([80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            await compute_and_store_dqa_map_points(fake_db, df)

    row = fake_db.collection(db_collections.DQA_MAP_POINTS).inserted[0]
    assert row[REGION_FIELD] == "Dar es Salaam"
    assert "region" not in row
    assert row[DISTRICT_FIELD] == "Ilala"
    assert row[WARD_FIELD] == "WardB"


async def test_omits_a_location_level_entirely_when_its_field_is_not_configured():
    # Mirrors map_data.py's own district_line handling for a deployment
    # that only maps Admin Level 1 - a falsy field name must not become a
    # literal key (e.g. "None") on every stored row.
    df = pd.DataFrame({
        "_key": ["va-1"],
        "coordinates": [[34.8, -6.3, 0]],
        REGION_FIELD: ["Dodoma"],
        "id10012": ["2026-01-05"],
    })
    series = pd.Series([80.0], index=df.index)
    fake_db = FakeDB()

    with _patched_odk_config(location_level2=None, location_level3=None):
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            await compute_and_store_dqa_map_points(fake_db, df)

    row = fake_db.collection(db_collections.DQA_MAP_POINTS).inserted[0]
    assert row[REGION_FIELD] == "Dodoma"
    assert DISTRICT_FIELD not in row
    assert WARD_FIELD not in row
    assert None not in row


async def test_skips_records_with_no_gps_coordinates():
    df = pd.DataFrame({
        "_key": ["va-1", "va-2"],
        "coordinates": [None, [39.2, -6.8, 0]],
        REGION_FIELD: ["Dodoma", "Dar es Salaam"],
        DISTRICT_FIELD: [None, "Ilala"],
        WARD_FIELD: [None, "WardB"],
        "id10012": ["2026-01-05", "2026-02-10"],
    })
    n = len(df)
    series = pd.Series([np.nan] * n, index=df.index)

    fake_db = FakeDB()

    with _patched_odk_config():
        patches = _patched_compute_functions(series, series, series, series)
        with patches[0], patches[1], patches[2], patches[3]:
            count = await compute_and_store_dqa_map_points(fake_db, df)

    assert count == 1
    rows = fake_db.collection(db_collections.DQA_MAP_POINTS).inserted
    assert rows[0]["_key"] == "va-2"


async def test_empty_dataframe_truncates_without_inserting():
    fake_db = FakeDB()
    df = pd.DataFrame()

    count = await compute_and_store_dqa_map_points(fake_db, df)

    assert count == 0
    assert fake_db.collection(db_collections.DQA_MAP_POINTS).truncated is True
    assert fake_db.collection(db_collections.DQA_MAP_POINTS).inserted == []


async def test_fetch_applies_date_and_location_filters():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    await fetch_dqa_map_points(
        current_user={},
        start_date="2026-01-01",
        end_date="2026-01-31",
        locations=f'[{{"field":"{REGION_FIELD}","value":"Dar es Salaam"}}]',
        db=fake_db,
    )

    query, bind_vars = fake_db.aql.queries[0]
    assert "doc.date >= @start_date" in query
    assert "doc.date <= @end_date" in query
    # The filter interpolates whatever raw field name the frontend sends -
    # confirming it lines up with the same key compute_and_store_dqa_map_
    # points now writes rows under (see the "raw field name" test above).
    assert f"doc.{REGION_FIELD}" in query
    assert bind_vars["start_date"] == "2026-01-01"
    assert bind_vars["end_date"] == "2026-01-31"
    assert "Dar es Salaam" in bind_vars["userLocations0"]


async def test_fetch_returns_the_expected_point_shape():
    point = {"va_id": "va-1", "lat": -6.3, "lng": 34.8, "rrs": 80.0, "ics": 95.0, "ici": 100.0, "aid": 45.0}
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([point]))

    result = await fetch_dqa_map_points(current_user={}, db=fake_db)

    assert result.error is None or result.error is False
    assert result.data == [point]
