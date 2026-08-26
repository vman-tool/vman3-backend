from types import SimpleNamespace
from unittest.mock import patch

from app.statistics.services.charts import fetch_charts_statistics
from tests.support.fakes import FakeCursor, FakeDB


def _fake_config(district_field=None):
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            location_level1="region",
            location_level2=district_field,
            is_adult="isadult",
            is_child="ischild",
            is_neonate="isneonatal",
            death_date="id10023",
            submitted_date="today",
            interview_date="id10012",
            deceased_gender="id10019",
        )
    )


def _patch_config(district_field=None):
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config(district_field)
    return patch("app.statistics.services.charts.fetch_odk_config", new=fake_fetch_odk_config)


async def test_omits_district_from_the_combined_query_when_location_level2_is_unmapped():
    # Regression: with only Admin Level 1 mapped, interpolating a blank
    # district field as `doc.` (or `, district: doc.`) is invalid AQL and
    # crashed the whole combined query - taking every chart on the
    # dashboard down with it, not just the geo stats.
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([{}]))

    with _patch_config(district_field=None):
        await fetch_charts_statistics(current_user={}, db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "district: doc." not in query
    assert "distinctDistricts = 0" in query


async def test_includes_district_in_the_combined_query_when_location_level2_is_mapped():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([{}]))

    with _patch_config(district_field="district"):
        await fetch_charts_statistics(current_user={}, db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "district: doc.district" in query
    assert "distinctDistricts = LENGTH(geoData)" in query
