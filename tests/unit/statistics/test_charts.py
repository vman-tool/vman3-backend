from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.statistics.services.charts import _resolve_location_nodes, fetch_charts_statistics
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


def _combined_query_result():
    return {
        "monthly_submissions": [{"month": 1, "year": 2024, "count": 5}],
        "distribution_by_age": [{"adult": 3, "child": 1, "neonatal": 1}],
        "gender_distribution": [{"male": 2, "female": 3, "other": 0}],
        "data_overview": {
            "total": 5,
            "first_submission": "2024-01-01",
            "last_submission": "2024-01-31",
            "distinct_regions": 1,
            "distinct_districts": 1,
        },
    }


class TestMonthlyTarget:
    # The response's monthly_target: annual expected deaths for the most
    # recent year, /12 - a flat reference line for the Monthly Submissions
    # chart. Scoped to whatever the dashboard's location filter currently
    # selects (via get_expected_deaths_total_for_nodes), falling back to the
    # country-wide total (every level-1 admin unit) when no filter is set.

    async def test_uses_the_most_recent_year_with_data_divided_by_twelve(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([_combined_query_result()]))

        with _patch_config(district_field="district"), \
             patch("app.statistics.services.charts.get_expected_deaths_total_for_nodes",
                   new=AsyncMock(return_value={"2023": 1200, "2024": 2400})):
            result = await fetch_charts_statistics(current_user={}, db=fake_db)

        assert result.data["monthly_target"] == 200  # 2024 (latest): 2400 / 12

    async def test_falls_back_to_a_single_bare_total_period(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([_combined_query_result()]))

        with _patch_config(district_field="district"), \
             patch("app.statistics.services.charts.get_expected_deaths_total_for_nodes",
                   new=AsyncMock(return_value={"total": 1200})):
            result = await fetch_charts_statistics(current_user={}, db=fake_db)

        assert result.data["monthly_target"] == 100

    async def test_is_none_when_no_expected_deaths_data_exists(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([_combined_query_result()]))

        with _patch_config(district_field="district"), \
             patch("app.statistics.services.charts.get_expected_deaths_total_for_nodes",
                   new=AsyncMock(return_value={})):
            result = await fetch_charts_statistics(current_user={}, db=fake_db)

        assert result.data["monthly_target"] is None

    async def test_a_location_filter_is_translated_to_a_level_value_node_and_passed_through(self):
        # Regression: filtering the dashboard to a specific location (e.g.
        # "Dar es Salaam") previously left the target line showing the
        # unfiltered country-wide total - it should shrink to that
        # location's own (smaller) expected deaths instead.
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([_combined_query_result()]))
        captured = {}

        async def fake_total_for_nodes(db, nodes):
            captured['nodes'] = nodes
            return {"2024": 1200}

        with _patch_config(district_field="district"), \
             patch("app.statistics.services.charts.get_expected_deaths_total_for_nodes", new=fake_total_for_nodes):
            result = await fetch_charts_statistics(
                current_user={}, db=fake_db,
                locations='[{"field": "region", "value": "Dar_es_Salaam"}]',
            )

        assert captured['nodes'] == [(1, "Dar_es_Salaam")]
        assert result.data["monthly_target"] == 100

    async def test_no_location_filter_resolves_to_an_empty_node_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([_combined_query_result()]))
        captured = {}

        async def fake_total_for_nodes(db, nodes):
            captured['nodes'] = nodes
            return {}

        with _patch_config(district_field="district"), \
             patch("app.statistics.services.charts.get_expected_deaths_total_for_nodes", new=fake_total_for_nodes):
            await fetch_charts_statistics(current_user={}, db=fake_db, locations=None)

        assert captured['nodes'] == []


class TestResolveLocationNodes:
    def test_maps_a_mapped_field_to_its_configured_level(self):
        field_mapping = SimpleNamespace(location_level1="region", location_level2="district", location_level3=None)
        nodes = _resolve_location_nodes('[{"field": "district", "value": "Kongwa_DC"}]', field_mapping)
        assert nodes == [(2, "Kongwa_DC")]

    def test_drops_a_pair_for_a_field_that_is_not_a_configured_location_level(self):
        field_mapping = SimpleNamespace(location_level1="region", location_level2=None, location_level3=None)
        nodes = _resolve_location_nodes('[{"field": "ward", "value": "Some_Ward"}]', field_mapping)
        assert nodes == []

    def test_empty_or_missing_filter_gives_an_empty_list(self):
        field_mapping = SimpleNamespace(location_level1="region", location_level2=None, location_level3=None)
        assert _resolve_location_nodes(None, field_mapping) == []
        assert _resolve_location_nodes('[]', field_mapping) == []

    def test_multiple_selected_locations_across_levels_are_all_resolved(self):
        field_mapping = SimpleNamespace(location_level1="region", location_level2="district", location_level3=None)
        nodes = _resolve_location_nodes(
            '[{"field": "region", "value": "Dodoma"}, {"field": "district", "value": "Kongwa_DC"}]',
            field_mapping,
        )
        assert nodes == [(1, "Dodoma"), (2, "Kongwa_DC")]
