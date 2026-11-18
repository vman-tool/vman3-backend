from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.records.services.list_data import (
    fetch_va_records,
    fetch_va_records_grouped,
    fetch_va_records_map_points,
    get_va_records_ccva_filter_options,
)
from app.settings.models.settings import AidThresholds, DqaThresholds, IndicatorThresholds, TierConfig
from app.shared.middlewares.exceptions import BadRequestException
from tests.support.fakes import FakeCursor, FakeDB

import pytest


def _fake_config(dqa_thresholds=None):
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            location_level1="id10005r", location_level2="id10005d",
            instance_id="instanceid", va_id="vaid",
            interviewer_name="id10010",
            death_date="id10023", submitted_date="today", interview_date="id10012",
            is_neonate="isneonatal", is_child="ischild", is_adult="isadult",
            deceased_gender="id10019",
        ),
        # None exercises the "admin hasn't configured thresholds yet" ->
        # shipped-defaults fallback in _group_expression()/
        # get_va_records_ccva_filter_options.
        dqa_thresholds=dqa_thresholds,
    )


def _patch_config(dqa_thresholds=None):
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config(dqa_thresholds)
    return patch("app.records.services.list_data.fetch_odk_config", new=fake_fetch_odk_config)


def _responder(query: str, bind_vars):
    if query.strip().startswith("RETURN LENGTH("):
        return FakeCursor([0])
    return FakeCursor([])


async def test_searching_by_va_id_filters_on_the_configured_va_id_field():
    # va_id and instance_id are two independently-configurable fields
    # (Settings > Configuration > Field Mapping) - "VA ID" must search
    # whatever is mapped as va_id, not silently stand in for instance_id.
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(
            current_user={}, search_by="vaId", search_value="2026-09-07-E2KGJ", db=fake_db,
        )

    query, bind_vars = fake_db.aql.queries[0]
    assert "doc.vaid" in query
    assert "doc.instanceid" not in query
    assert bind_vars["search_value_pattern"] == "%2026-09-07-e2kgj%"


async def test_searching_by_instance_id_filters_on_the_configured_instance_id_field():
    # Regression coverage for the actual confusion reported live: the VA ID
    # shown in the table (instanceid, e.g. "uuid:...") and the field mapped
    # as "VA ID" (va_id) can be two different fields - Instance ID is its
    # own distinct search-by option so a value copied from the table always
    # has a matching option, whichever field it actually came from.
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(
            current_user={}, search_by="instanceId", search_value="uuid:f92c094d-a4b6", db=fake_db,
        )

    query, bind_vars = fake_db.aql.queries[0]
    assert "doc.instanceid" in query
    assert "doc.vaid" not in query
    assert bind_vars["search_value_pattern"] == "%uuid:f92c094d-a4b6%"


async def test_searching_by_region_still_filters_on_location_level1():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(
            current_user={}, search_by="region", search_value="Dodoma", db=fake_db,
        )

    query = fake_db.aql.queries[0][0]
    assert "doc.id10005r" in query


async def test_searching_by_interviewer_name_filters_on_the_interviewer_field():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(
            current_user={}, search_by="interviewer_name", search_value="John", db=fake_db,
        )

    query = fake_db.aql.queries[0][0]
    assert "doc.id10010" in query


async def test_no_search_filter_applied_without_both_search_by_and_search_value():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, search_by="vaId", search_value=None, db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "LIKE" not in query


# The 4 CCVA columns denormalized onto form_submissions by
# _sync_ccva_default_to_va_records (ccva_data_services.py) - sortable/
# filterable here like any other stored column.
async def test_sorting_by_a_ccva_column_adds_a_sort_clause_on_the_denormalized_field():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, sort_by="ccvaCauseBroad", sort_dir="desc", db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "SORT doc.ccva_cause1_broad DESC" in query


async def test_sorting_by_an_unrecognized_field_adds_no_sort_clause():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, sort_by="notARealField", db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "SORT" not in query


# Region/District/Gender/Instrument Type - unlike the CCVA/DQA columns
# above, these aren't a bare doc.<field> (region/district read the
# deployment's configured field_mapping, Instrument Type is derived), so
# sort_by resolves them through _group_expression() instead of
# _VA_RECORDS_CCVA_SORT_FIELDS's flat name lookup.
class TestSortingByRegionDistrictGenderAndInstrumentType:
    async def test_sorting_by_region_sorts_on_the_configured_location_level1_field(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, sort_by="region", sort_dir="asc", db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "SORT (doc.id10005r) ASC" in query

    async def test_sorting_by_district_sorts_on_the_configured_location_level2_field(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, sort_by="district", sort_dir="desc", db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "SORT (doc.id10005d) DESC" in query

    async def test_sorting_by_gender_sorts_on_the_configured_gender_field(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, sort_by="gender", sort_dir="asc", db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "SORT (doc.id10019) ASC" in query

    async def test_sorting_by_questionnaire_type_sorts_on_the_same_instrument_type_classification(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, sort_by="questionnaireType", sort_dir="asc", db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert 'SORT (TO_STRING(doc.isneonatal) == "1" ? "Neonate"' in query
        assert 'TO_STRING(doc.isadult) == "1" ? "Adult" : "Unclassified") ASC' in query


async def test_filtering_by_a_ccva_column_adds_a_filter_clause_on_the_denormalized_field():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, filter_by="ccvaCauseSpecific", filter_value="Stroke", db=fake_db)

    query, bind_vars = fake_db.aql.queries[0]
    assert "(doc.ccva_cause1) == @ccva_filter_value" in query
    assert bind_vars["ccva_filter_value"] == "Stroke"


async def test_algorithm_is_not_a_filterable_field():
    # Only ever 1-2 distinct values in play at a time (whichever engine(s)
    # produced the currently default run) - a filter adds little over just
    # looking at the column.
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, filter_by="ccvaAlgorithm", filter_value="InterVA5", db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "ccva_algorithm ==" not in query


async def test_get_va_records_ccva_filter_options_returns_the_configured_categories_and_distinct_causes():
    lookup = (
        {},
        [
            ("Diseases of the circulatory system", "Group II: Non-Communicable"),
            ("Road traffic accidents", "Group III: Injuries"),
        ],
    )
    # Both distinct-value queries (ccva_cause1, then the configured gender
    # field) share the same FakeCursor responder here - identical shape is
    # fine since the test only cares that both end up in the response.
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(["Stroke", "Malaria"]))
    with _patch_config(), patch(
        "app.records.services.list_data.get_cause_category_lookup",
        new=AsyncMock(return_value=lookup),
    ):
        result = await get_va_records_ccva_filter_options(db=fake_db)

    assert result.data == {
        "broad": ["Group II: Non-Communicable", "Group III: Injuries"],
        "major": ["Diseases of the circulatory system", "Road traffic accidents"],
        "specific": ["Malaria", "Stroke"],
        "gender": ["Malaria", "Stroke"],
        "age_group": ["Neonate", "Child", "Adult", "Unclassified"],
        "rrs": ["Excellent", "Good", "Critical"],
        "ics": ["Excellent", "Good", "Critical"],
        "ici": ["Excellent", "Good", "Critical"],
        "aid": ["Too Short", "Normal", "Too Long"],
    }


# Group By - same 3 fields Filter By offers (Broad/Major/Specific Cause),
# backing the summary table that replaces the individual-record rows when
# a Group By is active.
class TestFetchVaRecordsGrouped:
    async def test_rejects_an_unsupported_group_by_value(self):
        fake_db = FakeDB()

        with pytest.raises(BadRequestException):
            await fetch_va_records_grouped(current_user={}, group_by="not-a-real-field", db=fake_db)

    async def test_groups_by_the_denormalized_field_with_an_unclassified_bucket(self):
        def responder(query, bind_vars):
            return FakeCursor([
                {"group": "Group I: Communicable", "count": 7},
                {"group": "Unclassified", "count": 3},
            ])
        fake_db = FakeDB(responder=responder)

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="ccvaCauseBroad", db=fake_db)

        assert result.data == [
            {"group": "Group I: Communicable", "count": 7},
            {"group": "Unclassified", "count": 3},
        ]
        assert result.total == 10
        query = fake_db.aql.queries[0][0]
        assert 'COLLECT grouped_value = (doc.ccva_cause1_broad) != null ? (doc.ccva_cause1_broad) : "Unclassified" WITH COUNT INTO count' in query

    async def test_applies_the_same_shared_filters_as_the_individual_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_grouped(
                current_user={}, group_by="ccvaCauseMajor",
                filter_by="ccvaCauseBroad", filter_value="Group I: Communicable", db=fake_db,
            )

        query, bind_vars = fake_db.aql.queries[0]
        assert "(doc.ccva_cause1_broad) == @ccva_filter_value" in query
        assert bind_vars["ccva_filter_value"] == "Group I: Communicable"

    async def test_empty_result_returns_an_empty_group_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="ccvaCauseSpecific", db=fake_db)

        assert result.data == []
        assert result.total == 0


# The VA Records table's own Map view - a dedicated endpoint (not the
# general-purpose /records/maps behind the standalone Data Map page) so it
# can share every filter with the individual list/Group By summary.
class TestFetchVaRecordsMapPoints:
    async def test_filters_to_points_with_gps_coordinates_and_applies_the_shared_filters(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_map_points(
                current_user={}, search_by="vaId", search_value="2026-09", db=fake_db,
            )

        query, bind_vars = fake_db.aql.queries[0]
        assert "doc.coordinates != null" in query
        assert "LENGTH(doc.coordinates) == 3" in query
        assert "doc.vaid" in query  # va_id_field from _fake_config
        assert bind_vars["search_value_pattern"] == "%2026-09%"

    async def test_projects_vaid_coordinates_location_and_the_denormalized_ccva_fields(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_map_points(current_user={}, db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "vaId: doc.instanceid" in query
        assert "lat: doc.coordinates[1]" in query
        assert "lng: doc.coordinates[0]" in query
        assert "region: doc.id10005r" in query
        assert "district: doc.id10005d" in query
        assert "ccvaCauseBroad: doc.ccva_cause1_broad" in query
        assert "ccvaCauseMajor: doc.ccva_cause1_major" in query
        assert "ccvaCauseSpecific: doc.ccva_cause1" in query
        assert "ccvaAlgorithm: doc.ccva_algorithm" in query

    async def test_truncates_at_the_cap_without_the_extra_probe_row(self):
        rows = [{"vaId": f"va-{i}"} for i in range(5001)]
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(list(rows)))

        with _patch_config():
            result = await fetch_va_records_map_points(current_user={}, db=fake_db)

        assert len(result.data) == 5000
        assert result.total == 5000

    async def test_applies_the_ccva_filter_by_value_the_same_as_the_individual_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_map_points(
                current_user={}, filter_by="ccvaCauseBroad", filter_value="Group I: Communicable", db=fake_db,
            )

        query, bind_vars = fake_db.aql.queries[0]
        assert "(doc.ccva_cause1_broad) == @ccva_filter_value" in query
        assert bind_vars["ccva_filter_value"] == "Group I: Communicable"

    async def test_projects_gender_and_age_group(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_map_points(current_user={}, db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "gender: doc.id10019" in query
        assert 'ageGroup: (TO_STRING(doc.isneonatal) == "1" ? "Neonate"' in query


# Gender and Age Group - not CCVA-denormalized (Gender is a plain
# form_submissions field, Age Group is computed the same Neonate/Child/
# Adult way as the Instrument Type column) but still full Filter By/
# Group By dimensions via _group_expression().
class TestGenderAndAgeGroupDimensions:
    async def test_filters_by_gender_on_the_configured_field(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, filter_by="gender", filter_value="female", db=fake_db)

        query, bind_vars = fake_db.aql.queries[0]
        assert "(doc.id10019) == @ccva_filter_value" in query
        assert bind_vars["ccva_filter_value"] == "female"

    async def test_filters_by_age_group_using_the_instrument_type_classification(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, filter_by="age_group", filter_value="Neonate", db=fake_db)

        query, bind_vars = fake_db.aql.queries[0]
        assert 'TO_STRING(doc.isneonatal) == "1" ? "Neonate"' in query
        assert 'TO_STRING(doc.ischild) == "1" ? "Child"' in query
        assert 'TO_STRING(doc.isadult) == "1" ? "Adult" : "Unclassified"' in query
        assert bind_vars["ccva_filter_value"] == "Neonate"

    async def test_groups_by_gender(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([
            {"group": "female", "count": 5}, {"group": "male", "count": 3},
        ]))

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="gender", db=fake_db)

        assert result.total == 8
        query = fake_db.aql.queries[0][0]
        assert 'COLLECT grouped_value = (doc.id10019) != null ? (doc.id10019) : "Unclassified"' in query

    async def test_groups_by_age_group(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([
            {"group": "Adult", "count": 9},
        ]))

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="age_group", db=fake_db)

        assert result.total == 9
        query = fake_db.aql.queries[0][0]
        assert 'TO_STRING(doc.isadult) == "1" ? "Adult" : "Unclassified") != null' in query


# RRS/ICS/ICI/AID (Table 2's own dimensions) - continuous scores bucketed
# into the same admin-configurable tiers DqaThresholdService already uses
# on the frontend (Settings > Configuration > Data Quality Thresholds),
# rather than a fixed stored category the way Gender/Age Group are.
class TestDqaDimensions:
    async def test_filters_by_rrs_using_the_shipped_default_thresholds(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, filter_by="dqaRrs", filter_value="Excellent", db=fake_db)

        query, bind_vars = fake_db.aql.queries[0]
        assert 'doc.dqa_rrs == null ? "Unclassified"' in query
        assert 'doc.dqa_rrs >= @rrs_high ? @rrs_tier1' in query
        assert bind_vars["rrs_high"] == 80.0
        assert bind_vars["rrs_mid"] == 50.0
        assert bind_vars["rrs_tier1"] == "Excellent"
        assert bind_vars["rrs_tier3"] == "Critical"
        assert bind_vars["ccva_filter_value"] == "Excellent"

    async def test_filters_by_aid_using_the_shipped_default_thresholds(self):
        fake_db = FakeDB(responder=_responder)

        with _patch_config():
            await fetch_va_records(current_user={}, filter_by="dqaAid", filter_value="Too Long", db=fake_db)

        query, bind_vars = fake_db.aql.queries[0]
        assert 'doc.dqa_aid > @aid_max ? @aid_long' in query
        assert 'doc.dqa_aid < @aid_min ? @aid_short' in query
        assert bind_vars["aid_min"] == 30.0
        assert bind_vars["aid_max"] == 60.0
        assert bind_vars["aid_long"] == "Too Long"

    async def test_filters_using_an_admin_overridden_threshold_config(self):
        fake_db = FakeDB(responder=_responder)
        custom_thresholds = DqaThresholds(
            ics=IndicatorThresholds(
                threshold_high=95.0, threshold_mid=60.0,
                tier1=TierConfig(label="Great", color="green"),
                tier2=TierConfig(label="OK", color="amber"),
                tier3=TierConfig(label="Bad", color="red"),
            ),
        )

        with _patch_config(dqa_thresholds=custom_thresholds):
            await fetch_va_records(current_user={}, filter_by="dqaIcs", filter_value="Great", db=fake_db)

        bind_vars = fake_db.aql.queries[0][1]
        assert bind_vars["ics_high"] == 95.0
        assert bind_vars["ics_tier1"] == "Great"
        assert bind_vars["ics_tier3"] == "Bad"

    async def test_groups_by_ici(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([
            {"group": "Excellent", "count": 6}, {"group": "Critical", "count": 2},
        ]))

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="dqaIci", db=fake_db)

        assert result.total == 8
        query = fake_db.aql.queries[0][0]
        assert 'doc.dqa_ici == null ? "Unclassified"' in query

    async def test_map_points_project_raw_dqa_scores_not_tier_labels(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_map_points(current_user={}, db=fake_db)

        query = fake_db.aql.queries[0][0]
        assert "dqaRrs: doc.dqa_rrs" in query
        assert "dqaIcs: doc.dqa_ics" in query
        assert "dqaIci: doc.dqa_ici" in query
        assert "dqaAid: doc.dqa_aid" in query
