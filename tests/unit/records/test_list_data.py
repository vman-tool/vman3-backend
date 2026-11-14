from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.records.services.list_data import fetch_va_records, fetch_va_records_grouped, get_va_records_ccva_filter_options
from app.shared.middlewares.exceptions import BadRequestException
from tests.support.fakes import FakeCursor, FakeDB

import pytest


def _fake_config():
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            location_level1="id10005r", location_level2="id10005d",
            instance_id="instanceid", va_id="vaid",
            interviewer_name="id10010",
            death_date="id10023", submitted_date="today", interview_date="id10012",
            is_neonate="isneonatal", is_child="ischild", is_adult="isadult",
            deceased_gender="id10019",
        )
    )


def _patch_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config()
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


async def test_filtering_by_a_ccva_column_adds_a_filter_clause_on_the_denormalized_field():
    fake_db = FakeDB(responder=_responder)

    with _patch_config():
        await fetch_va_records(current_user={}, filter_by="ccvaCauseSpecific", filter_value="Stroke", db=fake_db)

    query, bind_vars = fake_db.aql.queries[0]
    assert "doc.ccva_cause1 == @ccva_filter_value" in query
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
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(["Stroke", "Malaria"]))
    with patch(
        "app.records.services.list_data.get_cause_category_lookup",
        new=AsyncMock(return_value=lookup),
    ):
        result = await get_va_records_ccva_filter_options(db=fake_db)

    assert result.data == {
        "broad": ["Group II: Non-Communicable", "Group III: Injuries"],
        "major": ["Diseases of the circulatory system", "Road traffic accidents"],
        "specific": ["Malaria", "Stroke"],
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
        assert 'COLLECT grouped_value = doc.ccva_cause1_broad != null ? doc.ccva_cause1_broad : "Unclassified" WITH COUNT INTO count' in query

    async def test_applies_the_same_shared_filters_as_the_individual_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            await fetch_va_records_grouped(
                current_user={}, group_by="ccvaCauseMajor",
                filter_by="ccvaCauseBroad", filter_value="Group I: Communicable", db=fake_db,
            )

        query, bind_vars = fake_db.aql.queries[0]
        assert "doc.ccva_cause1_broad == @ccva_filter_value" in query
        assert bind_vars["ccva_filter_value"] == "Group I: Communicable"

    async def test_empty_result_returns_an_empty_group_list(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with _patch_config():
            result = await fetch_va_records_grouped(current_user={}, group_by="ccvaCauseSpecific", db=fake_db)

        assert result.data == []
        assert result.total == 0
