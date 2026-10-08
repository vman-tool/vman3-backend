from types import SimpleNamespace
from unittest.mock import patch

from app.records.services.list_data import fetch_va_records
from tests.support.fakes import FakeCursor, FakeDB


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
