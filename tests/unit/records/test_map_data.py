from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.records.services.map_data import fetch_va_map_records
from tests.support.fakes import FakeCursor, FakeDB


def _fake_config(location_level2):
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            location_level1="region",
            location_level2=location_level2,
            interviewer_name="id10010",
            interview_date="id10012",
        )
    )


@pytest.fixture
def patched_odk_config():
    def _patch(location_level2):
        async def fake_fetch_odk_config(db):
            return _fake_config(location_level2)
        return patch(
            "app.records.services.map_data.fetch_odk_config",
            new=fake_fetch_odk_config,
        )
    return _patch


async def test_omits_district_from_the_query_when_location_level2_is_unmapped(patched_odk_config):
    # Regression: some deployments only map Admin Level 1. Interpolating a
    # blank field name as `district: doc.,` is invalid AQL and crashed this
    # whole query - not just the map, since it's the only query this
    # endpoint issues.
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    with patched_odk_config(location_level2=None):
        await fetch_va_map_records(current_user={}, db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "district:" not in query
    assert "doc.," not in query


async def test_includes_district_in_the_query_when_location_level2_is_mapped(patched_odk_config):
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    with patched_odk_config(location_level2="district"):
        await fetch_va_map_records(current_user={}, db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert "district: doc.district," in query
