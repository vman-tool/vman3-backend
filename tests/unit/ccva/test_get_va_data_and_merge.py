from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.ccva.services.ccva_services import getVADataAndMergeWithResults
from tests.support.fakes import FakeCursor, FakeDB


def _fake_config(location_level2=None, location_level3=None, location_level4=None):
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            is_adult="isadult",
            is_child="ischild",
            is_neonate="isneonatal",
            deceased_gender="id10019",
            location_level1="region",
            location_level2=location_level2,
            location_level3=location_level3,
            location_level4=location_level4,
            death_date=None,
            submitted_date=None,
            interview_date="id10012",
            instance_id="instanceID",
        )
    )


def _patch_config(**level_overrides):
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config(**level_overrides)
    return patch(
        "app.settings.services.odk_configs.fetch_odk_config",
        new=fake_fetch_odk_config,
    )


async def test_returns_results_unchanged_when_none_have_an_id():
    fake_db = FakeDB()

    with _patch_config():
        result = await getVADataAndMergeWithResults(fake_db, [{"CAUSE1": "Malaria"}])

    assert result == [{"CAUSE1": "Malaria"}]
    assert fake_db.aql.queries == []  # no query issued - nothing to look up


async def test_omits_unmapped_location_levels_from_the_query():
    # Regression: location_level2/3/4 aren't always mapped. Interpolating a
    # blank field name as `locationLevel2: LOWER(doc.),` is invalid AQL and
    # crashed this whole batch query.
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    with _patch_config(location_level2=None, location_level3=None, location_level4=None):
        await getVADataAndMergeWithResults(fake_db, [{"ID": "uuid-a"}])

    query = fake_db.aql.queries[0][0]
    assert "locationLevel2:" not in query
    assert "locationLevel3:" not in query
    assert "locationLevel4:" not in query
    assert "doc.)," not in query
    assert "doc.LOWER" not in query  # would indicate a mangled blank interpolation


async def test_includes_mapped_location_levels_in_the_query():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    with _patch_config(location_level2="district", location_level3="ward"):
        await getVADataAndMergeWithResults(fake_db, [{"ID": "uuid-a"}])

    query = fake_db.aql.queries[0][0]
    assert "locationLevel2: LOWER(doc.district)," in query
    assert "locationLevel3: LOWER(doc.ward)," in query
    assert "locationLevel4:" not in query


async def test_merges_matched_results_and_leaves_unmatched_ones_untouched():
    def responder(query, bind_vars):
        assert bind_vars == {"data_uids": ["uuid-a", "uuid-missing"]}
        return FakeCursor([{"uid": "uuid-a", "gender": "male", "age_group": "adult"}])

    fake_db = FakeDB(responder=responder)

    with _patch_config():
        result = await getVADataAndMergeWithResults(
            fake_db,
            [{"ID": "uuid-a", "CAUSE1": "Malaria"}, {"ID": "uuid-missing", "CAUSE1": "Unknown"}],
        )

    assert result[0] == {"ID": "uuid-a", "CAUSE1": "Malaria", "uid": "uuid-a", "gender": "male", "age_group": "adult"}
    assert result[1] == {"ID": "uuid-missing", "CAUSE1": "Unknown"}
