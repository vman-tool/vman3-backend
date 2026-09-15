from types import SimpleNamespace
from unittest.mock import patch

from app.users.services.user import (
    _location_pairs,
    is_access_limit_within_scope,
)
from tests.support.fakes import FakeCursor, FakeDB

# Deliberately the real deployment's raw field names (not the friendly words
# "region"/"district") - this app's location fields are dynamic per
# deployment, and using realistic names here avoids masking a field-name
# mismatch the way an earlier, unrelated bug slipped through a test fixture
# that happened to use the same word as a hardcoded key.
REGION_FIELD = "id10005r"
DISTRICT_FIELD = "id10005d"


def _fake_field_mapping():
    return SimpleNamespace(
        location_level1=REGION_FIELD, location_level2=DISTRICT_FIELD,
        location_level3="id10005w", location_level4=None,
    )


def _patched_odk_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=_fake_field_mapping())
    return patch("app.users.services.user.fetch_odk_config", new=fake_fetch_odk_config)


class TestLocationPairs:
    def test_flattens_limit_by_items_carrying_their_own_field(self):
        access_limit = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        assert _location_pairs(access_limit) == [(REGION_FIELD, "Dodoma")]

    def test_falls_back_to_the_legacy_top_level_field(self):
        access_limit = {"field": REGION_FIELD, "limit_by": [{"value": "Dodoma"}]}
        assert _location_pairs(access_limit) == [(REGION_FIELD, "Dodoma")]

    def test_empty_or_missing_access_limit_has_no_pairs(self):
        assert _location_pairs(None) == []
        assert _location_pairs({}) == []
        assert _location_pairs({"limit_by": []}) == []


class TestIsAccessLimitWithinScope:
    async def test_an_unrestricted_scope_admits_everything(self):
        fake_db = FakeDB()
        assert await is_access_limit_within_scope(
            {"limit_by": [{"field": REGION_FIELD, "value": "Anywhere"}]}, {}, fake_db
        ) is True
        assert await is_access_limit_within_scope(None, None, fake_db) is True

    async def test_an_unrestricted_candidate_is_denied_by_a_restricted_scope(self):
        # A regional admin must never see/edit an account with no location
        # restriction of its own (e.g. a national admin).
        fake_db = FakeDB()
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(None, scope, fake_db) is False
            assert await is_access_limit_within_scope({"limit_by": []}, scope, fake_db) is False

    async def test_same_level_same_value_is_within_scope_with_no_db_lookup(self):
        fake_db = FakeDB(responder=lambda q, b: (_ for _ in ()).throw(AssertionError("should not query VA_TABLE for an exact match")))
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is True

    async def test_same_level_different_value_is_out_of_scope(self):
        fake_db = FakeDB()
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is False

    async def test_deeper_level_confirmed_as_a_descendant_via_va_table_is_within_scope(self):
        fake_db = FakeDB(responder=lambda q, b: FakeCursor([1]))
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [{"field": DISTRICT_FIELD, "value": "Kongwa"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is True
        query, bind_vars = fake_db.aql.queries[0]
        assert REGION_FIELD in query and DISTRICT_FIELD in query
        assert bind_vars == {"sv": "Dodoma", "cv": "Kongwa"}

    async def test_deeper_level_with_no_matching_va_record_is_out_of_scope(self):
        fake_db = FakeDB(responder=lambda q, b: FakeCursor([]))
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [{"field": DISTRICT_FIELD, "value": "Ilala"}]}  # actually in Dar es Salaam
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is False

    async def test_shallower_level_than_scope_is_out_of_scope(self):
        # A district-restricted scope cannot be satisfied by a broader,
        # region-level candidate.
        fake_db = FakeDB(responder=lambda q, b: FakeCursor([]))
        scope = {"limit_by": [{"field": DISTRICT_FIELD, "value": "Kongwa"}]}
        candidate = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is False

    async def test_multiple_scope_pairs_are_ored(self):
        fake_db = FakeDB()
        scope = {"limit_by": [
            {"field": REGION_FIELD, "value": "Dodoma"},
            {"field": REGION_FIELD, "value": "Mtwara"},
        ]}
        candidate = {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is True

    async def test_every_candidate_pair_must_pass(self):
        fake_db = FakeDB(responder=lambda q, b: FakeCursor([]))
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [
            {"field": REGION_FIELD, "value": "Dodoma"},
            {"field": REGION_FIELD, "value": "Mtwara"},  # not within scope
        ]}
        with _patched_odk_config():
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is False

    async def test_fails_closed_when_the_field_mapping_cannot_be_resolved(self):
        fake_db = FakeDB()
        scope = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}
        candidate = {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}

        async def broken_fetch_odk_config(*a, **k):
            raise RuntimeError("config unavailable")

        with patch("app.users.services.user.fetch_odk_config", new=broken_fetch_odk_config):
            assert await is_access_limit_within_scope(candidate, scope, fake_db) is False
