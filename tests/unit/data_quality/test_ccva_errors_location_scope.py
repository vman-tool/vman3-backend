import pytest

from app.data_quality.services.data_quality import (
    _is_within_location_scope,
    fetch_error_details,
    fetch_error_list,
    save_corrections_data,
)
from app.shared.configs.constants import db_collections
from app.shared.middlewares.exceptions import BadRequestException, ForbiddenException
from tests.support.fakes import FakeCursor, FakeDB

REGION_FIELD = "id10005r"

UNRESTRICTED_USER = {}
DODOMA_USER = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}


class TestForbiddenException:
    def test_constructs_without_error_and_has_403_status(self):
        exc = ForbiddenException()
        assert exc.status_code == 403
        assert exc.to_dict()["status_code"] == 403

    def test_accepts_a_custom_detail_message(self):
        exc = ForbiddenException("custom message")
        assert exc.detail == "custom message"


class TestIsWithinLocationScope:
    def test_unrestricted_user_is_always_in_scope(self):
        assert _is_within_location_scope(UNRESTRICTED_USER, None) is True
        assert _is_within_location_scope(UNRESTRICTED_USER, {REGION_FIELD: "Dodoma"}) is True

    def test_restricted_user_with_missing_record_is_out_of_scope(self):
        assert _is_within_location_scope(DODOMA_USER, None) is False

    def test_restricted_user_matching_value_is_in_scope(self):
        assert _is_within_location_scope(DODOMA_USER, {REGION_FIELD: "Dodoma"}) is True

    def test_restricted_user_with_different_value_is_out_of_scope(self):
        assert _is_within_location_scope(DODOMA_USER, {REGION_FIELD: "Dar es Salaam"}) is False


class TestFetchErrorList:
    async def test_unrestricted_user_query_has_no_scope_join(self):
        responder = lambda query, bind_vars: FakeCursor([
            {"groupedErrorsCounts": [], "individualErrorsResults": [], "totalMatching": 0}
        ])
        fake_db = FakeDB(responder=responder)

        await fetch_error_list(fake_db, UNRESTRICTED_USER, page_number=1, limit=10)

        query, bind_vars = fake_db.aql.queries[0]
        assert db_collections.VA_TABLE not in query
        assert "userLocations0" not in bind_vars and "locationValues0" not in bind_vars

    async def test_restricted_user_query_joins_va_table_and_filters(self):
        responder = lambda query, bind_vars: FakeCursor([
            {"groupedErrorsCounts": [], "individualErrorsResults": [], "totalMatching": 0}
        ])
        fake_db = FakeDB(responder=responder)

        await fetch_error_list(fake_db, DODOMA_USER, page_number=1, limit=10)

        query, bind_vars = fake_db.aql.queries[0]
        assert db_collections.VA_TABLE in query
        assert f"fs.{REGION_FIELD}" in query
        assert "Dodoma" in bind_vars["locationValues0"]

    async def test_total_reflects_filtered_set_not_raw_collection_length(self):
        responder = lambda query, bind_vars: FakeCursor([
            {"groupedErrorsCounts": [], "individualErrorsResults": [{"uuid": "a"}], "totalMatching": 1}
        ])
        fake_db = FakeDB(responder=responder)

        result = await fetch_error_list(fake_db, UNRESTRICTED_USER, page_number=1, limit=10)

        assert result.total == 1
        assert result.data["individualErrorsResults"] == [{"uuid": "a"}]

    async def test_pagination_offset_and_size_are_wired_through(self):
        captured = {}

        def responder(query, bind_vars):
            captured.update(bind_vars)
            return FakeCursor([{"groupedErrorsCounts": [], "individualErrorsResults": [], "totalMatching": 0}])

        fake_db = FakeDB(responder=responder)

        await fetch_error_list(fake_db, UNRESTRICTED_USER, page_number=3, limit=10)

        assert captured["offset"] == 20
        assert captured["size"] == 10


class TestFetchErrorDetails:
    def _responder_for(self, form_data):
        def responder(query, bind_vars):
            if db_collections.CCVA_ERRORS_CORRECTIONS in query:
                return FakeCursor([])
            return FakeCursor([{
                "error": {"uuid": "va-1", "error_types": ["x"], "group": "g", "error_messages": ["m"]},
                "form_data": form_data,
            }])
        return responder

    async def test_in_scope_record_returns_normally(self):
        fake_db = FakeDB(responder=self._responder_for({REGION_FIELD: "Dodoma"}))

        result = await fetch_error_details(fake_db, "va-1", DODOMA_USER)

        assert result.data["error"]["uuid"] == "va-1"

    async def test_out_of_scope_record_raises_forbidden(self):
        fake_db = FakeDB(responder=self._responder_for({REGION_FIELD: "Dar es Salaam"}))

        with pytest.raises(ForbiddenException):
            await fetch_error_details(fake_db, "va-1", DODOMA_USER)

    async def test_missing_va_record_raises_bad_request_not_forbidden(self):
        fake_db = FakeDB(responder=self._responder_for(None))

        with pytest.raises(BadRequestException):
            await fetch_error_details(fake_db, "va-1", DODOMA_USER)

    async def test_unrestricted_user_sees_any_record(self):
        fake_db = FakeDB(responder=self._responder_for({REGION_FIELD: "Dar es Salaam"}))

        result = await fetch_error_details(fake_db, "va-1", UNRESTRICTED_USER)

        assert result.data["error"]["uuid"] == "va-1"


class TestSaveCorrectionsData:
    def _responder_for(self, current_record):
        def responder(query, bind_vars):
            if "UPDATE fs" in query:
                return FakeCursor([{**current_record, **bind_vars.get("updated_data", {})}])
            if db_collections.CCVA_ERRORS_CORRECTIONS in query:
                return FakeCursor([])
            return FakeCursor([current_record])
        return responder

    async def test_out_of_scope_edit_raises_forbidden_and_never_writes(self):
        fake_db = FakeDB(responder=self._responder_for({REGION_FIELD: "Dar es Salaam"}))

        with pytest.raises(ForbiddenException):
            await save_corrections_data(fake_db, "va-1", {"id10023": "new"}, {"name": "x"}, DODOMA_USER)

        issued = [q for q, _ in fake_db.aql.queries]
        assert not any("UPDATE fs" in q for q in issued)
        assert not any(f"INSERT @entry INTO {db_collections.CCVA_ERRORS_CORRECTIONS}" in q for q in issued)

    async def test_in_scope_edit_succeeds(self):
        fake_db = FakeDB(responder=self._responder_for({REGION_FIELD: "Dodoma"}))

        result = await save_corrections_data(
            fake_db, "va-1", {"id10023": "new"}, {"name": "x"}, DODOMA_USER
        )

        assert result.data["id10023"] == "new"

    async def test_missing_record_raises_bad_request(self):
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

        with pytest.raises(BadRequestException):
            await save_corrections_data(fake_db, "va-1", {"id10023": "new"}, {"name": "x"}, DODOMA_USER)
