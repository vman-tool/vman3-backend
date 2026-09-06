from unittest.mock import AsyncMock, patch

import pytest

from app.ccva.services.ccva_data_services import (
    clear_ccva_default,
    fetch_all_processed_ccva_graphs,
    fetch_ccva_individual_results,
    get_ccva_filter_options,
    set_ccva_as_default,
)
from app.shared.middlewares.exceptions import BadRequestException
from tests.support.fakes import FakeCursor, FakeDB


@pytest.fixture(autouse=True)
def no_real_cache():
    # invalidate_cache_pattern talks to FastAPICache's configured backend,
    # which is never initialized in a unit test.
    with patch(
        "app.ccva.services.ccva_data_services.invalidate_cache_pattern",
        new=AsyncMock(),
    ):
        yield


class TestSetCcvaAsDefault:
    def _responder(self, ccva_id: str, matched: bool = True):
        def responder(query: str, bind_vars):
            if bind_vars is None:
                # Step 1: unset the current default - return value unused.
                return FakeCursor([])
            assert bind_vars == {"ccva_id": ccva_id}
            if not matched:
                return FakeCursor([])
            return FakeCursor([{"_key": ccva_id, "isDefault": True}])
        return responder

    async def test_unsets_the_previous_default_before_setting_the_new_one(self):
        fake_db = FakeDB(responder=self._responder("56007"))

        result = await set_ccva_as_default("56007", fake_db)

        assert result.data == {"_key": "56007", "isDefault": True}
        assert "56007 set as default successfully" in result.message
        queries = [q for q, _ in fake_db.aql.queries]
        assert any("isDefault: false" in q and "@ccva_id" not in q for q in queries)
        assert any("isDefault: true" in q and "@ccva_id" in q for q in queries)

    async def test_raises_when_the_target_id_does_not_exist(self):
        fake_db = FakeDB(responder=self._responder("missing-id", matched=False))

        with pytest.raises(BadRequestException):
            await set_ccva_as_default("missing-id", fake_db)


async def test_clear_ccva_default_clears_and_returns_the_updated_doc():
    def responder(query: str, bind_vars):
        assert bind_vars == {"ccva_id": "56007"}
        return FakeCursor([{"_key": "56007", "isDefault": False}])

    fake_db = FakeDB(responder=responder)

    result = await clear_ccva_default("56007", fake_db)

    assert result.data == {"_key": "56007", "isDefault": False}
    assert "56007 default cleared successfully" in result.message


async def test_clear_ccva_default_raises_when_not_found_or_not_currently_default():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    with pytest.raises(BadRequestException):
        await clear_ccva_default("56007", fake_db)


class TestFetchAllProcessedCcvaGraphs:
    async def test_includes_malaria_and_hiv_status_in_the_returned_rows(self):
        row = {
            "id": "465580", "task_id": "t1", "created_at": "2026-01-01",
            "total_records": 10, "elapsed_time": "0:1:0", "start": None, "end": None,
            "isDefault": False, "algorithm": "InterVA5",
            "malaria_status": "h", "hiv_status": "l", "run_by_name": "admin",
        }
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([row]))

        result = await fetch_all_processed_ccva_graphs(db=fake_db)

        assert result.data[0]["malaria_status"] == "h"
        assert result.data[0]["hiv_status"] == "l"
        query = fake_db.aql.queries[0][0]
        assert "doc.malaria_status" in query
        assert "doc.hiv_status" in query


class TestFetchCcvaIndividualResults:
    @pytest.fixture(autouse=True)
    def cause_category_lookup(self):
        # A real (not mocked) classify_cause() runs against this small
        # lookup, so tests exercise the actual matching logic rather than a
        # stubbed-out cause1_major/cause1_broad.
        lookup = (
            {"04": ("Diseases of the circulatory system", "Group II: Non-Communicable")},
            [("Diseases of the circulatory system", "Group II: Non-Communicable")],
        )
        with patch(
            "app.ccva.services.ccva_data_services.get_cause_category_lookup",
            new=AsyncMock(return_value=lookup),
        ):
            yield

    def _row(self, **overrides):
        row = {
            "va_id": "uuid-1",
            "locationLevel1": "dodoma",
            "locationLevel2": "kongwa_dc",
            "locationLevel3": "",
            "gender": "male",
            "age_group": "adult",
            "cause1": "Stroke",
            "cause1_probability": "83",
            "cause2": " ",
            "cause2_probability": " ",
        }
        row.update(overrides)
        return row

    def _responder(self, rows, total):
        def responder(query, bind_vars):
            if "COLLECT WITH COUNT" in query:
                return FakeCursor([total])
            return FakeCursor(list(rows))
        return responder

    def _responder_with_distinct_causes(self, rows, total, distinct_causes):
        def responder(query, bind_vars):
            if "COLLECT WITH COUNT" in query:
                return FakeCursor([total])
            if "COLLECT cause = doc.CAUSE1" in query:
                return FakeCursor(list(distinct_causes))
            return FakeCursor(list(rows))
        return responder

    async def test_filters_by_task_id_and_returns_total_count(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        assert result.total == 1
        assert result.data[0]["va_id"] == "uuid-1"
        queries = [q for q, _ in fake_db.aql.queries]
        assert any("doc.task_id == @task_id" in q for q in queries)

    async def test_attaches_the_broad_and_major_cause_category_for_cause1(self):
        fake_db = FakeDB(responder=self._responder([self._row(cause1="Stroke")], 1))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        row = result.data[0]
        assert row["cause1_major"] == "Diseases of the circulatory system"
        assert row["cause1_broad"] == "Group II: Non-Communicable"

    async def test_leaves_the_category_columns_none_when_cause1_does_not_match(self):
        fake_db = FakeDB(responder=self._responder([self._row(cause1="Some Unmapped Cause")], 1))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        row = result.data[0]
        assert row["cause1_major"] is None
        assert row["cause1_broad"] is None

    async def test_normalizes_blank_cause2_and_probability_to_none(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        row = result.data[0]
        assert row["cause2"] is None
        assert row["cause2_probability"] is None

    async def test_normalizes_a_numeric_string_probability_to_a_float(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        assert result.data[0]["cause1_probability"] == 83.0

    async def test_normalizes_a_real_float_probability_unchanged(self):
        fake_db = FakeDB(responder=self._responder(
            [self._row(cause1_probability=91.5)], 1
        ))

        result = await fetch_ccva_individual_results(task_id="t1", db=fake_db)

        assert result.data[0]["cause1_probability"] == 91.5

    async def test_adds_the_va_id_search_filter_when_provided(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        await fetch_ccva_individual_results(
            task_id="t1", search_va_id="uuid-1", db=fake_db
        )

        queries = [q for q, _ in fake_db.aql.queries]
        assert any("CONTAINS(LOWER(TO_STRING(doc.ID))" in q for q in queries)

    async def test_filters_by_a_plain_stored_field_gender(self):
        fake_db = FakeDB(responder=self._responder([self._row(gender="male")], 1))

        await fetch_ccva_individual_results(
            task_id="t1", filter_by="gender", filter_value="male", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries if "COLLECT WITH COUNT" not in q
        ][0]
        assert "doc.gender == @filter_value" in data_query
        assert data_bind_vars["filter_value"] == "male"

    async def test_filters_by_a_plain_stored_field_age_group(self):
        fake_db = FakeDB(responder=self._responder([self._row(age_group="child")], 1))

        await fetch_ccva_individual_results(
            task_id="t1", filter_by="age_group", filter_value="child", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries if "COLLECT WITH COUNT" not in q
        ][0]
        assert "doc.age_group == @filter_value" in data_query
        assert data_bind_vars["filter_value"] == "child"

    async def test_filters_by_broad_category_via_matching_cause1_values(self):
        # Stroke ("04") is the only distinct cause1 in this run and matches
        # the "Group II: Non-Communicable" broad group per the fixture's
        # cause_category_lookup - so it should end up in the IN filter.
        fake_db = FakeDB(responder=self._responder_with_distinct_causes(
            [self._row()], 1, distinct_causes=["Stroke"]
        ))

        await fetch_ccva_individual_results(
            task_id="t1", filter_by="broad", filter_value="Group II: Non-Communicable", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries
            if "COLLECT WITH COUNT" not in q and "COLLECT cause = doc.CAUSE1" not in q
        ][0]
        assert "doc.CAUSE1 IN @matching_causes" in data_query
        assert data_bind_vars["matching_causes"] == ["Stroke"]

    async def test_broad_category_filter_excludes_non_matching_causes(self):
        fake_db = FakeDB(responder=self._responder_with_distinct_causes(
            [self._row()], 1, distinct_causes=["Stroke", "Some Unmapped Cause"]
        ))

        await fetch_ccva_individual_results(
            task_id="t1", filter_by="broad", filter_value="Group II: Non-Communicable", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries
            if "COLLECT WITH COUNT" not in q and "COLLECT cause = doc.CAUSE1" not in q
        ][0]
        assert data_bind_vars["matching_causes"] == ["Stroke"]

    async def test_ignores_an_unrecognized_filter_by_value(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        await fetch_ccva_individual_results(
            task_id="t1", filter_by="not-a-real-field", filter_value="x", db=fake_db
        )

        data_query = [q for q, _ in fake_db.aql.queries if "COLLECT WITH COUNT" not in q][0]
        assert "filter_value" not in data_query

    async def test_sorts_by_a_whitelisted_field_and_direction(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        await fetch_ccva_individual_results(
            task_id="t1", sort_by="cause1", sort_dir="desc", db=fake_db
        )

        data_query = [q for q, _ in fake_db.aql.queries if "COLLECT WITH COUNT" not in q][0]
        assert "SORT doc.CAUSE1 DESC" in data_query

    async def test_unknown_sort_field_falls_back_to_va_id_ascending(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        await fetch_ccva_individual_results(
            task_id="t1", sort_by="not-a-real-field", db=fake_db
        )

        data_query = [q for q, _ in fake_db.aql.queries if "COLLECT WITH COUNT" not in q][0]
        assert "SORT doc.ID ASC" in data_query

    async def test_sorts_by_broad_category_via_a_cause1_translate_map(self):
        # Stroke ("04") is the only distinct cause1 in this run, per the
        # fixture's cause_category_lookup it maps to "Group II:
        # Non-Communicable" - the sort should translate doc.CAUSE1 through a
        # map built from exactly that, since Broad Category isn't a stored
        # field the AQL SORT clause could reference directly.
        fake_db = FakeDB(responder=self._responder_with_distinct_causes(
            [self._row()], 1, distinct_causes=["Stroke", "Some Unmapped Cause"]
        ))

        await fetch_ccva_individual_results(
            task_id="t1", sort_by="broad", sort_dir="asc", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries
            if "COLLECT WITH COUNT" not in q and "COLLECT cause = doc.CAUSE1" not in q
        ][0]
        assert 'SORT TRANSLATE(doc.CAUSE1, @cause1_category_map, "") ASC' in data_query
        assert data_bind_vars["cause1_category_map"] == {"Stroke": "Group II: Non-Communicable"}

    async def test_sorts_by_major_category_via_a_cause1_translate_map(self):
        fake_db = FakeDB(responder=self._responder_with_distinct_causes(
            [self._row()], 1, distinct_causes=["Stroke"]
        ))

        await fetch_ccva_individual_results(
            task_id="t1", sort_by="major", sort_dir="desc", db=fake_db
        )

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries
            if "COLLECT WITH COUNT" not in q and "COLLECT cause = doc.CAUSE1" not in q
        ][0]
        assert 'SORT TRANSLATE(doc.CAUSE1, @cause1_category_map, "") DESC' in data_query
        assert data_bind_vars["cause1_category_map"] == {"Stroke": "Diseases of the circulatory system"}

    async def test_paginates_with_the_expected_offset(self):
        fake_db = FakeDB(responder=self._responder([self._row()], 1))

        await fetch_ccva_individual_results(task_id="t1", page_number=3, limit=10, db=fake_db)

        data_query, data_bind_vars = [
            (q, b) for q, b in fake_db.aql.queries if "COLLECT WITH COUNT" not in q
        ][0]
        assert "LIMIT @offset, @size" in data_query
        assert data_bind_vars["offset"] == 20
        assert data_bind_vars["size"] == 10


class TestGetCcvaFilterOptions:
    @pytest.fixture(autouse=True)
    def cause_category_lookup(self):
        lookup = (
            {"04": ("Diseases of the circulatory system", "Group II: Non-Communicable")},
            [("Diseases of the circulatory system", "Group II: Non-Communicable")],
        )
        with patch(
            "app.ccva.services.ccva_data_services.get_cause_category_lookup",
            new=AsyncMock(return_value=lookup),
        ):
            yield

    def _responder(self, genders, age_groups, causes):
        def responder(query, bind_vars):
            if "doc.gender" in query:
                return FakeCursor(list(genders))
            if "doc.age_group" in query:
                return FakeCursor(list(age_groups))
            if "COLLECT cause = doc.CAUSE1" in query:
                return FakeCursor(list(causes))
            return FakeCursor([])
        return responder

    async def test_returns_distinct_gender_and_age_group_values(self):
        fake_db = FakeDB(responder=self._responder(
            genders=["male", "female"], age_groups=["adult", "child"], causes=[]
        ))

        result = await get_ccva_filter_options("t1", fake_db)

        assert result.data["gender"] == ["female", "male"]
        assert result.data["age_group"] == ["adult", "child"]

    async def test_derives_broad_and_major_options_from_the_distinct_cause1_values(self):
        fake_db = FakeDB(responder=self._responder(
            genders=[], age_groups=[], causes=["Stroke", "Some Unmapped Cause"]
        ))

        result = await get_ccva_filter_options("t1", fake_db)

        assert result.data["broad"] == ["Group II: Non-Communicable"]
        assert result.data["major"] == ["Diseases of the circulatory system"]

    async def test_empty_run_gives_empty_option_lists(self):
        fake_db = FakeDB(responder=self._responder(genders=[], age_groups=[], causes=[]))

        result = await get_ccva_filter_options("t1", fake_db)

        assert result.data == {"gender": [], "age_group": [], "broad": [], "major": []}
