from unittest.mock import patch

from app.shared.services.va_records import (
    get_distinct_va_field_names,
    shared_fetch_va_records,
)
from tests.support.fakes import FakeCursor, FakeDB


def _responder(query: str, bind_vars):
    if query.strip().startswith("RETURN LENGTH("):
        return FakeCursor([2])
    return FakeCursor([
        {"__id": "uuid:a", "field": "value_a"},
        {"__id": "uuid:b", "field": "value_b"},
    ])


def _responder_with_computed_fields(query: str, bind_vars):
    if query.strip().startswith("RETURN LENGTH("):
        return FakeCursor([1])
    return FakeCursor([
        {
            "__id": "uuid:a",
            "id10147": "yes",
            "ccva_cause1": "Malaria",
            "ccva_cause1_broad": "Infectious",
            "ccva_cause1_major": "Malaria",
            "ccva_algorithm": "InterVA5",
            "dqa_rrs": 0.8,
            "dqa_ics": 0.7,
            "dqa_ici": 0.6,
            "dqa_aid": 0.5,
        },
    ])


async def test_include_assignment_no_limit():
    fake_db = FakeDB(responder=_responder)

    async def fake_fetch_odk_config(db):
        return {}

    with patch(
        "app.shared.services.va_records.fetch_odk_config",
        new=fake_fetch_odk_config,
    ):
        response = await shared_fetch_va_records(
            paging=True,
            page_number=1,
            limit=None,
            include_assignment=True,
            filters={},
            format_records=False,
            db=fake_db,
        )

    assert response.total == 2
    assert len(response.data) == 2
    assert response.data[0]["__id"] == "uuid:a"
    assert any("LET paginatedVa" in query for query, _ in fake_db.aql.queries)
    assert any("LET vaIds" in query for query, _ in fake_db.aql.queries)
    assert any("LET assignments" in query for query, _ in fake_db.aql.queries)


async def test_unformatted_fetch_strips_ccva_and_dqa_fields():
    """PCVA Coding / View VA ask for unformatted records to show as VA
    content - CCVA's computed cause and DQA's scores, denormalized onto the
    same document purely for the VA Records table, must not leak into that
    view (coder bias risk), while the actual submission content survives.
    """
    fake_db = FakeDB(responder=_responder_with_computed_fields)

    async def fake_fetch_odk_config(db):
        return {}

    with patch(
        "app.shared.services.va_records.fetch_odk_config",
        new=fake_fetch_odk_config,
    ):
        response = await shared_fetch_va_records(
            paging=False,
            include_assignment=False,
            filters={},
            format_records=False,
            db=fake_db,
        )

    assert len(response.data) == 1
    record = response.data[0]
    assert record["__id"] == "uuid:a"
    assert record["id10147"] == "yes"
    for computed_field in (
        "ccva_cause1", "ccva_cause1_broad", "ccva_cause1_major", "ccva_algorithm",
        "dqa_rrs", "dqa_ics", "dqa_ici", "dqa_aid",
    ):
        assert computed_field not in record


async def test_distinct_field_names_excludes_internal_and_computed_fields():
    """Backs the "give me everything" Field Mapping / question-dictionary
    merge (get_form_questions_service) - CCVA/DQA computed columns and
    bookkeeping fields are not real submission fields, so neither should be
    offered as one, the same way _key/_rev already aren't.
    """
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([
        "id10147", "ccva_cause1", "dqa_rrs", "_key", "vman_data_source",
    ]))

    fields = await get_distinct_va_field_names(fake_db)

    assert fields == ["id10147"]
