from unittest.mock import patch

from app.shared.services.va_records import shared_fetch_va_records
from tests.support.fakes import FakeCursor, FakeDB


def _responder(query: str, bind_vars):
    if query.strip().startswith("RETURN LENGTH("):
        return FakeCursor([2])
    return FakeCursor([
        {"__id": "uuid:a", "field": "value_a"},
        {"__id": "uuid:b", "field": "value_b"},
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
