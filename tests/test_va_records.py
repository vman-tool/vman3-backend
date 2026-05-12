import asyncio
import unittest
from unittest.mock import patch

from app.shared.services.va_records import shared_fetch_va_records


class DummyCursor:
    def __init__(self, values):
        self._values = list(values)

    def __iter__(self):
        return iter(self._values)

    def next(self):
        if not self._values:
            raise StopIteration
        return self._values.pop(0)


class FakeAQL:
    def __init__(self):
        self.queries = []

    def execute(self, query, bind_vars=None, cache=None):
        self.queries.append((query, bind_vars))
        if query.strip().startswith("RETURN LENGTH("):
            return DummyCursor([2])
        return DummyCursor([
            {"__id": "uuid:a", "field": "value_a"},
            {"__id": "uuid:b", "field": "value_b"},
        ])


class FakeCollection:
    def __init__(self, name):
        self.name = name


class FakeDB:
    def __init__(self):
        self.aql = FakeAQL()

    def has_collection(self, collection_name):
        return True

    def collection(self, collection_name):
        return FakeCollection(collection_name)


class SharedFetchVaRecordsTests(unittest.IsolatedAsyncioTestCase):
    async def test_include_assignment_no_limit(self):
        fake_db = FakeDB()

        async def fake_fetch_odk_config(db):
            return {}

        async def fake_run_in_threadpool(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "app.shared.services.va_records.fetch_odk_config",
            new=fake_fetch_odk_config,
        ), patch(
            "app.shared.services.va_records.run_in_threadpool",
            new=fake_run_in_threadpool,
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

        self.assertEqual(response.total, 2)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["__id"], "uuid:a")
        self.assertTrue(any("LET paginatedVa" in query for query, _ in fake_db.aql.queries))
        self.assertTrue(any("LET vaIds" in query for query, _ in fake_db.aql.queries))
        self.assertTrue(any("LET assignments" in query for query, _ in fake_db.aql.queries))


if __name__ == "__main__":
    unittest.main()
