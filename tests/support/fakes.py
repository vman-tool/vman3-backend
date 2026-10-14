"""Lightweight stand-ins for python-arango's StandardDatabase, used to unit
test AQL-issuing service functions without a real ArangoDB instance.

Not a mock library wrapper - these are plain objects that implement just
enough of the real interface (db.aql.execute, db.collection(name), cursor
iteration) for the service layer's actual usage patterns, including the
`next(cursor, default)` builtin call some services use.
"""
from typing import Any, Callable, Iterable, Optional


class FakeCursor:
    def __init__(self, values: Iterable[Any]):
        self._values = list(values)

    def __iter__(self):
        return self

    def __next__(self):
        if not self._values:
            raise StopIteration
        return self._values.pop(0)

    # python-arango's real Cursor also exposes this name directly.
    def next(self):
        return self.__next__()


Responder = Callable[[str, Optional[dict]], FakeCursor]


class FakeAQL:
    def __init__(self, responder: Optional[Responder] = None):
        self.queries: list[tuple[str, Optional[dict]]] = []
        self._responder = responder or (lambda query, bind_vars: FakeCursor([]))

    def execute(self, query: str, bind_vars: Optional[dict] = None, **kwargs):
        self.queries.append((query, bind_vars))
        return self._responder(query, bind_vars)


class FakeCollection:
    def __init__(self, name: str):
        self.name = name
        self.inserted: list[Any] = []
        self.truncated = False
        # In-memory key-value backing for get/has/update, independent of
        # `inserted` (which some tests assert against as an ordered list) -
        # mirrors a real ArangoDB document collection closely enough for
        # the _upsert_doc_sync-style read/write-by-_key usage this supports.
        self._docs: dict[str, Any] = {}

    def insert(self, document, **kwargs):
        self.inserted.append(document)
        doc = {**document, "_key": document.get("_key", "fake-key")}
        self._docs[doc["_key"]] = doc
        return doc

    def insert_many(self, documents, **kwargs):
        self.inserted.extend(documents)
        result = [{**d, "_key": d.get("_key", "fake-key")} for d in documents]
        for doc in result:
            self._docs[doc["_key"]] = doc
        return result

    def truncate(self):
        self.truncated = True
        self.inserted = []
        self._docs = {}

    def has(self, key: str) -> bool:
        return key in self._docs

    def get(self, key: str):
        return self._docs.get(key)

    def get_many(self, keys):
        return [self._docs[k] for k in keys if k in self._docs]

    def update(self, document, merge: bool = True, **kwargs):
        key = document["_key"]
        if merge and key in self._docs:
            self._docs[key] = {**self._docs[key], **document}
        else:
            self._docs[key] = dict(document)
        return self._docs[key]


class FakeDB:
    def __init__(self, responder: Optional[Responder] = None):
        self.aql = FakeAQL(responder)
        self._collections: dict[str, FakeCollection] = {}

    def has_collection(self, name: str) -> bool:
        return True

    def collection(self, name: str) -> FakeCollection:
        return self._collections.setdefault(name, FakeCollection(name))
