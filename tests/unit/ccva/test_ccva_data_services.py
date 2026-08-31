from unittest.mock import AsyncMock, patch

import pytest

from app.ccva.services.ccva_data_services import clear_ccva_default, set_ccva_as_default
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
