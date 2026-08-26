from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.ccva.ccva_routes import ccva_router
from app.shared.configs.arangodb import get_arangodb_session
from app.users.decorators.user import get_current_user
from tests.support.fakes import FakeCursor, FakeDB


def _make_client(fake_db: FakeDB) -> TestClient:
    app = FastAPI()
    app.include_router(ccva_router)
    app.dependency_overrides[get_current_user] = lambda: {"uuid": "test-user"}

    async def fake_db_dependency():
        yield fake_db

    app.dependency_overrides[get_arangodb_session] = fake_db_dependency
    return TestClient(app)


@pytest.fixture(autouse=True)
def no_real_cache():
    with patch(
        "app.ccva.services.ccva_data_services.invalidate_cache_pattern",
        new=AsyncMock(),
    ):
        yield


def test_clear_default_endpoint_returns_200_and_the_updated_doc():
    def responder(query, bind_vars):
        assert bind_vars == {"ccva_id": "56007"}
        return FakeCursor([{"_key": "56007", "isDefault": False}])

    client = _make_client(FakeDB(responder=responder))

    response = client.post("/ccva/56007/clear-default")

    assert response.status_code == 200
    assert response.json() == {"message": "CCVA default cleared successfully"}


def test_clear_default_endpoint_errors_when_the_entry_is_not_currently_default():
    client = _make_client(FakeDB(responder=lambda query, bind_vars: FakeCursor([])))

    response = client.post("/ccva/56007/clear-default")

    assert response.status_code == 400
