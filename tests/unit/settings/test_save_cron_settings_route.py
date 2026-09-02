from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.settings.settings_routes import settings_router
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import AccessPrivileges
from app.shared.middlewares.exceptions import BadRequestException
from app.users.decorators.user import get_current_user, get_current_user_privileges
from tests.support.fakes import FakeDB


def _make_client() -> TestClient:
    app = FastAPI()
    app.include_router(settings_router)
    app.dependency_overrides[get_current_user] = lambda: {"uuid": "test-user"}
    app.dependency_overrides[get_current_user_privileges] = lambda: [AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS]

    async def fake_db_dependency():
        yield FakeDB()

    app.dependency_overrides[get_arangodb_session] = fake_db_dependency
    return TestClient(app)


def test_rejects_enabling_a_schedule_when_the_odk_api_is_not_ready():
    # Regression: this is the reported bug - a schedule saved fine even
    # though the only data source that could ever run automatically (the
    # ODK API) wasn't configured/working, so it would fail on every fire.
    client = _make_client()

    with patch(
        "app.settings.settings_routes.ensure_odk_api_ready_for_scheduling",
        new=AsyncMock(side_effect=BadRequestException("ODK API is not configured.")),
    ), patch("app.settings.settings_routes.add_configs_settings") as mock_save:
        response = client.post("/settings/cron", json={"days": ["monday"], "time": "09:00"})

    assert response.status_code == 400
    assert "not configured" in response.json()["detail"]
    mock_save.assert_not_called()  # never got to actually saving


def test_saves_when_enabling_a_schedule_with_a_working_odk_api():
    client = _make_client()

    with patch("app.settings.settings_routes.ensure_odk_api_ready_for_scheduling", new=AsyncMock(return_value=None)), \
         patch("app.settings.settings_routes.add_configs_settings", new=AsyncMock(return_value=SimpleNamespace())) as mock_save:
        response = client.post("/settings/cron", json={"days": ["monday"], "time": "09:00"})

    assert response.status_code == 200
    mock_save.assert_called_once()


def test_disabling_a_schedule_never_checks_the_odk_api():
    client = _make_client()

    with patch("app.settings.settings_routes.ensure_odk_api_ready_for_scheduling", new=AsyncMock()) as mock_check, \
         patch("app.settings.settings_routes.add_configs_settings", new=AsyncMock(return_value=SimpleNamespace())) as mock_save:
        response = client.post("/settings/cron", json={"days": [], "time": "09:00"})

    assert response.status_code == 200
    mock_check.assert_not_called()
    mock_save.assert_called_once()
