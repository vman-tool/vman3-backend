from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.settings.models.settings import ImagesConfigData
from app.settings.services.odk_configs import save_system_images
from app.settings.settings_routes import settings_router
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import AccessPrivileges
from app.users.decorators.user import get_current_user, get_current_user_privileges
from tests.support.fakes import FakeCursor, FakeDB


class TestSaveSystemImagesFieldsToReset:
    # get_system_images() is itself cached (see cache.py's invalidate_cache,
    # which only knows how to invalidate a Redis-backed cache - nothing a
    # unit test has), so it's mocked directly here rather than exercised for
    # real. What's under test is save_system_images()'s own merge logic:
    # what document it hands to save_system_settings.

    async def test_resets_only_the_named_field_leaving_the_others_untouched(self):
        existing = {"logo": "/vman/api/v1/uploads/logo.png", "favicon": "/vman/api/v1/uploads/fav.ico", "home_image": "/vman/api/v1/uploads/bg.png"}
        saved_documents = []

        async def fake_save_system_settings(data, db):
            saved_documents.append(data["system_images"])

        with patch("app.settings.services.odk_configs.get_system_images", new=AsyncMock(return_value=[existing])), \
             patch("app.settings.services.odk_configs.save_system_settings", new=fake_save_system_settings), \
             patch("app.settings.services.odk_configs.invalidate_cache", new=AsyncMock()):
            await save_system_images(data=ImagesConfigData(), fields_to_reset=["favicon"], db=None)

        assert saved_documents == [{"logo": existing["logo"], "favicon": None, "home_image": existing["home_image"]}]

    async def test_a_normal_partial_upload_only_overwrites_the_field_that_was_sent(self):
        existing = {"logo": "/vman/api/v1/uploads/logo.png", "favicon": "/vman/api/v1/uploads/fav.ico", "home_image": None}
        saved_documents = []

        async def fake_save_system_settings(data, db):
            saved_documents.append(data["system_images"])

        with patch("app.settings.services.odk_configs.get_system_images", new=AsyncMock(return_value=[existing])), \
             patch("app.settings.services.odk_configs.save_system_settings", new=fake_save_system_settings), \
             patch("app.settings.services.odk_configs.invalidate_cache", new=AsyncMock()):
            await save_system_images(data=ImagesConfigData(logo="/vman/api/v1/uploads/new-logo.png"), db=None)

        assert saved_documents == [{"logo": "/vman/api/v1/uploads/new-logo.png", "favicon": existing["favicon"], "home_image": None}]


class TestDeleteSingleSystemImageRoute:
    def _make_client(self, existing_images: dict) -> TestClient:
        app = FastAPI()
        app.include_router(settings_router)
        app.dependency_overrides[get_current_user] = lambda: {"uuid": "test-user"}
        app.dependency_overrides[get_current_user_privileges] = lambda: [AccessPrivileges.SETTINGS_UPDATE_SYSTEM_IMAGES]

        async def fake_db_dependency():
            yield FakeDB()

        app.dependency_overrides[get_arangodb_session] = fake_db_dependency
        return TestClient(app)

    def test_rejects_an_unknown_image_type(self):
        client = self._make_client({})

        response = client.delete("/settings/system_images/not-a-real-type")

        assert response.status_code == 400

    def test_resets_the_named_image_and_deletes_its_file(self):
        existing = {"logo": "/vman/api/v1/uploads/logo.png", "favicon": None, "home_image": None}
        client = self._make_client(existing)

        with patch("app.settings.settings_routes.get_system_images", new=AsyncMock(return_value=[existing])), \
             patch("app.settings.settings_routes.delete_file") as mock_delete_file, \
             patch("app.settings.settings_routes.save_system_images", new=AsyncMock(return_value=[{"logo": None, "favicon": None, "home_image": None}])) as mock_save:
            response = client.delete("/settings/system_images/logo")

        assert response.status_code == 200
        mock_delete_file.assert_called_once_with(existing["logo"])
        mock_save.assert_awaited_once()
        assert mock_save.await_args.kwargs["fields_to_reset"] == ["logo"]
        assert response.json()["data"] == [{"logo": None, "favicon": None, "home_image": None}]

    def test_does_not_try_to_delete_a_file_that_was_never_set(self):
        existing = {"logo": None, "favicon": None, "home_image": None}
        client = self._make_client(existing)

        with patch("app.settings.settings_routes.get_system_images", new=AsyncMock(return_value=[existing])), \
             patch("app.settings.settings_routes.delete_file") as mock_delete_file, \
             patch("app.settings.settings_routes.save_system_images", new=AsyncMock(return_value=[existing])):
            response = client.delete("/settings/system_images/favicon")

        assert response.status_code == 200
        mock_delete_file.assert_not_called()
