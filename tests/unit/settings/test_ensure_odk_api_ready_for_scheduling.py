from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.settings.services.odk_configs import ensure_odk_api_ready_for_scheduling
from app.shared.middlewares.exceptions import BadRequestException


def _fake_odk_client(getFormSubmissions_return):
    client = AsyncMock()
    client.getFormSubmissions.return_value = getFormSubmissions_return
    client.__aenter__.return_value = client
    client.__aexit__.return_value = False
    return client


async def test_rejects_when_odk_api_was_never_configured():
    # Matches the reported scenario: data was uploaded via CSV, the ODK API
    # tab was never saved at all, so odk_api_configs is None.
    fake_config = SimpleNamespace(odk_api_configs=None)
    with patch("app.settings.services.odk_configs.fetch_odk_config", new=AsyncMock(return_value=fake_config)):
        with pytest.raises(BadRequestException, match="not configured"):
            await ensure_odk_api_ready_for_scheduling(db=None)


async def test_rejects_when_the_vman_config_document_does_not_exist_at_all():
    with patch("app.settings.services.odk_configs.fetch_odk_config", new=AsyncMock(side_effect=ValueError("not found"))):
        with pytest.raises(BadRequestException, match="not configured"):
            await ensure_odk_api_ready_for_scheduling(db=None)


async def test_rejects_when_credentials_are_present_but_the_server_rejects_them():
    # The exact scenario the user described: odk_api_configs holds the
    # unedited placeholder values (so it *looks* configured), but the
    # password is wrong - a live check is the only thing that can catch
    # this, since the config data itself is present and well-formed.
    fake_config = SimpleNamespace(odk_api_configs=SimpleNamespace(
        url="https://central.iact.co.tz", username="admin@vman.net",
        password="password", form_id="WHOVA_V1_5_3_TZV1", project_id="2",
        api_version="v1", is_sort_allowed=False,
    ))
    fake_client = _fake_odk_client(getFormSubmissions_return=None)  # auth failed -> no data
    with patch("app.settings.services.odk_configs.fetch_odk_config", new=AsyncMock(return_value=fake_config)), \
         patch("app.settings.services.odk_configs.ODKClientAsync", return_value=fake_client):
        with pytest.raises(BadRequestException, match="Could not connect"):
            await ensure_odk_api_ready_for_scheduling(db=None)


async def test_rejects_when_the_odk_server_is_unreachable():
    fake_config = SimpleNamespace(odk_api_configs=SimpleNamespace(url="https://unreachable.example"))
    with patch("app.settings.services.odk_configs.fetch_odk_config", new=AsyncMock(return_value=fake_config)), \
         patch("app.settings.services.odk_configs.ODKClientAsync", side_effect=ConnectionError("refused")):
        with pytest.raises(BadRequestException, match="Could not connect"):
            await ensure_odk_api_ready_for_scheduling(db=None)


async def test_passes_silently_when_the_odk_api_is_reachable_and_authenticates():
    fake_config = SimpleNamespace(odk_api_configs=SimpleNamespace(url="https://central.iact.co.tz"))
    fake_client = _fake_odk_client(getFormSubmissions_return=[{"id": "a"}])
    with patch("app.settings.services.odk_configs.fetch_odk_config", new=AsyncMock(return_value=fake_config)), \
         patch("app.settings.services.odk_configs.ODKClientAsync", return_value=fake_client):
        await ensure_odk_api_ready_for_scheduling(db=None)  # no exception
