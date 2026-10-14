import json
from unittest.mock import AsyncMock, patch

import pytest

from app.ccva.services.ccva_services import websocket_broadcast
from app.ccva.models.ccva_models import InterVA5Progress


@pytest.fixture
def mock_manager():
    manager = AsyncMock()
    with patch("app.main.websocket__manager", manager):
        yield manager


class TestWebsocketBroadcast:
    async def test_dict_payload_is_sent_as_object(self, mock_manager):
        await websocket_broadcast("task-1", {"progress": 42, "status": "running"})

        sent = mock_manager.broadcast.call_args.args[1]
        assert json.loads(sent) == {"progress": 42, "status": "running"}

    async def test_pydantic_model_payload_is_sent_as_object(self, mock_manager):
        payload = InterVA5Progress(
            progress=4,
            message="Running InterVA5 analysis...",
            status="running",
            total_records=10,
            elapsed_time="0:0:1",
            task_id="task-1",
            error=False,
        )

        await websocket_broadcast("task-1", payload)

        sent = mock_manager.broadcast.call_args.args[1]
        decoded = json.loads(sent)
        assert isinstance(decoded, dict)
        assert decoded["progress"] == 4

    async def test_pre_serialized_json_string_is_not_double_encoded(self, mock_manager):
        # Regression test: some callers pass an already-serialized payload
        # via `SomeModel(...).model_dump_json()`. Before the fix, this string
        # was re-dumped as-is, producing a JSON-encoded *string* over the
        # wire instead of a JSON object - the frontend's
        # `JSON.parse(event.data)` then yielded a string primitive, so every
        # field access (progress, status, ...) was undefined and the update
        # was silently dropped.
        payload = InterVA5Progress(
            progress=1,
            message="Collecting data.",
            status="running",
            total_records=5,
            elapsed_time="0:0:0",
            task_id="task-1",
            error=False,
        ).model_dump_json()
        assert isinstance(payload, str)

        await websocket_broadcast("task-1", payload)

        sent = mock_manager.broadcast.call_args.args[1]
        decoded = json.loads(sent)
        assert isinstance(decoded, dict)
        assert decoded["progress"] == 1
        assert decoded["status"] == "running"
