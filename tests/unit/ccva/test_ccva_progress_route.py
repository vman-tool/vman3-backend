from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.ccva.ccva_routes import ccva_router
from app.shared.configs.arangodb import get_arangodb_session
from app.users.decorators.user import get_current_user
from tests.support.fakes import FakeDB


def _make_client() -> TestClient:
    app = FastAPI()
    app.include_router(ccva_router)
    app.dependency_overrides[get_current_user] = lambda: {"uuid": "test-user"}

    async def fake_db_dependency():
        yield FakeDB()

    app.dependency_overrides[get_arangodb_session] = fake_db_dependency
    return TestClient(app)


def _async_result(state: str, result=None):
    mock = MagicMock()
    mock.state = state
    mock.result = result
    return mock


# GET /ccva/progress/{task_id} - covers the Celery-specific safety net used
# to resync a run's true status when TaskProgressService has nothing (or
# something stale) for it. Regression coverage for the "stuck at Running..."
# bug: run_ccva_task now persists progress durably (see
# tests/unit/tasks/test_ccva_tasks.py), but these branches are the
# defense-in-depth layer for the rare case a worker dies between the Redis
# publish and the DB write.
class TestGetCcvaProgressCelerySuccessFallback:
    def test_no_persisted_progress_but_celery_reports_success_returns_completed(self):
        with patch("app.ccva.ccva_routes.USE_CELERY", True), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=None),
             ), \
             patch(
                 "app.ccva.ccva_routes.AsyncResult",
                 return_value=_async_result("SUCCESS", {"status": "completed", "task_id": "t1", "elapsed_time": "0:05:00"}),
             ):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "completed"
        assert data["progress"] == 100
        assert data["elapsed_time"] == "0:05:00"
        assert data["error"] is False

    def test_no_persisted_progress_and_celery_has_no_result_still_404s(self):
        with patch("app.ccva.ccva_routes.USE_CELERY", True), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=None),
             ), \
             patch("app.ccva.ccva_routes.AsyncResult", return_value=_async_result("PENDING")):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 404

    def test_persisted_progress_stuck_running_but_celery_reports_success_is_corrected(self):
        # The exact "stuck at Running..." scenario before run_ccva_task's
        # own persistence fix landed: a worker died right after publishing
        # a "running" update but before ever persisting "completed".
        stuck_progress = {"status": "running", "progress": 42, "task_id": "t1", "timestamp": "2026-01-01T00:00:00"}
        with patch("app.ccva.ccva_routes.USE_CELERY", True), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=dict(stuck_progress)),
             ), \
             patch("app.ccva.ccva_routes.AsyncResult", return_value=_async_result("SUCCESS")):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "completed"
        assert data["progress"] == 100
        assert data["error"] is False

    def test_persisted_progress_already_completed_is_not_disturbed_by_the_success_check(self):
        already_done = {"status": "completed", "progress": 100, "task_id": "t1", "message": "CCVA analysis completed successfully"}
        with patch("app.ccva.ccva_routes.USE_CELERY", True), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=dict(already_done)),
             ), \
             patch("app.ccva.ccva_routes.AsyncResult", return_value=_async_result("SUCCESS")):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["message"] == "CCVA analysis completed successfully"

    def test_failure_state_still_takes_priority_over_a_stuck_running_status(self):
        stuck_progress = {"status": "running", "progress": 10, "task_id": "t1"}
        with patch("app.ccva.ccva_routes.USE_CELERY", True), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=dict(stuck_progress)),
             ), \
             patch("app.ccva.ccva_routes.AsyncResult", return_value=_async_result("FAILURE", "boom")):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "failed"
        assert data["error"] is True

    def test_non_celery_deployments_are_unaffected(self):
        progress = {"status": "running", "progress": 55, "task_id": "t1"}
        with patch("app.ccva.ccva_routes.USE_CELERY", False), \
             patch(
                 "app.ccva.ccva_routes.TaskProgressService.get_progress",
                 new=AsyncMock(return_value=dict(progress)),
             ):
            response = _make_client().get("/ccva/progress/t1")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "running"
        assert data["progress"] == 55
