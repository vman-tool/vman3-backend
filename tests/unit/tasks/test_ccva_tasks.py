from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.tasks.ccva_tasks import run_ccva_task


def _fake_config():
    return SimpleNamespace(
        field_mapping=SimpleNamespace(instance_id="instanceid", interview_date="id10012")
    )


def _run(records_data=None, **kwargs):
    """Runs run_ccva_task with everything it needs mocked out except the
    progress-reporting path itself, which is what these tests exercise.
    Returns (mock_publish, save_progress) so callers can inspect both the
    live Redis broadcast and the durable DB write.
    """
    save_progress = AsyncMock()
    fake_db = MagicMock()

    with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
         patch(
             "app.shared.services.task_progress_service.TaskProgressService.save_progress",
             new=save_progress,
         ), \
         patch("app.tasks.ccva_tasks.publish_progress") as mock_publish, \
         patch(
             "app.settings.services.odk_configs.fetch_odk_config",
             new=AsyncMock(return_value=_fake_config()),
         ), \
         patch("app.ccva.services.ccva_services.runCCVA") as mock_run_ccva:
        mock_run_ccva.return_value = kwargs.pop("ccva_result", {"task_id": kwargs.get("task_id", "t1")})
        run_ccva_task(
            records_data=records_data if records_data is not None else [
                {"instanceid": "va-1", "id10012": "2026-01-01"}
            ],
            task_id=kwargs.pop("task_id", "t1"),
            user_id=kwargs.pop("user_id", "user-1"),
            **kwargs,
        )

    return mock_publish, save_progress


class TestRunCcvaTaskProgressPersistence:
    # Regression: run_ccva_task used to report progress ONLY via a one-shot
    # Redis pub/sub publish (publish_progress) - a missed WebSocket message
    # (network blip, proxy/idle timeout, worker recycle) left no durable
    # record anywhere, so GET /ccva/progress/{task_id} always 404'd for a
    # Celery-run task and a page refresh could never resync a
    # completed/failed run - the exact "stuck at 5%, spinner never stops"
    # bug this file guards against.

    def test_every_broadcast_has_a_matching_durable_persist(self):
        mock_publish, save_progress = _run()

        assert mock_publish.call_count >= 4  # initial, prepare, running, completed
        assert save_progress.call_count == mock_publish.call_count

    def test_the_very_first_update_is_persisted_too(self):
        # Not just the ones after runCCVA starts - db is now created before
        # the first publish, specifically so this one isn't lost either.
        _, save_progress = _run()

        first_persisted = save_progress.call_args_list[0].args[2]
        assert first_persisted["status"] == "running"
        assert first_persisted["progress"] == 1

    def test_the_final_persisted_update_reflects_true_completion(self):
        _, save_progress = _run(task_id="t1", ccva_result={"task_id": "t1", "total_records": 1})

        final = save_progress.call_args_list[-1].args[2]
        assert final["status"] == "completed"
        assert final["progress"] == 100
        assert final["task_id"] == "t1"

    def test_persists_using_the_same_task_id_and_db_for_every_call(self):
        _, save_progress = _run(task_id="run-42")

        task_ids = {call.args[1] for call in save_progress.call_args_list}
        dbs = {id(call.args[0]) for call in save_progress.call_args_list}
        assert task_ids == {"run-42"}
        assert len(dbs) == 1  # same db object reused, not reconnected per update

    def test_error_status_is_persisted_when_the_pipeline_raises(self):
        save_progress = AsyncMock()
        fake_db = MagicMock()

        with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
             patch(
                 "app.shared.services.task_progress_service.TaskProgressService.save_progress",
                 new=save_progress,
             ), \
             patch("app.tasks.ccva_tasks.publish_progress"), \
             patch(
                 "app.settings.services.odk_configs.fetch_odk_config",
                 new=AsyncMock(return_value=_fake_config()),
             ), \
             patch("app.ccva.services.ccva_services.runCCVA", side_effect=ValueError("boom")), \
             patch.object(run_ccva_task, "retry", side_effect=lambda exc, **_: exc):
            # self.retry(exc=e) is mocked to hand the exception straight
            # back rather than invoking real Celery retry machinery (no
            # broker in a unit test) - run_ccva_task then does `raise
            # self.retry(exc=e)`, so the original exception still
            # propagates, same as it would once Celery's own retry/
            # max-retries handling took over for real. autoretry_for also
            # wraps the task and calls task.retry(exc=..., countdown=...)
            # itself on top, hence **_ rather than a fixed signature.
            with pytest.raises(ValueError, match="boom"):
                run_ccva_task(
                    records_data=[{"instanceid": "va-1", "id10012": "2026-01-01"}],
                    task_id="t1",
                    user_id="user-1",
                )

        final = save_progress.call_args_list[-1].args[2]
        assert final["status"] == "error"
        assert final["error"] is True
        assert "boom" in final["message"]
