from datetime import datetime
from unittest.mock import MagicMock, patch

from app.tasks.odk_tasks import check_odk_sync_schedule


def _fake_db(cron_settings):
    collection = MagicMock()
    collection.get.return_value = {"cron_settings": cron_settings} if cron_settings is not None else {}
    db = MagicMock()
    db.collection.return_value = collection
    return db


def test_does_not_dispatch_when_no_days_are_configured():
    # Regression: this is now the ONLY thing that triggers a scheduled ODK
    # sync - a redundant in-process APScheduler job used to also dispatch
    # the same sync independently (with no dedup lock), so a configured
    # schedule could fire twice. That path was removed; this task must be
    # the sole gate on cron_settings.
    fake_db = _fake_db({"days": [], "time": "09:00"})
    with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
         patch("app.tasks.odk_tasks.get_redis_client") as mock_get_redis, \
         patch("app.tasks.odk_tasks.datetime") as mock_dt, \
         patch("app.tasks.odk_tasks.run_scheduled_odk_sync") as mock_sync_task:
        mock_dt.utcnow.return_value = datetime(2026, 9, 1, 9, 0)
        check_odk_sync_schedule()

    mock_sync_task.delay.assert_not_called()
    mock_get_redis.assert_not_called()  # never even needs the lock


def test_does_not_dispatch_when_the_current_day_or_time_does_not_match():
    fake_db = _fake_db({"days": ["monday"], "time": "09:00"})
    with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
         patch("app.tasks.odk_tasks.get_redis_client") as mock_get_redis, \
         patch("app.tasks.odk_tasks.datetime") as mock_dt, \
         patch("app.tasks.odk_tasks.run_scheduled_odk_sync") as mock_sync_task:
        mock_dt.utcnow.return_value = datetime(2026, 9, 1, 9, 0)  # a Tuesday
        check_odk_sync_schedule()

    mock_sync_task.delay.assert_not_called()


def test_dispatches_exactly_once_when_day_and_time_match():
    fake_db = _fake_db({"days": ["tuesday"], "time": "09:00"})
    fake_redis = MagicMock()
    fake_redis.set.return_value = True  # NX lock acquired
    with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
         patch("app.tasks.odk_tasks.get_redis_client", return_value=fake_redis), \
         patch("app.tasks.odk_tasks.datetime") as mock_dt, \
         patch("app.tasks.odk_tasks.run_scheduled_odk_sync") as mock_sync_task:
        mock_dt.utcnow.return_value = datetime(2026, 9, 1, 9, 0)  # a Tuesday
        check_odk_sync_schedule()

    mock_sync_task.delay.assert_called_once()


def test_skips_when_the_per_minute_lock_is_already_held():
    # The lock (SET NX) is what prevents a double-fire if beat's own
    # schedule ever overlaps within the same matching minute.
    fake_db = _fake_db({"days": ["tuesday"], "time": "09:00"})
    fake_redis = MagicMock()
    fake_redis.set.return_value = False  # NX failed - another worker already holds it
    with patch("app.shared.configs.arangodb.get_arangodb_client_sync", return_value=fake_db), \
         patch("app.tasks.odk_tasks.get_redis_client", return_value=fake_redis), \
         patch("app.tasks.odk_tasks.datetime") as mock_dt, \
         patch("app.tasks.odk_tasks.run_scheduled_odk_sync") as mock_sync_task:
        mock_dt.utcnow.return_value = datetime(2026, 9, 1, 9, 0)
        check_odk_sync_schedule()

    mock_sync_task.delay.assert_not_called()
