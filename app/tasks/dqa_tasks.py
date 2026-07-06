"""
DQA Analytics Background Tasks

Two tasks:
  compute_dqa_analytics_task  — full recompute (can be triggered manually or by schedule)
  check_dqa_analytics_schedule — runs every hour, fires compute task when the configured
                                  hour matches the current UTC hour.
"""

import asyncio
from datetime import datetime

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(
    bind=False,
    name="app.tasks.dqa_tasks.compute_dqa_analytics_task",
    ignore_result=True,
    max_retries=2,
    default_retry_delay=120,
)
def compute_dqa_analytics_task():
    """Recompute all four DQA indicators and persist the snapshot."""
    from app.shared.configs.arangodb import get_arangodb_client_sync
    from app.data_quality.services.dqa_analytics_service import (
        compute_and_store_dqa_analytics,
    )

    try:
        db = get_arangodb_client_sync()
        asyncio.run(compute_and_store_dqa_analytics(db))
        logger.info("DQA analytics snapshot recomputed successfully")
    except Exception as exc:
        logger.error(f"DQA analytics computation failed: {exc}")
        raise


@shared_task(
    bind=False,
    name="app.tasks.dqa_tasks.check_dqa_analytics_schedule",
    ignore_result=True,
)
def check_dqa_analytics_schedule():
    """
    Runs every hour (see celery_app.py beat schedule).
    Dispatches compute_dqa_analytics_task when the current UTC hour matches
    the configured run_hour AND the task hasn't already run today.
    """
    from app.shared.configs.arangodb import get_arangodb_client_sync
    from app.data_quality.services.dqa_analytics_service import (
        get_dqa_analytics_config,
        save_dqa_analytics_config,
    )

    try:
        db = get_arangodb_client_sync()
        config = asyncio.run(get_dqa_analytics_config(db))

        if not config.get("enabled", True):
            logger.debug("DQA scheduled analytics is disabled — skipping")
            return

        run_hour = int(config.get("run_hour", 2))
        current_hour = datetime.utcnow().hour

        if current_hour != run_hour:
            return

        today = datetime.utcnow().date().isoformat()
        if config.get("last_triggered_date") == today:
            logger.debug(f"DQA analytics already triggered today ({today}) — skipping")
            return

        # Record that we triggered today so the next hourly check won't re-fire.
        config["last_triggered_date"] = today
        asyncio.run(save_dqa_analytics_config(db, config))

        compute_dqa_analytics_task.delay()
        logger.info(f"DQA analytics computation scheduled for {today} at UTC {run_hour:02d}:00")

    except Exception as exc:
        logger.error(f"DQA schedule check failed: {exc}")
