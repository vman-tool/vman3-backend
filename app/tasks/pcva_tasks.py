"""
PCVA background tasks.

Single-record ML analysis runs here rather than in the API process. Inference
takes 20-30 seconds on a developer machine and roughly five times that on a
modest server, which is past gunicorn's --timeout 120: the request was killed
outright, leaving no traceback and a bare "analysis failed" in the UI. The
worker also pre-warms the predictor at process init (see celery_app), so the
model is already resident here, and a two-minute job no longer occupies one of
only two API workers.
"""

import time

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task(
    bind=True,
    name="app.tasks.pcva_tasks.analyse_va_with_ml_task",
    ignore_result=True,
)
def analyse_va_with_ml_task(self, va_id: str, user_id: str = None):
    """Predict the cause of death for one VA, reporting progress as it goes."""
    from app.shared.configs.arangodb import get_arangodb_client_sync
    from app.shared.services.task_progress_service import TaskProgressService
    from app.pcva.services.ml_analysis_service import analyse_va_with_ml_sync

    task_id = self.request.id
    db = get_arangodb_client_sync()
    started = time.time()

    def save(payload):
        try:
            TaskProgressService._save_progress_sync(db, task_id, payload)
        except Exception as exc:  # progress must never sink the analysis
            logger.warning(f"Could not record ML analysis progress: {exc}")

    def progress(pct, message):
        save({
            "status": "running",
            "progress": pct,
            "message": message,
            "va_id": va_id,
            "user_id": user_id,
            "elapsed_time": round(time.time() - started, 1),
        })

    save({"status": "running", "progress": 0, "message": "Queued...",
          "va_id": va_id, "user_id": user_id, "elapsed_time": 0})

    try:
        result = analyse_va_with_ml_sync(va_id, db, progress=progress)
        save({
            "status": "completed",
            "progress": 100,
            "message": f"Probable cause of death: {result.get('cause')}",
            "va_id": va_id,
            "user_id": user_id,
            "elapsed_time": round(time.time() - started, 1),
            "result": result,
        })
        logger.info(f"ML analysis for {va_id} completed in {time.time() - started:.1f}s")
    except Exception as exc:
        logger.error(f"ML analysis for {va_id} failed: {exc}")
        save({
            "status": "failed",
            "progress": 100,
            "message": str(exc),
            "va_id": va_id,
            "user_id": user_id,
            "elapsed_time": round(time.time() - started, 1),
            "error": str(exc),
        })
        raise
