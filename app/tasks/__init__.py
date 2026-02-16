"""
Tasks module for Celery background task processing.

This module contains:
- ccva_tasks: CCVA analysis background tasks (high priority)
- odk_tasks: ODK data sync background tasks (normal priority)
"""

from app.tasks.ccva_tasks import run_ccva_task
from app.tasks.odk_tasks import sync_odk_data_task

__all__ = [
    'run_ccva_task',
    'sync_odk_data_task',
]
