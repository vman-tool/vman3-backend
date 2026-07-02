"""
ODK Data Sync Background Tasks

Celery tasks for ODK data synchronization with:
- Progress updates via Redis Pub/Sub
- Per-page retry (not whole-task retry) to prevent counter resets on timeout
- Sync history recording
- Cooperative cancellation via Redis flag (sync:cancel:<task_id>)
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import redis
from celery import shared_task
from celery.utils.log import get_task_logger
from decouple import config
from json import loads

logger = get_task_logger(__name__)

CANCEL_KEY_PREFIX = "sync:cancel:"
CANCEL_KEY_TTL    = 3600  # seconds — used for cancel flag and celery task ID keys
SNAPSHOT_TTL      = 300   # seconds — snapshot is refreshed every page; if the task
                           # dies the guard auto-unlocks within this window


# ── Redis helpers ─────────────────────────────────────────────────────────────

def get_redis_client():
    redis_url = config('REDIS_URL', default='redis://localhost:6370')
    redis_password = config('REDIS_PASSWORD', default=None)
    return redis.from_url(redis_url, password=redis_password, decode_responses=True)


def publish_progress(task_id: str, progress_data: dict):
    try:
        r = get_redis_client()
        r.publish(f"ws:broadcast:{task_id}", json.dumps(progress_data))
    except Exception as e:
        logger.error(f"Failed to publish ODK progress: {e}")


def _is_cancelled(task_id: str) -> bool:
    try:
        r = get_redis_client()
        return r.exists(f"{CANCEL_KEY_PREFIX}{task_id}") > 0
    except Exception:
        return False


def _clear_cancel_flag(task_id: str) -> None:
    try:
        r = get_redis_client()
        r.delete(f"{CANCEL_KEY_PREFIX}{task_id}")
    except Exception:
        pass


def _update_snapshot(task_id: str, records_saved: int, start_time: float,
                     user_name: str, method: str, total_data_count: int) -> None:
    """Write live progress to Redis so the cancel endpoint can save accurate history."""
    try:
        r = get_redis_client()
        r.setex(f"sync:snapshot:{task_id}", SNAPSHOT_TTL, json.dumps({
            "records_saved": records_saved,
            "start_time": start_time,
            "user_name": user_name,
            "method": method,
            "total_data_count": total_data_count,
        }))
    except Exception:
        pass


def _clear_snapshot(task_id: str) -> None:
    try:
        r = get_redis_client()
        r.delete(f"sync:snapshot:{task_id}")
    except Exception:
        pass


# ── Main task ─────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name='app.tasks.odk_tasks.sync_odk_data_task',
    # No autoretry_for — that would restart the whole task from scratch on any
    # timeout, causing the progress counter to reset.  Per-page retry is handled
    # inside the async loop instead.
    max_retries=0,
    acks_late=True,
)
def sync_odk_data_task(
    self,
    total_data_count: int,
    task_id: str = "123",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip: int = 0,
    top: int = 100,
    user_name: str = "System",
    method: str = "api",
    server_total: int = 0,
    local_count: int = 0,
):
    # Clear any stale cancel flag from a previous run before starting
    _clear_cancel_flag(task_id)

    # Store this task's Celery UUID so the cancel endpoint can hard-kill it
    try:
        r = get_redis_client()
        r.setex(f"sync:celery_task_id:{task_id}", CANCEL_KEY_TTL, self.request.id)
    except Exception as e:
        logger.warning(f"Could not store Celery task ID in Redis: {e}")

    _server_total = server_total if server_total > 0 else total_data_count
    logger.info(
        f"Starting ODK sync task {task_id}: {total_data_count} new records "
        f"(server={_server_total}, local={local_count}, user={user_name})"
    )
    start_time = time.time()
    _update_snapshot(task_id, 0, start_time, user_name, method, total_data_count)

    publish_progress(task_id, {
        "total_records": total_data_count,
        "server_total": _server_total,
        "local_count": local_count,
        "progress": 0,
        "elapsed_time": 0,
        "records_processed": 0,
        "status": "running",
        "message": f"Starting ODK data sync... {total_data_count:,} new records",
    })

    try:
        from app.shared.configs.arangodb import get_arangodb_client_sync
        from app.settings.services.odk_configs import fetch_odk_config
        from app.odk.utils.odk_client import ODKClientAsync
        from app.odk.services.data_download import (
            insert_many_data_to_arangodb,
            update_sync_status_internal,
            get_margin_dates_and_records_count,
        )

        db = get_arangodb_client_sync()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            config_obj = loop.run_until_complete(fetch_odk_config(db))
            records_saved = 0

            async def fetch_and_process() -> Tuple[int, bool]:
                nonlocal records_saved
                was_cancelled = False

                async def fetch_page_with_retry(odk_client, max_attempts=3, **kwargs):
                    last_exc = None
                    for attempt in range(max_attempts):
                        try:
                            return await odk_client.getFormSubmissions(**kwargs)
                        except Exception as exc:
                            last_exc = exc
                            logger.warning(
                                f"Page fetch attempt {attempt + 1}/{max_attempts} failed: {exc}. "
                                + ("Retrying..." if attempt < max_attempts - 1 else "Giving up.")
                            )
                            if attempt < max_attempts - 1:
                                await asyncio.sleep(2 ** attempt)  # 1s, 2s
                    raise last_exc

                async with ODKClientAsync(config_obj.odk_api_configs) as odk_client:
                    next_link = None
                    first_page = True

                    while True:
                        # ── Cooperative cancellation check ────────────────────
                        # Runs before every page fetch so cancellation takes
                        # effect between pages (never mid-download).
                        if _is_cancelled(task_id):
                            logger.info(
                                f"Sync {task_id} cancelled by user after {records_saved} records"
                            )
                            _clear_cancel_flag(task_id)
                            was_cancelled = True
                            break
                        # ──────────────────────────────────────────────────────

                        if next_link:
                            data = await fetch_page_with_retry(odk_client, next_link=next_link)
                        else:
                            data = await fetch_page_with_retry(
                                odk_client,
                                top=top,
                                skip=skip if first_page else None,
                                start_date=start_date,
                                end_date=end_date,
                                order_by='__system/submissionDate',
                                order_direction='asc',
                            )
                            first_page = False

                        if isinstance(data, str):
                            raise Exception(f"Unexpected string response: {data}")

                        page_records = data.get('value', [])
                        if not page_records:
                            break

                        df = pd.json_normalize(page_records, sep='/')
                        df.columns = [col.split('/')[-1] for col in df.columns]
                        df.columns = df.columns.str.lower()
                        df = df.dropna(axis=1, how='all')
                        df = df.loc[:, ~df.columns.duplicated()]
                        records = loads(df.to_json(orient='records'))

                        await insert_many_data_to_arangodb(records, overwrite_mode='replace')
                        records_saved += len(records)
                        _update_snapshot(task_id, records_saved, start_time, user_name, method, total_data_count)

                        progress = min((records_saved / total_data_count) * 100, 100.0)
                        elapsed = time.time() - start_time

                        publish_progress(task_id, {
                            "total_records": total_data_count,
                            "server_total": _server_total,
                            "local_count": local_count,
                            "progress": progress,
                            "elapsed_time": elapsed,
                            "records_processed": records_saved,
                            "status": "running",
                            "message": f"Syncing... {records_saved:,}/{total_data_count:,} new records",
                        })

                        next_link = data.get('@odata.nextLink')
                        if not next_link:
                            break

                # ── Post-loop bookkeeping ─────────────────────────────────────
                if was_cancelled:
                    # History already saved by the cancel endpoint from the Redis snapshot.
                    # Clear the snapshot so a late cancel request doesn't create a duplicate.
                    _clear_snapshot(task_id)
                else:
                    current_records = await get_margin_dates_and_records_count(db)
                    current_total = current_records.get('total_records', 0) if current_records else 0
                    await update_sync_status_internal(db, records_saved, current_total)
                    await _save_sync_history(
                        db,
                        records_synced=records_saved,
                        total_records=total_data_count,
                        user_name=user_name,
                        duration_seconds=time.time() - start_time,
                        method=method,
                        status="completed",
                    )
                    _clear_snapshot(task_id)
                    # Bust the Dashboard stats Redis cache so the new count shows immediately.
                    try:
                        _r = get_redis_client()
                        for key in _r.scan_iter("*submissions_statistics*"):
                            _r.delete(key)
                    except Exception:
                        pass

                return records_saved, was_cancelled

            records_saved, was_cancelled = loop.run_until_complete(fetch_and_process())

        finally:
            loop.close()

        elapsed = time.time() - start_time

        if was_cancelled:
            pct = min((records_saved / total_data_count) * 100, 100.0) if total_data_count else 0
            publish_progress(task_id, {
                "total_records": total_data_count,
                "server_total": _server_total,
                "local_count": local_count,
                "progress": pct,
                "elapsed_time": elapsed,
                "records_processed": records_saved,
                "status": "cancelled",
                "message": f"Sync cancelled — {records_saved:,} records saved in {elapsed:.0f}s",
            })
            logger.info(f"ODK sync task {task_id} cancelled: {records_saved} records saved in {elapsed:.2f}s")
            return {"status": "cancelled", "records_saved": records_saved, "elapsed_time": elapsed}
        else:
            publish_progress(task_id, {
                "total_records": total_data_count,
                "server_total": _server_total,
                "local_count": local_count,
                "progress": 100,
                "elapsed_time": elapsed,
                "records_processed": records_saved,
                "status": "completed",
                "message": f"Sync completed: {records_saved:,} new records in {elapsed:.0f}s",
            })
            logger.info(f"ODK sync task {task_id} completed: {records_saved} records in {elapsed:.2f}s")
            return {"status": "completed", "records_saved": records_saved, "elapsed_time": elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"ODK sync task {task_id} failed after {elapsed:.1f}s: {e}")
        publish_progress(task_id, {
            "total_records": total_data_count,
            "server_total": _server_total,
            "local_count": local_count,
            "progress": 0,
            "elapsed_time": elapsed,
            "records_processed": 0,
            "status": "error",
            "message": f"Sync failed: {e}",
            "error": True,
        })
        try:
            from app.shared.configs.arangodb import get_arangodb_client_sync
            _db = get_arangodb_client_sync()
            _loop = asyncio.new_event_loop()
            _loop.run_until_complete(
                _save_sync_history(
                    _db,
                    records_synced=0,
                    total_records=total_data_count,
                    user_name=user_name,
                    duration_seconds=elapsed,
                    method=method,
                    status="failed",
                )
            )
            _loop.close()
        except Exception:
            pass
        raise


# ── Sync history helper ───────────────────────────────────────────────────────

async def _save_sync_history(db, records_synced: int, total_records: int,
                              user_name: str, duration_seconds: float,
                              method: str, status: str):
    try:
        from app.shared.configs.constants import db_collections

        record = {
            "date": datetime.now(timezone.utc).isoformat(),
            "records_synced": records_synced,
            "total_records": total_records,
            "user_name": user_name,
            "duration_seconds": round(duration_seconds, 1),
            "method": method,
            "status": status,
        }

        col_name = db_collections.SYNC_HISTORY
        if not db.has_collection(col_name):
            db.create_collection(col_name)
        db.collection(col_name).insert(record)
        logger.info(f"Saved sync history record: {status}, {records_synced} records")
    except Exception as e:
        logger.error(f"Failed to save sync history: {e}")
