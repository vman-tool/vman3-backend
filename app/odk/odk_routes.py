


import uuid
from datetime import datetime
from arango.database import StandardDatabase
from decouple import config
from fastapi import (APIRouter, BackgroundTasks, Depends, HTTPException, Query,
                     status)

from app.odk.services import data_download
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.utilits.db_logger import db_logger, log_to_db
from app.utilits.logger import app_logger
from app.settings.services.odk_configs import add_configs_settings
from app.settings.models.settings import SettingsConfigData, SyncStatus
from app.shared.configs.constants import db_collections

# Celery task imports
from app.tasks.odk_tasks import sync_odk_data_task, get_redis_client, CANCEL_KEY_TTL, ACTIVE_TASK_KEY

from app.users.decorators.user import check_privileges, get_current_user
from app.shared.configs.constants import AccessPrivileges
from typing import List

odk_router = APIRouter(
    prefix="/odk",
    tags=["ODK"],
    responses={404: {"description": "Not found"}},
)



#@log_to_db(context="fetch_and_store_data", log_args=True)
@odk_router.post("/fetch-and-store", status_code=status.HTTP_201_CREATED)
async def fetch_and_store_data(
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.ODK_DATA_SYNC]))
):

    res= await data_download.fetch_odk_datas(
          start_date= start_date,
    end_date=end_date,
    skip=skip,
    top=top
    )
    return res

#@log_to_db(context="get_form_submission_status", log_args=True)
@odk_router.post("/fetch_formsubmission_status", status_code=status.HTTP_200_OK)
async def get_form_submission_status(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
    force_update: bool = Query(default=False),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        records_margins= await data_download. get_margin_dates_and_records_count(db)
        if records_margins is not None:
            earliest_date = records_margins.get('earliest_date', None)
            latest_date = records_margins.get('latest_date', None)
            total_records = records_margins.get('total_records', 0)
            return {
                    "status": "Data fetch",
                    "earliest_date": latest_date,
                    "latest_date": earliest_date,
                    "available_data_count": total_records
                }
        else:
            return {
                    "status": "failed to fetch data",
                    "earliest_date": None,
                    "latest_date": None,
                    "available_data_count": 0
                }
           
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#@log_to_db(context="sync_odk_data_with_async", log_args=True)
@odk_router.post("/sync_odk_data_with_async", status_code=status.HTTP_200_OK)
async def fetch_odk_data_with_async_endpoint(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
    force_update: bool = Query(default=False),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.ODK_DATA_SYNC])),
    current_user=Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    app_logger.info(f"ODK sync requested by {getattr(current_user, 'email', 'unknown')} at {datetime.now().isoformat()}")

    try:
        # Guard: reject if a sync is already running
        r = get_redis_client()
        active_id = r.get(ACTIVE_TASK_KEY)
        if active_id and r.exists(f"sync:snapshot:{active_id}"):
            raise HTTPException(
                status_code=409,
                detail="A sync is already in progress. Cancel it before starting a new one."
            )

        initial_response = await data_download.fetch_odk_data_initial(
            db=db,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            top=1,
            force_update=force_update
        )

        if initial_response.get('download_status') is True:
            total_data_count = initial_response.get('total_data_count', 0)
            fetch_start_date = initial_response.get('start_date')

            user_name = (
                getattr(current_user, 'name', None)
                or getattr(current_user, 'email', None)
                or "System"
            )

            task_id = str(uuid.uuid4())
            r.setex(ACTIVE_TASK_KEY, CANCEL_KEY_TTL, task_id)

            sync_odk_data_task.delay(
                total_data_count=total_data_count,
                task_id=task_id,
                start_date=fetch_start_date,
                end_date=end_date,
                skip=skip,
                top=top,
                user_name=user_name,
                method="api",
                server_total=initial_response.get('server_total', total_data_count),
                local_count=initial_response.get('local_count', 0),
            )
            app_logger.info(f"ODK sync task dispatched (id={task_id}, user={user_name}, {total_data_count} records)")
        else:
            task_id = None

        return {"status": "Data fetch initiated", "using_celery": True, "task_id": task_id, **initial_response}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#@log_to_db(context="get_form_questions", log_args=True)
@odk_router.post("/fetch_form_questions", status_code=status.HTTP_200_OK)
async def get_form_questions(
    db: StandardDatabase = Depends(get_arangodb_session),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.ODK_QUESTIONS_SYNC]))
) -> ResponseMainModel:
    try:
        return await data_download.fetch_form_questions(db=db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Manual sync status update endpoint removed - sync status is now updated automatically by backend


#@log_to_db(context="get_sync_status", log_args=True)
@odk_router.get("/sync_status", status_code=status.HTTP_200_OK)
async def get_sync_status(
    db: StandardDatabase = Depends(get_arangodb_session)
) -> ResponseMainModel:
    """Get the current sync status from settings configuration"""
    try:
        # Check if the collection exists first
        if not db.has_collection(db_collections.SYSTEM_CONFIGS):
            # Get current total data count even if no sync status exists
            from app.odk.services.data_download import get_margin_dates_and_records_count
            current_records_info = await get_margin_dates_and_records_count(db)
            current_total_data = current_records_info.get('total_records', 0) if current_records_info else 0
            
            # Collection doesn't exist, return default sync status with current data count
            default_sync_status = {
                "last_sync_date": None,
                "last_sync_data_count": 0,
                "total_synced_data": current_total_data,
                "available_data_count": current_total_data,
                "earliest_date": current_records_info.get('earliest_date') if current_records_info else None,
                "latest_date": current_records_info.get('latest_date') if current_records_info else None
            }
            return ResponseMainModel(
                data=default_sync_status,
                message="System configs collection not found, returning default sync status with current data count"
            )
        
        # Get the current sync status from system_configs collection
        config_data = db.collection(db_collections.SYSTEM_CONFIGS).get('vman_config')
        
        # Get current total data count from database
        from app.odk.services.data_download import get_margin_dates_and_records_count
        current_records_info = await get_margin_dates_and_records_count(db)
        current_total_data = current_records_info.get('total_records', 0) if current_records_info else 0
        
        if config_data and 'sync_status' in config_data:
            sync_status = config_data['sync_status']
            # Update total_synced_data with current database count
            sync_status['total_synced_data'] = current_total_data
            
            # Also include form submission status data
            sync_status['available_data_count'] = current_total_data
            if current_records_info:
                sync_status['earliest_date'] = current_records_info.get('earliest_date')
                sync_status['latest_date'] = current_records_info.get('latest_date')
            
            return ResponseMainModel(
                data=sync_status,
                message="Sync status retrieved successfully"
            )
        else:
            # Return default sync status with current database count
            default_sync_status = {
                "last_sync_date": None,
                "last_sync_data_count": 0,
                "total_synced_data": current_total_data,
                "available_data_count": current_total_data,
                "earliest_date": current_records_info.get('earliest_date') if current_records_info else None,
                "latest_date": current_records_info.get('latest_date') if current_records_info else None
            }
            return ResponseMainModel(
                data=default_sync_status,
                message="No sync status found, returning default with current data count"
            )
        
    except Exception as e:
        app_logger.error(f"Error getting sync status: {e}")
        # Return default sync status instead of raising an error
        try:
            from app.odk.services.data_download import get_margin_dates_and_records_count
            current_records_info = await get_margin_dates_and_records_count(db)
            current_total_data = current_records_info.get('total_records', 0) if current_records_info else 0
        except:
            current_total_data = 0
            current_records_info = None
            
        default_sync_status = {
            "last_sync_date": None,
            "last_sync_data_count": 0,
            "total_synced_data": current_total_data,
            "available_data_count": current_total_data,
            "earliest_date": current_records_info.get('earliest_date') if current_records_info else None,
            "latest_date": current_records_info.get('latest_date') if current_records_info else None
        }
        return ResponseMainModel(
            data=default_sync_status,
            message=f"Error retrieving sync status: {str(e)}, returning default with current data count"
        )


@odk_router.post("/cancel-sync", status_code=status.HTTP_200_OK)
async def cancel_sync(
    db: StandardDatabase = Depends(get_arangodb_session),
) -> ResponseMainModel:
    """
    Cancel the running ODK sync task cooperatively.

    1. Reads the per-page Redis snapshot and saves a 'cancelled' history record immediately.
       This is reliable because it runs here in the endpoint, not in the task process.
    2. Sets the cooperative cancel flag so the Celery task exits at its next page boundary.

    No SIGTERM: terminating the worker process prevented history from ever being saved.
    """
    try:
        from app.tasks.odk_tasks import CANCEL_KEY_PREFIX, CANCEL_KEY_TTL, get_redis_client
        from fastapi.concurrency import run_in_threadpool
        import json as _json, time as _time
        from datetime import timezone

        r = get_redis_client()
        task_id = r.get(ACTIVE_TASK_KEY) or "123"

        # 1. Save cancelled history from snapshot BEFORE signalling the task.
        snapshot_json = r.get(f"sync:snapshot:{task_id}")
        if snapshot_json:
            try:
                snapshot = _json.loads(snapshot_json)
                elapsed = _time.time() - snapshot.get("start_time", _time.time())
                record = {
                    "date": datetime.now(timezone.utc).isoformat(),
                    "records_synced": snapshot.get("records_saved", 0),
                    "total_records": snapshot.get("total_data_count", 0),
                    "user_name": snapshot.get("user_name", "System"),
                    "duration_seconds": round(elapsed, 1),
                    "method": snapshot.get("method", "api"),
                    "status": "cancelled",
                }

                def _insert_history():
                    col_name = db_collections.SYNC_HISTORY
                    if not db.has_collection(col_name):
                        db.create_collection(col_name)
                    db.collection(col_name).insert(record)

                await run_in_threadpool(_insert_history)
                r.delete(f"sync:snapshot:{task_id}")
                app_logger.info(
                    f"Saved cancelled sync history: {snapshot.get('records_saved', 0)} records "
                    f"in {round(elapsed, 1)}s"
                )
            except Exception as hist_err:
                app_logger.error(f"Could not save cancelled history from snapshot: {hist_err}")

        # 2. Set cooperative cancel flag — task exits at next page boundary without SIGTERM.
        r.setex(f"{CANCEL_KEY_PREFIX}{task_id}", CANCEL_KEY_TTL, "1")

        # 3. Invalidate the Dashboard statistics cache so it refreshes on next load.
        try:
            from app.shared.utils.cache import invalidate_cache
            await invalidate_cache('submissions_statistics')
        except Exception:
            pass

        return ResponseMainModel(
            data={"cancelled": True},
            message="Cancellation requested"
        )
    except Exception as e:
        app_logger.error(f"Cancel sync error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@odk_router.post("/reset-sync-state", status_code=status.HTTP_200_OK)
async def reset_sync_state(
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.ODK_DATA_SYNC])),
) -> ResponseMainModel:
    """
    Clear all stale sync-related Redis keys.
    Call this when the Celery worker is being restarted after a stuck or
    interrupted sync — ensures the next sync start isn't blocked by stale state.
    """
    try:
        r = get_redis_client()
        active_id = r.get(ACTIVE_TASK_KEY)
        deleted_keys = []
        keys_to_delete = [ACTIVE_TASK_KEY]
        if active_id:
            keys_to_delete += [
                f"sync:snapshot:{active_id}",
                f"sync:cancel:{active_id}",
                f"sync:celery_task_id:{active_id}",
            ]
        for key in keys_to_delete:
            if r.delete(key):
                deleted_keys.append(key)
        app_logger.info(f"Sync state reset — cleared keys: {deleted_keys}")
        return ResponseMainModel(
            data={"cleared_keys": deleted_keys, "previous_task_id": active_id},
            message="Sync state cleared. Safe to restart Celery worker and start a new sync."
        )
    except Exception as e:
        app_logger.error(f"Reset sync state error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@odk_router.get("/active-sync", status_code=status.HTTP_200_OK)
async def get_active_sync(current_user=Depends(get_current_user)):
    """Returns whether a data sync is currently running and basic progress metadata."""
    try:
        import json as _json
        r = get_redis_client()

        # Last schedule fire info (persists 24 h so UI can always confirm it ran)
        fired_raw = r.get("odk:last_schedule_fired")
        last_schedule_fired = _json.loads(fired_raw) if fired_raw else None

        task_id = r.get(ACTIVE_TASK_KEY)
        if not task_id:
            return {"active": False, "last_schedule_fired": last_schedule_fired}
        # Task key exists — sync is active even if snapshot not written yet
        snapshot_raw = r.get(f"sync:snapshot:{task_id}")
        snap = _json.loads(snapshot_raw) if snapshot_raw else {}
        return {
            "active": True,
            "user_name": snap.get("user_name", "System"),
            "records_saved": snap.get("records_saved", 0),
            "total_data_count": snap.get("total_data_count", 0),
            "method": snap.get("method", "api"),
            "last_schedule_fired": last_schedule_fired,
        }
    except Exception:
        return {"active": False, "last_schedule_fired": None}


@odk_router.get("/sync-history", status_code=status.HTTP_200_OK)
async def get_sync_history(
    limit: int = Query(default=20, ge=1, le=100),
    db: StandardDatabase = Depends(get_arangodb_session),
) -> ResponseMainModel:
    """Return the most recent sync history entries, newest first."""
    try:
        from fastapi.concurrency import run_in_threadpool

        def _fetch():
            col_name = db_collections.SYNC_HISTORY
            if not db.has_collection(col_name):
                return []
            cursor = db.aql.execute(
                f"FOR doc IN {col_name} SORT doc.date DESC LIMIT @limit RETURN doc",
                bind_vars={"limit": limit},
            )
            return list(cursor)

        records = await run_in_threadpool(_fetch)
        return ResponseMainModel(data=records, message="Sync history retrieved", total=len(records))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



#TODOS: EXPERIMENTAL CODES: Do not delete
# @odk_router.post("/fetch_endpoint_with_async_old", status_code=status.HTTP_201_CREATED)
# async def fetch_odk_data(background_tasks: BackgroundTasks,  start_date: str = None,
#     end_date: str = None,
#     skip: int = 0,
#     top: int = 3000,      db: StandardDatabase = Depends(get_arangodb_session)):
#     try:
#         # await data_download_old.fetch_odk_data_with_async_old(
#         #         db=db,
#         #         start_date=start_date,
#         #         end_date=end_date,
#         #         skip=skip,
#         #         top=top)
#         background_tasks.add_task(
#                 data_download_old.fetch_odk_data_with_async_old,
#                 db=db,
#                 start_date=start_date,
#                 end_date=end_date,
#                 skip=skip,
#                 top=top
#             )
#         return {"status": "Data fetched sucessfuly"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @odk_router.get("/retry-failed-chunks",status_code=status.HTTP_200_OK)
# async def retry_failed_chunks_endpoint():
#     try:
#         await data_download.retry_failed_chunks()
#         return {"status": "Retries initiated for failed chunks"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

##TODOS: EXPERIMENTAL CODES: Do not delete