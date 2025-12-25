


from datetime import datetime
from arango.database import StandardDatabase
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
    top: int = 3000,
    force_update: bool = Query(default=False),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        records_margins= await data_download. get_margin_dates_and_records_count(db)
        print(records_margins)
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
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

#@log_to_db(context="sync_odk_data_with_async", log_args=True)
@odk_router.post("/sync_odk_data_with_async", status_code=status.HTTP_200_OK)
async def fetch_odk_data_with_async_endpoint(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    force_update: bool = Query(default=False),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    app_logger.info(f"Fetching ODK data with async: {datetime.now().isoformat()}")
    app_logger.info(f"Start date: {start_date}, End date: {end_date}, Skip: {skip}, Top: {top}")
    
    try:
        # First, do initial validation and get data count
        initial_response = await data_download.fetch_odk_data_initial(
            db=db,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            top=1,
            force_update=force_update
        )

        # If download is needed, start the background fetch task
        if initial_response.get('download_status') is True:
            # Create a proper background task function
            async def background_fetch_task():
                await data_download.fetch_odk_data_with_async(
                    db=db,
                    start_date=initial_response.get('start_date'),
                    end_date=end_date,
                    skip=skip,
                    top=top,
                    total_data_count=initial_response.get('total_data_count', 0)
                )
            
            # Add the background task
            background_tasks.add_task(background_fetch_task)
            
        return {"status": "Data fetch initiated", **initial_response}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


#@log_to_db(context="get_form_questions", log_args=True)
@odk_router.post("/fetch_form_questions", status_code=status.HTTP_200_OK)
async def get_form_questions(
    db: StandardDatabase = Depends(get_arangodb_session)
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
        print(f"Error getting sync status: {e}")
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