import io
import time
import uuid
from datetime import date
from typing import List, Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)

from app.ccva.services.ccva_upload import insert_all_csv_data
from app.settings.models.settings import ImagesConfigData, SettingsConfigData, SyncStatus
from app.settings.services.cron import BackupSettings, CronSettings, fetch_backup_settings, fetch_cron_settings, save_backup_settings, save_cron_settings
from app.settings.services.data_reset import preview_reset, reset_va_data
from app.settings.services.xform_dictionary import enrich_questions_from_xform, get_data_dictionary, update_question_label
from app.settings.services.expected_deaths import (
    delete_expected_deaths_node,
    get_expected_deaths_tree,
    import_expected_deaths_from_xform,
    update_expected_deaths_value,
)
from app.settings.services.odk_configs import (
    add_configs_settings,
    ensure_odk_api_ready_for_scheduling,
    fetch_configs_settings,
    get_questioners_fields,
    get_system_images,
    save_system_images,
)
from app.shared.utils.cache import invalidate_cache_pattern
from app.shared.utils.odk_columns import clean_odk_columns, synthesize_coordinates_from_latlon
from datetime import datetime, timezone
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import AccessPrivileges, data_sources, db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.services.va_records import get_field_value_from_va_records
from app.users.decorators.user import check_privileges, get_current_user
from app.utilits.db_logger import db_logger, log_to_db
from app.utilits.helpers import delete_file, save_file, validate_image
from app.shared.utils.cache import cache
# from sqlalchemy.orm import Session

async def get_current_total_data_count(db: StandardDatabase):
    """Get current total data count from VA records collection"""
    try:
        query = f"RETURN LENGTH(FOR doc IN {db_collections.VA_TABLE} RETURN 1)"
        result = db.aql.execute(query=query, cache=False).next()
        return result if result is not None else 0
    except Exception as e:
        print(f"Error getting total data count: {e}")
        return 0

def _sync_user_name(current_user) -> str:
    """Best-effort display name for the history row.

    get_current_user returns a dict on some paths and a model on others, so
    both are handled rather than assuming one.
    """
    if isinstance(current_user, dict):
        return current_user.get("name") or current_user.get("email") or "System"
    return (
        getattr(current_user, "name", None)
        or getattr(current_user, "email", None)
        or "System"
    )


async def _record_upload_history(
    db: StandardDatabase,
    records_added: int,
    rows_in_file: int,
    user_name: str,
    duration_seconds: float,
    status_text: str,
):
    """Add a file upload to the shared synchronization history.

    Reuses the ODK sync helper so uploads and API syncs land in one collection
    with the same shape; `method="csv"` is what the history table renders as
    "File Upload". `records_added` is what actually landed, not the row count
    of the file - re-uploading a file whose records already exist adds nothing,
    and the history should say so. Never allowed to break the upload itself:
    the helper swallows its own errors, and this guards the import as well.
    """
    try:
        from app.tasks.odk_tasks import _save_sync_history

        await _save_sync_history(
            db,
            records_synced=records_added,
            total_records=rows_in_file,
            user_name=user_name,
            duration_seconds=duration_seconds,
            method="csv",
            status=status_text,
        )
    except Exception as exc:
        print(f"Failed to record upload history: {exc}")


async def update_csv_sync_status(db: StandardDatabase, uploaded_records_count: int):
    """Update sync status after CSV upload"""
    try:
        # Ensure the system_configs collection exists
        if not db.has_collection(db_collections.SYSTEM_CONFIGS):
            db.create_collection(db_collections.SYSTEM_CONFIGS)
            print(f"Created {db_collections.SYSTEM_CONFIGS} collection")
        
        current_date = datetime.now().isoformat()
        current_total_data = await get_current_total_data_count(db)
        
        sync_status = SyncStatus(
            last_sync_date=current_date,
            last_sync_data_count=uploaded_records_count,
            total_synced_data=current_total_data
        )
        
        config_data = SettingsConfigData(
            type="sync_status",
            sync_status=sync_status
        )
        
        result = await add_configs_settings(config_data, db)
        print(f"CSV sync status updated: {current_date}, uploaded: {uploaded_records_count}, total: {current_total_data}")
        return result
        
    except Exception as e:
        print(f"Error updating CSV sync status: {e}")
        return None


settings_router = APIRouter(
    prefix="/settings",
    tags=["Settings"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]

)



     
@settings_router.get("/system_configs", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# @cache(namespace='system_configs',expire=1000)
async def get_configs_settings(
    db: StandardDatabase = Depends(get_arangodb_session)):
    # Note: Removed print statement to prevent log spam
    try:
        response = await fetch_configs_settings(db=db)
        return response
    except Exception as e:
        print(f"Error fetching system configs: {e}")
        # Return a default response instead of failing completely
        return ResponseMainModel(
            data={},
            message="Failed to fetch system configs, returning defaults"
        )


@settings_router.get("/version", status_code=status.HTTP_200_OK)
async def get_version():
    return '3.5.0'

@settings_router.post("/system_configs", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def save_configs_settings(
    configData: SettingsConfigData,
    current_user = Depends(get_current_user), 
    required_privs: List[str] = Depends(check_privileges(
        [
            AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS, 
            AccessPrivileges.SETTINGS_CREATE_ODK_DETAILS,
            AccessPrivileges.SETTINGS_CREATE_FIELD_MAPPING,
            AccessPrivileges.SETTINGS_CREATE_VA_SUMMARY,
            AccessPrivileges.USERS_UPDATE_ACCESS_LIMIT_LABELS
        ])),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        response = await add_configs_settings( configData, db=db)
        return response
    except HTTPException:
        # Preserve validation errors (e.g. BadRequestException, 400) raised by
        # add_configs_settings - the blanket handler below used to catch these
        # too and flatten them into a 500, losing both the status code and
        # the specific "which fields are missing" message.
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@settings_router.get("/questioner_fields", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
#@cache(namespace='questioner_fields_get',expire=6000)
async def get_questioner_fileds(
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await get_questioners_fields( db=db)
    return response


@settings_router.get("/get-field-unique-value", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# @cache(namespace='get_field_unique_value',expire=6000)
async def get_field_unique_value(
    field: Optional[str] = Query(None, alias="field"),
    parent_field: Optional[str] = Query(None, alias="parent_field"),
    parent_value: Optional[str] = Query(None, alias="parent_value"),
    scope_to_user: bool = Query(True, alias="scope_to_user"),
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await get_field_value_from_va_records(field=field, db=db, parent_field=parent_field, parent_value=parent_value, current_user=current_user, scope_to_user=scope_to_user)
    return response

VALID_IMAGE_EXTENSIONS = ['jpg', 'jpeg', 'png', 'ico', 'svg', 'gif', 'webp']

# (max file size in bytes, min width px, min height px) per image - a
# favicon is tiny by nature, while a logo or login background being under
# these floors is almost certainly a corrupted or placeholder upload.
IMAGE_VALIDATION_RULES = {
    "favicon": (512 * 1024, 16, 16),
    "logo": (5 * 1024 * 1024, 64, 64),
    "home_image": (5 * 1024 * 1024, 200, 200),
}

# Maps the DB/model field name to the multipart form field name - the
# upload form field is "login_image" (matching the frontend's <input> name)
# while everywhere else (the DB record, ImagesConfigData) calls it
# "home_image".
IMAGE_TYPE_FORM_FIELD = {"logo": "logo", "favicon": "favicon", "home_image": "login_image"}


@settings_router.post("/system_images/")
async def upload_image(
    logo: UploadFile = File(None),
    login_image: UploadFile = File(None),
    favicon: UploadFile = File(None),
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_UPDATE_SYSTEM_IMAGES])),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        uploads = {"logo": logo, "home_image": login_image, "favicon": favicon}

        for image_type, upload in uploads.items():
            if not upload:
                continue
            max_size, min_width, min_height = IMAGE_VALIDATION_RULES[image_type]
            try:
                validate_image(upload, max_size_bytes=max_size, min_width=min_width, min_height=min_height)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"{IMAGE_TYPE_FORM_FIELD[image_type]}: {e}")

        existing_images_record = await get_system_images(db)

        existing_images = existing_images_record[0] if len(existing_images_record) >  0 and existing_images_record[0] is not None else {}

        saved_paths = {}
        for image_type, upload in uploads.items():
            if not upload:
                continue
            saved_paths[image_type] = save_file(
                upload,
                valid_file_extensions=VALID_IMAGE_EXTENSIONS,
                delete_extisting=existing_images.get(image_type),
            )

        data = ImagesConfigData(
            logo=saved_paths.get("logo"),
            favicon=saved_paths.get("favicon"),
            home_image=saved_paths.get("home_image"),
        )
        results = await save_system_images(data = data, db = db)

        return ResponseMainModel(
            data = results,
            message="System images uploaded successfully"
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@settings_router.get("/system_images/", description="Get System Images")
#@cache(namespace='system_images_get',expire=6000)
async def get_images(
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:

        return ResponseMainModel(
            data = await get_system_images(db),
            message="System images fetched successfully"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@settings_router.delete("/system_images/", description="Reset all system images to default")
async def delete_system_images(
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_UPDATE_SYSTEM_IMAGES])),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        existing_images_record = await get_system_images(db)


        existing_images = existing_images_record[0] if len(existing_images_record) >  0 and existing_images_record[0] is not None else {}
        if existing_images:
            for image in existing_images.keys():
                try:
                    delete_file(existing_images[image])
                except:
                    print(f"Could not delete {image}")

        data = ImagesConfigData(
            logo=None,
            favicon=None,
            home_image = None
        )
        results = await save_system_images(data = data, reset=True, db = db)

        return ResponseMainModel(
            data = results,
            message="System images reset successfully"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@settings_router.delete("/system_images/{image_type}", description="Reset a single system image to default")
async def delete_single_system_image(
    image_type: str,
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_UPDATE_SYSTEM_IMAGES])),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    if image_type not in IMAGE_VALIDATION_RULES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid image type '{image_type}'. Must be one of: {', '.join(IMAGE_VALIDATION_RULES)}",
        )
    try:
        existing_images_record = await get_system_images(db)
        existing_images = existing_images_record[0] if len(existing_images_record) > 0 and existing_images_record[0] is not None else {}

        if existing_images.get(image_type):
            try:
                delete_file(existing_images[image_type])
            except:
                print(f"Could not delete {image_type}")

        results = await save_system_images(data=ImagesConfigData(), fields_to_reset=[image_type], db=db)

        return ResponseMainModel(
            data=results,
            message=f"{image_type} reset successfully",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@settings_router.post("/upload-csv", status_code=status.HTTP_200_OK)
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    unique_id: Optional[str] = Body ('KEY', alias="unique_id"),
    # oauth = Depends(oauth2_scheme),

    current_user: Optional[str] = Depends(get_current_user),
    start_date: Optional[date] = Body(None, alias="start_date"),
    end_date: Optional[date] = Body(None, alias="end_date"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    db: StandardDatabase = Depends(get_arangodb_session)
):


    # A file upload is a data synchronization like any other, so it belongs in
    # the same history as the ODK API syncs - previously only those were
    # recorded and manual uploads left no trace at all.
    started_at = time.monotonic()
    user_name = _sync_user_name(current_user)

    try:

        # # Generate task ID
        task_id = str(uuid.uuid4())


        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), low_memory=False)

        # ODK Central CSV exports join a submission's nested group path onto
        # each field name with '-' (e.g. "orgunit-region", "meta-instanceID"),
        # the same convention the ODK API sync path resolves via
        # clean_odk_columns on its '/'-joined json_normalize output. Without
        # this, fields land under their full compound name instead of the
        # bare name (e.g. "region", "instanceid") the rest of the app - field
        # mapping config, charts, dashboard filters - actually queries by.
        df.columns = clean_odk_columns(df.columns, sep='-')
        df.columns = df.columns.str.lower()
        df = df.loc[:, ~df.columns.duplicated()]

        # See synthesize_coordinates_from_latlon's own docstring: without
        # this, a CSV's geopoint column(s) land as bare latitude/longitude
        # fields instead of the single "coordinates" array the Data Map
        # reads, so uploaded records never show up on it.
        df = synthesize_coordinates_from_latlon(df)

        unique_id = (unique_id or 'KEY').strip().lower()

        if 'instanceid' not in df.columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instance ID (instanceid) not found in the uploaded CSV")
        if unique_id not in df.columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unique ID not found in the uploaded CSV")
       # Add additional fields to the DataFrame
        df['vman_data_source'] = data_sources.CSV_UPLOAD
        df['vman_data_name'] = 't'
        df['__id'] =df[unique_id]

        df['version_number'] = '1.0'
        df['trackid'] = task_id

        # Convert DataFrame back to a list of dictionaries
        recordsDF = df.to_dict(orient='records')
        rows_in_file = len(recordsDF)

        # insert_all_csv_data inserts with overwrite_mode='ignore' and
        # silent=True, so rows whose __id already exists are dropped without
        # comment and the driver reports nothing per document. Counting either
        # side of the write is the only way to know how many rows actually
        # landed - reporting the file's row count instead made a re-upload of
        # the same file look like a successful import of everything in it,
        # while the record total on screen correctly did not move.
        count_before = await get_current_total_data_count(db)
        await insert_all_csv_data(recordsDF)
        count_after = await get_current_total_data_count(db)

        inserted = max(count_after - count_before, 0)
        skipped = max(rows_in_file - inserted, 0)

        # Update sync status after successful CSV upload
        await update_csv_sync_status(db, inserted)

        # Invalidate regions cache as data has changed
        await invalidate_cache_pattern("unique_regions:*")

        await _record_upload_history(
            db, inserted, rows_in_file, user_name, time.monotonic() - started_at, "completed"
        )

        if inserted == 0:
            message = (
                f"No new records were added - all {rows_in_file} rows already "
                "exist in the database."
            )
        elif skipped:
            message = (
                f"Added {inserted} new record(s); {skipped} row(s) already existed "
                "and were skipped."
            )
        else:
            message = f"Added {inserted} new record(s)."

        return ResponseMainModel(
            data={
                "task_id": task_id,
                "total_records": rows_in_file,
                "inserted": inserted,
                "skipped": skipped,
            },
            message=message,
        )

    except HTTPException:
        # Rejected before any data was written - a validation failure, recorded
        # so the history explains why an expected upload never appeared.
        await _record_upload_history(
            db, 0, 0, user_name, time.monotonic() - started_at, "failed"
        )
        raise
    except Exception as e:
        await _record_upload_history(
            db, 0, 0, user_name, time.monotonic() - started_at, "failed"
        )
        # Raising the error so FastAPI can handle it
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
   


# Add these imports and endpoints to your existing settings_router file


# Add these endpoints to your existing settings_router

# @settings_router.get("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# async def get_cron_settings(
#     current_user = Depends(get_current_user),
#     db: StandardDatabase = Depends(get_arangodb_session)):
#     """Get API cron settings"""
#     try:
#         response = await fetch_cron_settings(db=db)
#         return ResponseMainModel(
#             data=response,
#             message="Cron settings fetched successfully"
#         )
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# @settings_router.post("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# async def save_api_cron_settings(
#     settings: CronSettings,
#     current_user = Depends(get_current_user),
#     required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
#     db: StandardDatabase = Depends(get_arangodb_session)):
#     """Save API cron settings"""
#     try:
#         response = await save_cron_settings(settings, db=db)
#         return ResponseMainModel(
#             data=response,
#             message="Cron settings saved successfully"
#         )
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# @settings_router.get("/backup", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# async def get_backup_settings(
#     current_user = Depends(get_current_user),
#     db: StandardDatabase = Depends(get_arangodb_session)):
#     """Get backup settings"""
#     try:
#         response = await fetch_backup_settings(db=db)
#         return ResponseMainModel(
#             data=response,
#             message="Backup settings fetched successfully"
#         )
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# @settings_router.post("/backup", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# async def save_data_backup_settings(
#     settings: BackupSettings,
#     current_user = Depends(get_current_user),
#     required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
#     db: StandardDatabase = Depends(get_arangodb_session)):
#     """Save backup settings"""
#     try:
#         response = await save_backup_settings(settings, db=db)
#         return ResponseMainModel(
#             data=response,
#             message="Backup settings saved successfully"
#         )
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# Add these imports


# Add these endpoints to your existing settings_router

@settings_router.get("/sync-settings", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# @cache(namespace='sync_settings_get',expire=6000)
async def get_sync_settings(
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Get both cron and backup settings in one API call"""
    try:
        # Query to get the vman_config document with both cron and backup settings
        aql_query = f"""
        FOR settings in {db_collections.SYSTEM_CONFIGS}
        FILTER settings._key == 'vman_config'
        RETURN {{
            cron_settings: settings.cron_settings,
            backup_settings: settings.backup_settings
        }}
        """
        cursor = db.aql.execute(aql_query, bind_vars={}, cache=True)
        settings_data = [doc for doc in cursor]
        
        # If no settings found, return default settings
        if not settings_data or not settings_data[0]:
            settings_data = [{
                "cron_settings": {"days": [], "time": "00:00"},
                "backup_settings": {
                    "frequency": "daily",
                    "time": "00:00",
                    "location": "local"
                }
            }]
        
        return ResponseMainModel(
            data=settings_data[0],
            message="Sync settings fetched successfully"
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@settings_router.get("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# @cache(namespace='cron_settings_get',expire=6000)
async def get_cron_settings(
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Get API cron settings"""
    try:
        # Query to get the vman_config document
        aql_query = f"""
        FOR settings in {db_collections.SYSTEM_CONFIGS}
        FILTER settings._key == 'vman_config'
        RETURN settings.cron_settings
        """
        cursor = db.aql.execute(aql_query, bind_vars={}, cache=True)
        cron_settings = [doc for doc in cursor]
        
        # If no cron settings found, return default settings
        if not cron_settings or not cron_settings[0]:
            cron_settings = [{"days": [], "time": "00:00"}]
        
        return ResponseMainModel(
            data=cron_settings[0],
            message="Cron settings fetched successfully"
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
#@log_to_db(context="save_api_cron_settings", log_args=True)
@settings_router.post("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def save_api_cron_settings(
    settings: CronSettings,
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Save API cron settings.

    Nothing needs to be re-scheduled here - Celery beat's
    check_odk_sync_schedule (app/tasks/odk_tasks.py) re-reads cron_settings
    from the database on every run (once a minute), so a saved change takes
    effect on its own within a minute, with no explicit "apply" step.
    """
    try:
        # A schedule can only ever run an ODK API sync - there's no way to
        # "schedule" a CSV upload - so enabling one (days non-empty) without
        # a working ODK API connection would just fail on every fire, every
        # day. Disabling (empty days) is always allowed regardless.
        if settings.days:
            await ensure_odk_api_ready_for_scheduling(db)

        # Create a SettingsConfigData object with cron_settings
        config_data = SettingsConfigData(
            type='cron_settings',
            cron_settings=settings
        )

        # Save the settings
        response = await add_configs_settings(config_data, db=db)

        return ResponseMainModel(
            data=settings.model_dump(),
            message="Cron settings saved successfully"
        )
    except HTTPException:
        # Preserve the 400 + message from ensure_odk_api_ready_for_scheduling
        # (BadRequestException) as-is - the blanket handler below would
        # otherwise rewrap it as a 500 with a mangled "400: ..." message.
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

#@log_to_db(context="get_backup_settings", log_args=True)
@settings_router.get("/backup", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# @cache(namespace='backup_settings_get',expire=6000)
async def get_backup_settings(
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Get backup settings"""
    try:
        # Query to get the vman_config document
        aql_query = f"""
        FOR settings in {db_collections.SYSTEM_CONFIGS}
        FILTER settings._key == 'vman_config'
        RETURN settings.backup_settings
        """
        cursor = db.aql.execute(aql_query, bind_vars={}, cache=True)
        backup_settings = [doc for doc in cursor]
        
        # If no backup settings found, return default settings
        if not backup_settings or not backup_settings[0]:
            backup_settings = [{
                "frequency": "daily",
                "time": "00:00",
                "location": "local"
            }]
        
        return ResponseMainModel(
            data=backup_settings[0],
            message="Backup settings fetched successfully"
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
#@log_to_db(context="save_data_backup_settings", log_args=True)
@settings_router.post("/backup", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def save_data_backup_settings(
    settings: BackupSettings,
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Save backup settings"""
    try:
        # Create a SettingsConfigData object with backup_settings
        config_data = SettingsConfigData(
            type='backup_settings',
            backup_settings=settings
        )
        
        # Save the settings
        response = await add_configs_settings(config_data, db=db)
        
        return ResponseMainModel(
            data=settings.model_dump(),
            message="Backup settings saved successfully"
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@settings_router.get("/server-time", status_code=status.HTTP_200_OK)
async def get_server_time():
    """Return current server UTC time for client clock synchronization."""
    import time as _time
    now = datetime.utcnow()
    return {
        "epoch_ms": int(_time.time() * 1000),
        "utc": now.strftime("%H:%M:%S"),
        "iso": now.isoformat() + "Z"
    }


# ── VMan ML Model management ───────────────────────────────────────────────────

import json as _json
import shutil as _shutil
from pathlib import Path as _Path

_ML_MODELS_DIR  = _Path(__file__).parent.parent / "ccva" / "ml_models"
_ACTIVE_MODEL   = _ML_MODELS_DIR / "ccva_model_combined.pkl"
_MODEL_REGISTRY = _ML_MODELS_DIR / "model_registry.json"
_MODEL_ARCHIVE  = _ML_MODELS_DIR / "archive"


def _invalidate_this_process_cache() -> None:
    """Clears vman_ml_service's in-memory predictor cache for _ACTIVE_MODEL in
    *this* process, so the next prediction in this API process picks up the
    model file just written, with no restart needed for it specifically.

    Only this process - Celery workers are separate OS processes with their
    own independent cache and are NOT affected by this call. See that
    module's invalidate_predictor_cache() docstring for why, and the admin
    UI's "reload" flow for how workers get a fresh model.
    """
    try:
        from app.ccva.services.vman_ml_service import invalidate_predictor_cache
        invalidate_predictor_cache(_ACTIVE_MODEL)
    except Exception:
        pass  # best-effort - a prediction request will just reload lazily anyway if this fails


def _celery_busy_workers(timeout: float = 3.0) -> list:
    """Worker names currently running at least one task, via Celery's own
    inspect().active() - used to block a model swap while a DQA/sync job
    might be mid-run against the soon-to-be-stale cached predictor.

    Returns [] (not busy) if Celery is unreachable or no worker responds in
    time - if nothing is listening, there's no running process whose cache
    could go stale, so there's nothing this check needs to protect against.
    Only a *confirmed* active task blocks the swap; an inconclusive check
    does not.
    """
    try:
        from app.celery_app import celery_app
        active = celery_app.control.inspect(timeout=timeout).active() or {}
    except Exception:
        return []
    return [worker for worker, tasks in active.items() if tasks]


def _require_celery_idle() -> None:
    busy = _celery_busy_workers()
    if busy:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Celery worker(s) are currently running a task ({', '.join(busy)}) - e.g. a "
                f"DQA or data-sync job. Swapping the model now would leave that job's worker "
                f"process serving a now-stale predictor for the rest of its run. Wait for it "
                f"to finish and try again."
            ),
        )


def _find_existing_version(version: str) -> Optional[str]:
    """Returns where `version` is already recorded (the active model, or
    which archived entry), or None if it isn't used anywhere yet.

    The whole point of a version label is to unambiguously identify one
    specific trained model - two different files sharing one would defeat
    that and risk real confusion about which is actually deployed, so
    uploading a version that already exists is rejected outright rather
    than silently allowed to shadow the original.
    """
    if not _MODEL_REGISTRY.exists():
        return None
    with open(_MODEL_REGISTRY) as f:
        registry = _json.load(f)
    if registry.get("version") == version:
        return "currently the active model"
    for h in registry.get("history", []):
        if h.get("version") == version:
            return f"already archived ({h.get('archive')})"
    return None


def _require_version_not_duplicate(version: str) -> None:
    existing = _find_existing_version(version)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Version '{version}' is {existing}. Retrain with a different "
                   f"--report-version, or delete/rename the existing one first.",
        )


def _broadcast_cache_clear(timeout: float = 3.0) -> None:
    """Tells any running Celery workers to drop their own cached predictor
    (see celery_app.py's clear_vman_ml_cache control command) so they pick up
    the model just swapped in on their next task - no worker restart, and
    only ever called right after _require_celery_idle() confirmed nothing
    was mid-task. Best-effort: a worker that doesn't get this will still
    self-correct whenever it does eventually restart.
    """
    try:
        from app.celery_app import celery_app
        celery_app.control.broadcast('clear_vman_ml_cache', reply=False, timeout=timeout)
    except Exception:
        pass


@settings_router.get("/ml-model", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_ml_model_info(_=Depends(get_current_user)):
    """Return the current model registry (version, metrics, history)."""
    try:
        if _MODEL_REGISTRY.exists():
            with open(_MODEL_REGISTRY) as f:
                registry = _json.load(f)
        else:
            registry = {}
        return ResponseMainModel(data=registry, message="Model info retrieved", error=False)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


_REQUIRED_ARTIFACT_KEYS = ('model', 'scaler', 'label_encoder', 'feature_encoders', 'preprocessor')


def _inspect_pkl_bytes(content: bytes) -> dict:
    """Unpickles an uploaded .pkl's bytes and validates/extracts what the
    registry needs: version, algorithm, and metrics - all read from the
    file's own artifacts['metrics'] / ['model_name'], never hand-typed.

    vman_ml >= v1.2.9 (ModelTrainer.save_model()) embeds the model's own
    headline metrics (accuracy/f1_macro/f1_weighted/auroc_macro/cv_f1_macro/
    n_training_samples/n_test_samples) and model_name directly in the
    artifacts dict, computed from the same hold-out evaluation shown in that
    run's training_summary.md - trusting these means what's recorded can
    never drift from what's actually in the file, and there's no manual
    fallback to keep in sync.

    Unpickling here is no new trust boundary: both endpoints that call this
    already require SETTINGS_CREATE_SYSTEM_CONFIGS, and the same bytes get
    unpickled anyway the first time a worker loads this file for prediction
    (vman_ml_service._get_cached_predictor).

    Returns {'valid': True, 'version', 'algorithm', 'metrics'} on success, or
    {'valid': False, 'error': <message>} - never raises - covering anything
    from a corrupt file to a pre-v1.2.9 model with no embedded metrics.
    """
    try:
        import io as _io
        import joblib as _joblib
        artifacts = _joblib.load(_io.BytesIO(content))
    except Exception as exc:
        return {'valid': False, 'error': f"Not a readable model file: {exc}"}

    if not isinstance(artifacts, dict):
        return {'valid': False, 'error': "File doesn't contain a recognised model bundle."}

    missing = [k for k in _REQUIRED_ARTIFACT_KEYS if k not in artifacts]
    if missing:
        return {'valid': False, 'error': f"Missing required model component(s): {', '.join(missing)}."}

    metrics = artifacts.get('metrics') or {}
    if not metrics:
        return {
            'valid': False,
            'error': "This file has no embedded metrics (requires vman_ml >= v1.2.9). "
                     "Retrain with a newer vman_ml version to upload it here.",
        }

    return {
        'valid':     True,
        'version':   metrics.get('version'),
        'notes':     metrics.get('notes') or '',  # absent on pre-notes-embedding files - not an error
        'algorithm': artifacts.get('model_name') or 'Unknown',
        'metrics':   metrics,
    }


def _archive_active_model(registry: dict, history: list) -> None:
    """Copies the current _ACTIVE_MODEL into _MODEL_ARCHIVE and appends its
    entry to `history` (mutated in place) - shared by upload and restore,
    which both replace whatever is currently active.
    """
    if _ACTIVE_MODEL.exists() and registry.get("version"):
        _MODEL_ARCHIVE.mkdir(parents=True, exist_ok=True)
        old_version  = registry["version"]
        old_date     = registry.get("trained_at", "unknown")
        archive_name = f"ccva_model_v{old_version}_{old_date}.pkl"
        _shutil.copy2(_ACTIVE_MODEL, _MODEL_ARCHIVE / archive_name)
        history.append({
            "version":     old_version,
            "trained_at":  old_date,
            "algorithm":   registry.get("algorithm", ""),
            "archive":     f"archive/{archive_name}",
            "replaced_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })


@settings_router.post("/ml-model/validate", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def validate_ml_model(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
):
    """Inspects a .pkl without deploying it - lets the UI show detected
    metrics (or a clear error) right after file selection, before the admin
    commits to uploading. Writes nothing to disk.
    """
    if not (file.filename or "").endswith(".pkl"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Only .pkl model files are accepted.")
    content = await file.read()
    result = _inspect_pkl_bytes(content)
    if not result['valid']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['error'])
    if result['version']:
        _require_version_not_duplicate(result['version'])
    return ResponseMainModel(data=result, message="File is valid", error=False)


@settings_router.post("/ml-model/upload", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def upload_ml_model(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
):
    """Upload a new .pkl model file and update the model registry.

    Every field - version, notes, metrics - is always read from the file
    itself (see _inspect_pkl_bytes). There's no manual-entry fallback: the
    person uploading a model has no basis to describe a training run they
    didn't do, so a file without embedded metadata is rejected outright
    rather than deployed with blanks or guessed values.
    """
    if not (file.filename or "").endswith(".pkl"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Only .pkl model files are accepted.")

    content = await file.read()
    result = _inspect_pkl_bytes(content)
    if not result['valid']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result['error'])
    if not result['version']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'version' is missing from the uploaded .pkl's embedded metadata.",
        )
    _require_version_not_duplicate(result['version'])

    _require_celery_idle()  # raises 409 if any worker is mid-task - checked before any write

    # Read old registry before overwriting
    old_registry: dict = {}
    if _MODEL_REGISTRY.exists():
        with open(_MODEL_REGISTRY) as f:
            old_registry = _json.load(f)

    history: list = list(old_registry.get("history", []))
    _archive_active_model(old_registry, history)

    with open(_ACTIVE_MODEL, "wb") as f:
        f.write(content)
    _invalidate_this_process_cache()
    _broadcast_cache_clear()

    metrics = result['metrics']
    new_registry = {
        "active":               "ccva_model_combined.pkl",
        "version":              result['version'],
        "trained_at":           datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_file":          file.filename,
        "algorithm":            result['algorithm'],
        "notes":                result['notes'],
        "metrics": {
            "accuracy":           round(metrics["accuracy"], 4),
            "f1_macro":           round(metrics["f1_macro"], 4),
            "f1_weighted":        round(metrics["f1_weighted"], 4),
            "n_training_samples": metrics["n_training_samples"],
            "n_test_samples":     metrics["n_test_samples"],
            **({"cv_f1_macro": round(metrics["cv_f1_macro"], 4)} if metrics.get("cv_f1_macro") is not None else {}),
        },
        "metrics_auto_detected": True,
        "history":              history,
    }
    with open(_MODEL_REGISTRY, "w") as f:
        _json.dump(new_registry, f, indent=2)

    return ResponseMainModel(data=new_registry, message="Model updated successfully", error=False)


@settings_router.post("/ml-model/restore", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def restore_ml_model(
    payload: dict,
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
):
    """Re-deploys a previously-archived model version as the active one.

    The currently active model is archived first (same as a fresh upload
    replacing it), then the chosen archive's bytes become the new active
    model, with its version/algorithm/metrics re-read directly from that
    file - never from the possibly-stale history entry - so a restore can
    never show different numbers than the file actually deployed.
    """
    archive_rel = (payload or {}).get("archive")
    if not archive_rel:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'archive' is required.")

    old_registry: dict = {}
    if _MODEL_REGISTRY.exists():
        with open(_MODEL_REGISTRY) as f:
            old_registry = _json.load(f)
    history: list = list(old_registry.get("history", []))

    matches = [h for h in history if h.get("archive") == archive_rel]
    if not matches:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="That version isn't in the history list.")

    # Path confined to _MODEL_ARCHIVE regardless of what the (validated-above)
    # archive_rel string contains - no path traversal outside this directory.
    archive_path = _MODEL_ARCHIVE / _Path(archive_rel).name
    if not archive_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Archived file not found: {archive_path.name}")

    with open(archive_path, "rb") as f:
        content = f.read()
    result = _inspect_pkl_bytes(content)
    if not result['valid']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Archived file is no longer valid: {result['error']}",
        )

    _require_celery_idle()  # raises 409 if any worker is mid-task - checked before any write

    _archive_active_model(old_registry, history)
    history = [h for h in history if h.get("archive") != archive_rel]  # it's active now, not history

    with open(_ACTIVE_MODEL, "wb") as f:
        f.write(content)
    _invalidate_this_process_cache()
    _broadcast_cache_clear()

    metrics = result['metrics']
    new_registry = {
        "active":               "ccva_model_combined.pkl",
        "version":              result['version'] or matches[0].get('version', 'unknown'),
        "trained_at":           matches[0].get('trained_at', datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        "source_file":          archive_path.name,
        "algorithm":            result['algorithm'],
        # The file's own training notes, same as a fresh upload - plus an
        # auto-generated (not hand-typed) marker that this came from a restore.
        "notes":                (result['notes'] + ' ' if result['notes'] else '') + f"(reloaded from {archive_rel})",
        "metrics": {
            "accuracy":           round(metrics["accuracy"], 4),
            "f1_macro":           round(metrics["f1_macro"], 4),
            "f1_weighted":        round(metrics["f1_weighted"], 4),
            "n_training_samples": metrics["n_training_samples"],
            "n_test_samples":     metrics["n_test_samples"],
            **({"cv_f1_macro": round(metrics["cv_f1_macro"], 4)} if metrics.get("cv_f1_macro") is not None else {}),
        },
        "metrics_auto_detected": True,
        "history":              history,
    }
    with open(_MODEL_REGISTRY, "w") as f:
        _json.dump(new_registry, f, indent=2)

    return ResponseMainModel(data=new_registry, message="Model restored successfully", error=False)


@settings_router.post("/ml-model/delete-version", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def delete_ml_model_version(
    payload: dict,
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
):
    """Permanently deletes one archived (non-active) model version.

    Only ever operates on entries in `history` - the currently active model
    is never stored there (see upload/restore above), so there's no code
    path here that can delete the deployed model.
    """
    archive_rel = (payload or {}).get("archive")
    if not archive_rel:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="'archive' is required.")

    with open(_MODEL_REGISTRY) as f:
        registry = _json.load(f)
    history: list = list(registry.get("history", []))

    if not any(h.get("archive") == archive_rel for h in history):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="That version isn't in the history list.")

    archive_path = _MODEL_ARCHIVE / _Path(archive_rel).name
    if archive_path.exists():
        archive_path.unlink()

    registry["history"] = [h for h in history if h.get("archive") != archive_rel]
    with open(_MODEL_REGISTRY, "w") as f:
        _json.dump(registry, f, indent=2)

    return ResponseMainModel(data=registry, message="Version deleted", error=False)

@settings_router.post("/xform/upload", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def upload_xform_dictionary(
    file: UploadFile = File(...),
    override_labels: bool = Form(False),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Enrich the VA question dictionary from an uploaded XLSForm (xForm).

    Non-destructive: questions are matched to existing records by name and a
    field is only written when the stored value is blank. Names absent from the
    dictionary are reported back, never inserted - ODK sync stays the source of
    truth for which questions exist.
    """
    if not (file.filename or "").lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="An xForm must be an XLSForm workbook (.xlsx or .xls).",
        )

    content = await file.read()
    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The uploaded file is empty.",
        )

    response = await enrich_questions_from_xform(content, file.filename, db, override_labels)

    # Optional: only present when the workbook's `choices` sheet carries an
    # `expected_deaths` column - skipped silently otherwise.
    expected_deaths_summary = await import_expected_deaths_from_xform(content, db)
    if expected_deaths_summary is not None:
        response.data = {**(response.data or {}), "expected_deaths": expected_deaths_summary}

    return response


@settings_router.get("/dictionary", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_dictionary(
    include_options: bool = Query(
        False, description="Also return each question's choice list, with per-language answer text"
    ),
    current_user=Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Return the stored VA data dictionary (read-only, does not contact ODK)."""
    return await get_data_dictionary(db, include_options=include_options)


@settings_router.patch("/dictionary/{name}/label", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def patch_dictionary_label(
    name: str,
    language: str = Body(..., embed=True),
    label: str = Body(..., embed=True),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Edit one question's label for one language, from the dictionary table."""
    return await update_question_label(name, language, label, db)


@settings_router.get("/expected-deaths", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_expected_deaths(
    current_user=Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """The admin-unit hierarchy and expected deaths, imported from an xForm's
    `choices` sheet. Empty (`configured: false`) until one has been uploaded
    with an `expected_deaths` column."""
    return await get_expected_deaths_tree(db)


@settings_router.patch("/expected-deaths/{key}", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def patch_expected_deaths(
    key: str,
    period: str = Body(..., embed=True),
    expected_deaths: int = Body(..., embed=True),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Edit one administrative unit's expected deaths for one period. Only a
    unit with no children can be edited directly - totals above it are
    recomputed automatically."""
    return await update_expected_deaths_value(key, period, expected_deaths, db)


@settings_router.delete("/expected-deaths/{key}", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def delete_expected_deaths(
    key: str,
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Delete an administrative unit and its whole subtree. Re-uploading the
    same xForm afterwards re-creates it (and anything else missing) fresh -
    deletion here only ever removes from storage, never from the file."""
    return await delete_expected_deaths_node(key, db)


# ── Danger zone: deleting VA data ────────────────────────────────────────────

@settings_router.get("/reset-data/preview", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def preview_reset_data(
    sources: str = Query(..., description="Comma-separated: odk_api, uploaded_csv"),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([
        AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS,
        AccessPrivileges.ODK_DATA_SYNC,
    ])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Report exactly what a reset would delete, without deleting anything."""
    return await preview_reset([s.strip() for s in sources.split(",") if s.strip()], db)


@settings_router.post("/reset-data", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def reset_data(
    sources: List[str] = Body(..., embed=True),
    confirm: str = Body(..., embed=True),
    current_user=Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([
        AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS,
        AccessPrivileges.ODK_DATA_SYNC,
    ])),
    db: StandardDatabase = Depends(get_arangodb_session),
):
    """Delete VA records of the given sources, and everything derived from them.

    The typed confirmation is checked here as well as in the dialog: the UI can
    be bypassed, and this endpoint destroys data irreversibly.
    """
    if (confirm or "").strip() != "DELETE":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Type DELETE to confirm this action.",
        )
    return await reset_va_data(sources, db)
