import io
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
    HTTPException,
    Query,
    UploadFile,
    status,
)

from app.ccva.services.ccva_upload import insert_all_csv_data
from app.settings.models.settings import ImagesConfigData, SettingsConfigData
from app.settings.services.cron import BackupSettings, CronSettings, fetch_backup_settings, fetch_cron_settings, save_backup_settings, save_cron_settings
from app.settings.services.odk_configs import (
    add_configs_settings,
    fetch_configs_settings,
    get_questioners_fields,
    get_system_images,
    save_system_images,
)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import AccessPrivileges, db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.services.va_records import get_field_value_from_va_records
from app.users.decorators.user import check_privileges, get_current_user
from app.utilits.db_logger import db_logger, log_to_db
from app.utilits.helpers import delete_file, save_file
from app.utilits.schedeular import schedule_odk_fetch_job

# from sqlalchemy.orm import Session


settings_router = APIRouter(
    prefix="/settings",
    tags=["Settings"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]

)



     
@settings_router.get("/system_configs", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_configs_settings(
    db: StandardDatabase = Depends(get_arangodb_session)):
    print("Fetching system configs")

    response = await fetch_configs_settings( db=db)
    return response


@settings_router.get("/version", status_code=status.HTTP_200_OK)
async def get_version():
    return '3.1.0'

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
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@settings_router.get("/questioner_fields", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_questioner_fileds(
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await get_questioners_fields( db=db)
    return response


@settings_router.get("/get-field-unique-value", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_field_unique_value(
    field: Optional[str] = Query(None, alias="field"),
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await get_field_value_from_va_records(field=field, db=db)
    return response

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
        valid_image_extensions = ['jpg', 'jpeg', 'png', 'ico', 'svg', 'gif', 'webp']
        logo_saved_path = None
        login_image_saved_path = None
        favicon_saved_path = None

        existing_images_record = await get_system_images(db)

        existing_images = existing_images_record[0] if len(existing_images_record) >  0 and existing_images_record[0] is not None else {}

        if logo:
            logo_saved_path = save_file(logo, valid_file_extensions=valid_image_extensions, delete_extisting=existing_images["logo"] if "logo" in existing_images else None)
        
        if login_image:
            login_image_saved_path = save_file(login_image, valid_file_extensions=valid_image_extensions, delete_extisting=existing_images["home_image"] if "home_image" in existing_images else None)
        
        if favicon:
            favicon_saved_path = save_file(favicon, valid_file_extensions=valid_image_extensions, delete_extisting=existing_images["favicon"] if "favicon" in existing_images else None)

        data = ImagesConfigData(
            logo=logo_saved_path,
            favicon=favicon_saved_path,
            home_image = login_image_saved_path
        )
        results = await save_system_images(data = data, db = db)

        return ResponseMainModel(
            data = results,
            message="System images uploaded successfully"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@settings_router.get("/system_images/", description="Get System Images")
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

@settings_router.delete("/system_images/", description="Get System Images")
async def delete_system_images(
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


    try:

        # # Generate task ID
        task_id = str(uuid.uuid4())
        

        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), low_memory=False)


        if 'instanceID' in df.columns:
            df['instanceid'] = df['instanceID']
            df.drop(columns=['instanceID'], inplace=True)
        if 'instanceid' not in df.columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instance ID (instanceid) not found in the uploaded CSV")
        if unique_id not in df.columns:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unique ID not found in the uploaded CSV")
       # Add additional fields to the DataFrame
        df['vman_data_source'] = 'uploaded_csv'
        df['vman_data_name'] = 't'
        df['__id'] =df[unique_id]

         
        
        df['version_number'] = '1.0'
        df['trackid'] = task_id
        df.columns = map(str.lower, df.columns)

        # Convert DataFrame back to a list of dictionaries
        recordsDF = df.to_dict(orient='records')
        print('before insert_all_csv_data', len(recordsDF))
        
        await insert_all_csv_data(recordsDF)
        print('after insert_all_csv_data')

        return ResponseMainModel(data={"task_id": task_id, "total_records": 0,}, message="CCVA is running with uploaded CSV data...")

    except Exception as e:
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

@settings_router.get("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
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
    
@log_to_db(context="save_api_cron_settings", log_args=True)
@settings_router.post("/cron", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def save_api_cron_settings(
     background_tasks: BackgroundTasks,
    settings: CronSettings,
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.SETTINGS_CREATE_SYSTEM_CONFIGS])),
    db: StandardDatabase = Depends(get_arangodb_session)):
    """Save API cron settings"""
    try:
        # Create a SettingsConfigData object with cron_settings
        config_data = SettingsConfigData(
            type='cron_settings',
            cron_settings=settings
        )
        
        # Save the settings
        response = await add_configs_settings(config_data, db=db)
        background_tasks.add_task(schedule_odk_fetch_job, db)
        print("Scheduled ODK fetch job executed successfully")
        
        
        return ResponseMainModel(
            data=settings.model_dump(),
            message="Cron settings saved successfully"
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
@log_to_db(context="get_backup_settings", log_args=True)
@settings_router.get("/backup", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
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
@log_to_db(context="save_data_backup_settings", log_args=True)
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