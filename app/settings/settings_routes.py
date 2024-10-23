from typing import List, Optional
from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status

from app.settings.models.settings import SettingsConfigData
from app.settings.services.odk_configs import (add_configs_settings,
                                               fetch_configs_settings,
                                               get_questioners_fields)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.shared.services.va_records import get_field_value_from_va_records
from app.shared.configs.constants import AccessPrivileges, Special_Constants
from app.users.decorators.user import check_privileges, get_current_user
from app.utilits.helpers import save_file

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

    response = await fetch_configs_settings( db=db)
    return response

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
    response = await add_configs_settings( configData, db=db)
    return response


@settings_router.get("/system_configs", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_download_status(

    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await fetch_configs_settings( db=db)
    return response



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
):
    try:
        valid_image_extensions = ['jpg', 'jpeg', 'png', 'ico', 'svg', 'gif', 'webp']
        logo_saved_path = None
        login_image_saved_path = None
        favicon_saved_path = None

        if logo:
            logo_saved_path = save_file(logo, valid_file_extensions=valid_image_extensions)
        
        if login_image:
            login_image_saved_path = save_file(login_image, valid_file_extensions=valid_image_extensions)
        
        if favicon:
            favicon_saved_path = save_file(favicon, valid_file_extensions=valid_image_extensions)
        
        print(logo_saved_path)
        print(login_image_saved_path)
        print(favicon_saved_path)

        return {"message": "Image uploaded successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




