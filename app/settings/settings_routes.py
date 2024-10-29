from typing import List, Optional
from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status

from app.settings.models.settings import ImagesConfigData, SettingsConfigData
from app.settings.services.odk_configs import (add_configs_settings,
                                               fetch_configs_settings,
                                               get_questioners_fields, get_system_images, save_system_images)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.shared.services.va_records import get_field_value_from_va_records
from app.shared.configs.constants import AccessPrivileges, Special_Constants
from app.users.decorators.user import check_privileges, get_current_user
from app.utilits.helpers import delete_file, save_file

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




