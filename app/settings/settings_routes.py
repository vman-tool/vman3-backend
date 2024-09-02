from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, status

from app.settings.models.settings import SettingsConfigData
from app.settings.services.odk_configs import (
    add_configs_settings,
    fetch_configs_settings,
    get_questioners_fields,
)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel

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
    db: StandardDatabase = Depends(get_arangodb_session)):
    response = await add_configs_settings( configData,db=db)
    return response


@settings_router.get("/system_configs", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_download_status(

    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await fetch_configs_settings( db=db)
    return response



@settings_router.get("/questioner_fields", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_questioner_fileds(

    db: StandardDatabase = Depends(get_arangodb_session)):

    response = await get_questioners_fields( db=db)
    return response


