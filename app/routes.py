import os

from fastapi import APIRouter
from fastapi.staticfiles import StaticFiles

from app.ccva import ccva_routes
from app.ccva import ccva_public_routes
from app.ccva_public_module.config import CCVA_PUBLIC_ENABLED
from app.data_quality import data_quality_routes
from app.odk import odk_routes
from app.pcva import pcva_routes
from app.records import records_routes
from app.settings import settings_routes
from app.shared.configs.constants import Special_Constants
from app.statistics import statistics_routes
from app.users import users_routes



def main_route(application):
    application.include_router(create_main_router())
    path= os.getcwd()+"/app"+Special_Constants.UPLOAD_FOLDER
    application.mount(Special_Constants.FILE_URL, StaticFiles(directory=path), name="uploads")

def create_main_router():
    main_router = APIRouter(prefix="/vman/api/v1")

    # Add routers to the main_router
    main_router.include_router(odk_routes.odk_router)
    main_router.include_router(users_routes.user_router)
    main_router.include_router(users_routes.guest_router)
    main_router.include_router(users_routes.auth_router)
    main_router.include_router(pcva_routes.pcva_router)
    main_router.include_router(pcva_routes.pcva_socket_router)
    
    # Conditionally include CCVA Public module (can be disabled for standalone deployment)
    # if CCVA_PUBLIC_ENABLED:
    main_router.include_router(ccva_public_routes.ccva_public_router)
    
    main_router.include_router(ccva_routes.ccva_router)
  
    main_router.include_router(records_routes.data_router)
    main_router.include_router(statistics_routes.statistics_router)
    main_router.include_router(settings_routes.settings_router)
    main_router.include_router(ccva_routes.ccva_router)
    main_router.include_router(data_quality_routes.data_quality_router)
    return main_router
    