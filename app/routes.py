from fastapi import APIRouter

from app.odk_download import route
from app.pcva import routes
from app.ccva import ccva_routes
from app.records import records_router
from app.settings import settings_router
from app.statistics import statistics_router
from app.users import users_router


def main_route(application):
    application.include_router(create_main_router()) 

def create_main_router():
    main_router = APIRouter(prefix="/vman/api/v1")

    # Add routers to the main_router
    main_router.include_router(route.odk_router)
    main_router.include_router(users_router.user_router)
    main_router.include_router(users_router.guest_router)
    main_router.include_router(users_router.auth_router)
    main_router.include_router(routes.pcva_router)
    main_router.include_router(records_router.data_router)
    main_router.include_router(statistics_router.statistics_router)
    main_router.include_router(settings_router.settings_router)
    main_router.include_router(ccva_routes.ccva_router)
    

    return main_router