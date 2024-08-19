from fastapi import APIRouter

from app.odk import odk_routes
from app.pcva import pcva_routes
from app.records import records_routes
from app.settings import settings_routes
from app.statistics import statistics_routes
from app.users import users_routes


def main_route(application):
    application.include_router(create_main_router()) 

def create_main_router():
    main_router = APIRouter(prefix="/vman/api/v1")

    # Add routers to the main_router
    main_router.include_router(odk_routes.odk_router)
    main_router.include_router(users_routes.user_router)
    main_router.include_router(users_routes.guest_router)
    main_router.include_router(users_routes.auth_router)
    main_router.include_router(pcva_routes.pcva_router)
    main_router.include_router(records_routes.data_router)
    main_router.include_router(statistics_routes.statistics_router)
    main_router.include_router(settings_routes.settings_router)

    return main_router