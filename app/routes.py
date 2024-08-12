from app.odk_download import route
from app.pcva import routes
from app.records import records_router
from app.settings import settings_router
from app.statistics import statistics_router
from app.users import users_router


def main_route(application):
    application.include_router(route.odk_router)  
    application.include_router(users_router.user_router)
    application.include_router(users_router.guest_router)
    application.include_router(users_router.auth_router) 
    application.include_router(routes.pcva_router) 
    application.include_router(records_router.data_router) 
    application.include_router(statistics_router.statistics_router) 
    application.include_router(settings_router.settings_router) 