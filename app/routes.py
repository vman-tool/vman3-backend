from app.odk_download import route
from app.users import users_router
from app.pcva import routes


def main_route(application):
    application.include_router(route.odk_router)  
    application.include_router(users_router.user_router)
    application.include_router(users_router.guest_router)
    application.include_router(users_router.auth_router) 
    application.include_router(routes.pcva_router) 