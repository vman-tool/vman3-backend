from app.odk_download import route
from app.users import users_router


def main_route(application):
    application.include_router(route.odk_router)  
    application.include_router(users_router.user_router)
    application.include_router(users_router.guest_router)
    application.include_router(users_router.auth_router) 