from app.odk_download import route


def main_route(application):
    application.include_router(route.odk_router)   