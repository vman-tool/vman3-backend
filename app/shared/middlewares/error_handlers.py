from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.shared.middlewares.exceptions import (BadRequestException,
                                               CustomHTTPException,
                                               ForbiddenException,
                                               ItemNotFoundException,
                                               UnauthorizedException)


async def custom_http_exception_handler(request: Request, exc: CustomHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )

async def item_not_found_exception_handler(request: Request, exc: ItemNotFoundException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )

async def unauthorized_exception_handler(request: Request, exc: UnauthorizedException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )

async def bad_request_exception_handler(request: Request, exc: BadRequestException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )

async def forbidden_exception_handler(request: Request, exc: ForbiddenException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict()
    )

async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
                "type": "InternalServerError",
                "message": "An unexpected error occurred.",
                "error": str(exc),
               
            },
    )

# Registering all exception handlers as a group
def register_error_handlers(app: FastAPI):
    app.add_exception_handler(CustomHTTPException, custom_http_exception_handler)
    app.add_exception_handler(ItemNotFoundException, item_not_found_exception_handler)
    app.add_exception_handler(UnauthorizedException, unauthorized_exception_handler)
    app.add_exception_handler(BadRequestException, bad_request_exception_handler)
    app.add_exception_handler(ForbiddenException, forbidden_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)