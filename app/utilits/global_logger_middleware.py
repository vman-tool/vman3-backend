# from fastapi import FastAPI, Request, HTTPException
# from fastapi.responses import JSONResponse
# from fastapi.exceptions import RequestValidationError
# from starlette.exceptions import HTTPException as StarletteHTTPException
# from starlette.middleware.base import BaseHTTPMiddleware
# import traceback
# from typing import Callable, Awaitable

# # Assuming you have these already set up
# from app.utilits.db_logger import DBLogger, LogLevel
# from app.utilits.logger import app_logger

# class GlobalErrorMiddleware(BaseHTTPMiddleware):
#     def __init__(self, app: FastAPI):
#         super().__init__(app)
#         self.db_logger = DBLogger()
        
#     async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable]) -> JSONResponse:
#         try:
#             response = await call_next(request)
#             return response
            
#         except RequestValidationError as exc:
#             # Handle validation errors (422)
#             error_detail = {
#                 "message": "Validation error",
#                 "errors": exc.errors(),
#                 "error": True
#             }
            
#             # Log to database
#             await self.log_error(
#                 request=request,
#                 exc=exc,
#                 status_code=422,
#                 level=LogLevel.ERROR,
#                 context="request_validation"
#             )
            
#             return JSONResponse(
#                 status_code=422,
#                 content=error_detail
#             )
            
#         except StarletteHTTPException as exc:
#             # Handle HTTP exceptions (404, etc.)
#             error_detail = {
#                 "message": exc.detail,
#                 "error": True
#             }
            
#             # Log to database
#             await self.log_error(
#                 request=request,
#                 exc=exc,
#                 status_code=exc.status_code,
#                 level=LogLevel.WARNING if exc.status_code < 500 else LogLevel.ERROR,
#                 context="http_exception"
#             )
            
#             return JSONResponse(
#                 status_code=exc.status_code,
#                 content=error_detail
#             )
            
#         except Exception as exc:
#             # Handle all other exceptions (500)
#             error_detail = {
#                 "message": "Internal server error",
#                 "error": True
#             }
            
#             # Log to database
#             await self.log_error(
#                 request=request,
#                 exc=exc,
#                 status_code=500,
#                 level=LogLevel.ERROR,
#                 context="unhandled_exception"
#             )
            
#             return JSONResponse(
#                 status_code=500,
#                 content=error_detail
#             )
    
#     async def log_error(self, request: Request, exc: Exception, status_code: int, level: str, context: str):
#         """Helper method to log errors to the database"""
#         try:
#             # Get user ID if available (modify based on your auth system)
#             user_id = None
#             if hasattr(request.state, "user"):
#                 user_id = getattr(request.state.user, "id", None)
            
#             # Prepare request data (be careful with sensitive info)
#             request_data = {
#                 "method": request.method,
#                 "url": str(request.url),
#                 "headers": dict(request.headers),
#                 "path_params": dict(request.path_params),
#                 "query_params": dict(request.query_params),
#                 "status_code": status_code
#             }
            
#             # Don't log body for security reasons, or sanitize it first
            
#             # Log to database
#             await self.db_logger.log(
#                 message=f"{exc.__class__.__name__}: {str(exc)}",
#                 level=level,
#                 context=context,
#                 module="error_middleware",
#                 exception=exc,
#                 data={
#                     "request": request_data,
#                     "error_type": exc.__class__.__name__
#                 },
#                 user_id=user_id,
#                 immediate=True  # Important errors should be logged immediately
#             )
            
#         except Exception as log_exc:
#             # If DB logging fails, fall back to regular logger
#             app_logger.error(
#                 f"Failed to log error to database: {str(log_exc)}",
#                 exc_info=True
#             )
#             app_logger.error(
#                 f"Original error: {exc.__class__.__name__}: {str(exc)}",
#                 exc_info=True
#             )
