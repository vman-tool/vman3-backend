
from fastapi import HTTPException


class CustomHTTPException(HTTPException):
    def __init__(self, status_code: int,detail: str, error_code: str = None, extra_info: dict = None):
        self.error_code = error_code
        self.extra_info = extra_info
        super().__init__(status_code=status_code, detail=detail)

    def to_dict(self):
        error_response = {
            "status_code": self.status_code,
            "message": self.detail,
            "error": self.extra_info,
            "data": None,
            "pager": None,
            "total": 0,
            
        }
        if self.error_code:
            error_response["error_code"] = self.error_code
        if self.extra_info:
            error_response["extra_info"] = self.extra_info
        return error_response

class ItemNotFoundException(CustomHTTPException):
    def __init__(self, item_id: str):
        super().__init__(
            status_code=404,
            detail=f"Item with ID {item_id} not found.",
            error_code="ITEM_NOT_FOUND"
        )

class UnauthorizedException(CustomHTTPException):
    def __init__(self):
        super().__init__(
            status_code=401,
            detail="You are not authorized to perform this action.",
            error_code="UNAUTHORIZED_ACCESS"
        )

class BadRequestException(CustomHTTPException):
    def __init__(self, detail: str = "Bad request.", extra_info: dict = None):
        super().__init__(
            status_code=400,
            detail=detail,
            error_code="BAD_REQUEST",
            extra_info=extra_info
        )

class ForbiddenException(CustomHTTPException):
    def __init__(self):
        super().__init__(
            status_code=403,
            detail="You do not have permission to access this resource.",
            error_code="FORBIDDEN_ACCESS",
            path = self.request.url.path
        )