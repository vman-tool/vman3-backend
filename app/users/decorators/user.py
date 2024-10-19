

from functools import wraps
from typing import List

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from arango.database import StandardDatabase

from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_token_user
from app.users.models.user import User
from app.users.responses.user import UserRolesResponse
from app.users.schemas.user import RegisterUserRequest
from app.users.services.user import get_user_roles

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_arangodb_session)):
    user = await get_token_user(token=token, db=db)
    if user is  not None:
        return user
    raise HTTPException(status_code=401, detail="Not authorised..")    


def created_by_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        current_user = await get_current_user()  # Manually call the dependency function
        if 'data' in kwargs and isinstance(kwargs['data'], RegisterUserRequest):
            kwargs['data'].created_by = current_user.id
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'data' in request")
        return await func(*args, **kwargs)
    return wrapper

async def get_current_user_privileges(current_user: User =  Depends(get_current_user), db: StandardDatabase = Depends(get_arangodb_session)):
    user_roles_data: ResponseMainModel = await get_user_roles(current_user = current_user, db=db)
    user_roles_data: UserRolesResponse = user_roles_data.data
    privileges = []
    if type(user_roles_data) is list:
        return privileges
    for role in user_roles_data.roles:
        privileges.extend(role.get("privileges", ""))
    return privileges

def check_privileges(required_privileges: List[str]):
    def dependency(user_privileges: str = Depends(get_current_user_privileges)):
        missing_privileges = [priv for priv in required_privileges if priv not in user_privileges]
        if missing_privileges:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"You do not have permission to perform this action. Missing privileges: {', '.join(missing_privileges)}"
            )
    return dependency