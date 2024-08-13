

from functools import wraps

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.shared.configs.arangodb_db import get_arangodb_session
from app.shared.configs.security import get_token_user
from app.users.schemas.user import RegisterUserRequest

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_arangodb_session)):
 
    user = await get_token_user(token=token, db=db)
    # print(user)
    if user is  not None:
        return user
    raise HTTPException(status_code=401, detail="Not authorised..")    


def created_by_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        current_user = await get_current_user()  # Manually call the dependency function
        print(current_user)
        if 'data' in kwargs and isinstance(kwargs['data'], RegisterUserRequest):
            kwargs['data'].created_by = current_user.id
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'data' in request")
        return await func(*args, **kwargs)
    return wrapper