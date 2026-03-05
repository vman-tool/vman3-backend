

from functools import wraps
from typing import List

from fastapi import Depends, HTTPException, WebSocket, status, Query
from fastapi.security import OAuth2PasswordBearer
from arango.database import StandardDatabase

from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_token_user
from app.users.models.user import User
from app.users.responses.user import UserRolesResponse
from app.users.schemas.user import RegisterUserRequest
from app.users.schemas.user import RegisterUserRequest
from app.users.services.user import get_user_roles
from app.shared.configs.security import get_token_payload, settings, load_user
from app.users.models.user import User # Ensure User is imported for type hinting if needed, though load_user returns dict/obj

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_arangodb_session)):
    user = await get_token_user(token=token, db=db)
    if user is  not None:
        return user
    raise HTTPException(status_code=401, detail="Not authorised..")

async def get_current_user_ws(websocket: WebSocket, db = Depends(get_arangodb_session)):
    try:
        
        token = websocket.query_params.get("token")
        if not token:
            token = websocket.headers.get("sec-websocket-protocol")
            if token and token.startswith("Bearer "):
                token = token.split(" ")[1]

        if not token:
            raise HTTPException(status_code=401, detail="Not authorized")
        

        user = await get_current_user(token, db=db)
        
        return user

    except Exception as e:
        raise HTTPException(status_code=401, detail="Failed to authorize")


async def get_export_user(
    token: str = Query(..., alias="token"),
    db = Depends(get_arangodb_session)
):
    """
    Validates a short-lived export token from query parameter.
    """
    try:
        # Decode token
        payload = get_token_payload(token, settings.JWT_SECRET, settings.JWT_ALGORITHM)
        
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid token")
            
        # Verify type is export_token
        if payload.get("type") != "export_token":
            raise HTTPException(status_code=401, detail="Invalid token type")
            
        # Get user
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token subject")
            
        # Find user by ID (using a direct query since load_user uses email)
        # Or we can reuse get_current_user logic if we had user_id lookup
        # For now, let's fetch the user directly
        user_cursor = await User.get(user_id, db=db)
        
        if not user_cursor:
             raise HTTPException(status_code=401, detail="User not found")
             
        return user_cursor

    except Exception as e:
        print(f"Export token validation error: {e}")
        raise HTTPException(status_code=401, detail="Invalid or expired export token")


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
    response = await get_user_roles(current_user = current_user, db=db)
    
    # Handle cached response (dict) vs fresh response (Pydantic model)
    if isinstance(response, dict):
        data = response.get("data")
    else:
        data = getattr(response, "data", None)

    if not data:
        return []

    # Check for list (empty case)
    if isinstance(data, list):
         return []

    # Extract roles
    if isinstance(data, dict):
        roles = data.get("roles", [])
    else:
        roles = getattr(data, "roles", [])

    privileges = []
    for role in roles:
        # Extract privileges from role
        if isinstance(role, dict):
            privs = role.get("privileges", [])
        else:
            privs = getattr(role, "privileges", [])
            
        privileges.extend(privs)
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