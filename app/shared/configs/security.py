import base64
import logging
from datetime import datetime, timedelta

import jwt
from arango.database import StandardDatabase
from passlib.context import CryptContext

from app.shared.configs.constants import AccessPrivileges, db_collections
from app.shared.configs.settings import get_settings

# from sqlalchemy.orm import Session


SPECIAL_CHARACTERS = ['@', '#', '$', '%', '=', ':', '?', '.', '/', '|', '~', '>']

settings = get_settings()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(password):
    return pwd_context.hash(password)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def is_password_strong_enough(password: str) -> bool:
    if len(password) < 8:
        return False

    if not any(char.isupper() for char in password):
        return False

    if not any(char.islower() for char in password):
        return False

    if not any(char.isdigit() for char in password):
        return False

    if not any(char in SPECIAL_CHARACTERS for char in password):
        return False

    return True


def str_encode(string: str) -> str:
    return base64.b85encode(string.encode('ascii')).decode('ascii')


def str_decode(string: str) -> str:
    return base64.b85decode(string.encode('ascii')).decode('ascii')


def get_token_payload(token: str, secret: str, algo: str):
    try:
        payload = jwt.decode(token, secret, algorithms=algo)
    except Exception as jwt_exec:
        logging.debug(f"JWT Error: {str(jwt_exec)}")
        payload = None
    return payload


def generate_token(payload: dict, secret: str, algo: str, expiry: timedelta):
    expire = datetime.now() + expiry
    payload.update({"exp": expire})
    return jwt.encode(payload, secret, algorithm=algo)


async def get_token_user(token: str, db:StandardDatabase ):
   
    payload = get_token_payload(token, settings.JWT_SECRET, settings.JWT_ALGORITHM)

    if payload:
        user_token_id = str_decode(payload.get('r'))
        user_id = str_decode(payload.get('sub'))
        access_key = payload.get('a')

        aql_query = f"""
        FOR token IN {db_collections.USER_TOKENS}
            FILTER token.access_key == @access_key
            FILTER token._key == @user_token_id
            FILTER token.user_id == @user_id
            FILTER token.expires_at > DATE_NOW()
            RETURN token
        """
        bind_vars = {
            'access_key': access_key,
            'user_token_id': user_token_id,
            'user_id': user_id
        }
  
        cursor = db.aql.execute(aql_query, bind_vars=bind_vars)

        user_token = [doc for doc in cursor][0]


        if user_token:
            user_collection = db.collection(db_collections.USERS)
            user = user_collection.get(user_token['user_id'])
            return user
    return None


async def load_user(email: str, db:StandardDatabase):
    collection = db.collection(db_collections.USERS)
    try:
        cursor =  collection.find({'email': email}, limit=1)
        user_cursor  = [doc for doc in cursor][0]
        user = user_cursor if user_cursor else None
    except Exception:
        logging.info(f"User Not Found, Email: {email}")
        user = None
    return user


# async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_arangodb_session)):
#     user = await get_token_user(token=token, db=db)
#     if user:
#         return user
#     raise HTTPException(status_code=401, detail="Not authorised.")    

from fastapi import Depends, HTTPException, status

def get_current_user_privileges(user_role: str):
    
    return AccessPrivileges.get(user_role, set())

def check_privilege(required_privilege: str):
    def dependency(user_role: str = Depends(get_current_user_privileges)):
        if required_privilege not in user_role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have permission to perform this action."
            )
    return dependency