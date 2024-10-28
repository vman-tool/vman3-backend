import base64
import logging
from datetime import datetime, timedelta

import jwt
from arango.database import StandardDatabase
from passlib.context import CryptContext

from app.shared.configs.constants import db_collections
from app.shared.configs.settings import get_settings
from app.users.models.user import User, UserToken

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

        filters = {
            'access_key': access_key,
            '_key': user_token_id,
            'user_id': user_id,
            'expires_at': {'$gte': datetime.now().isoformat()}
        }

        user_token_cursor = await UserToken.get_many(
            limit=1,
            filters = filters, 
            db = db
        )

        if user_token_cursor:
            user_token = user_token_cursor[0]
            filters = {
                '_key': user_token['user_id'],
                'is_active': True,
            }
            active_user = await User.get_many(filters = filters, db = db)
            ## temporary solution to

            user_id=active_user[0]['uuid']
            if user_id is not None:
                cursor_cr = db.collection(db_collections().USER_ACCESS_LIMIT).find({
                    "user": user_id,
                    "is_deleted": False
                })


                defaultsCr = [{key: document.get(key, None) for key in document} for document in cursor_cr]

            if len(active_user) > 0:
                return {
                    "uuid": active_user[0]["uuid"],
                    "id": active_user[0]["_key"],
                    "name": active_user[0]["name"],
                    "email": active_user[0]["email"],
                    "is_active": active_user[0]["is_active"],
                    "created_at": active_user[0].get("created_at"),
                    "created_by": active_user[0].get("created_by") if "created_by" in active_user[0] else None,
                    "image": active_user[0].get("image") if "image" in active_user[0] else None,
                    'access_limit': defaultsCr[0]['access_limit'] if len(defaultsCr) > 0 else None
                }
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