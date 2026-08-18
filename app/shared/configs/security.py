import base64
import logging
import re
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
    payload.update({"exp": expire})
    return jwt.encode(payload, secret, algorithm=algo)


def create_export_token(user_id: str):
    """
    Creates a short-lived token (1 minute) for export operations.
    """
    payload = {
        "sub": user_id,
        "type": "export_token"
    }
    # 1 minute expiry
    return generate_token(payload, settings.JWT_SECRET, settings.JWT_ALGORITHM, timedelta(minutes=1))


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
            ac= defaultsCr[0] if len(defaultsCr) > 0 else None
            if len(active_user) > 0:
                return {
                    "uuid": active_user[0].get("uuid"),
                    "id": active_user[0].get("_key"),
                    "name": active_user[0].get("name"),
                    "email": active_user[0].get("email"),
                    "is_active": active_user[0].get("is_active"),
                    "created_at": active_user[0].get("created_at"),
                    "created_by": active_user[0].get("created_by"),
                    "image": active_user[0].get("image"),
                    'access_limit': ac.get('access_limit') if ac is not None else None
                }
    return None

def get_location_limit_groups(current_user):
    """Groups a user's access_limit.limit_by entries by admin-level field.

    Returns [(field, [values, ...]), ...] - one group per field the user's
    access is restricted to, or [] for no restriction (full access). A user
    can now be limited to a combination of values spanning several admin
    levels at once (e.g. district_a OR ward_b), not just one level.

    Supports both the legacy shape (a single top-level `field` shared by all
    `limit_by` items) and the current shape (each `limit_by` item carries its
    own `field`), so previously-saved users keep working unchanged.
    """
    try:
        access_limit = current_user.get('access_limit', {}) or {}
        legacy_field = access_limit.get('field', '')
        groups: dict = {}
        for item in access_limit.get('limit_by', []) or []:
            field = item.get('field') or legacy_field
            value = item.get('value')
            # Field names are interpolated directly into AQL below by callers;
            # only allow identifier-shaped values (they come from configured
            # location_level1-4 field mappings, not free user input, but this
            # is cheap insurance against a malformed/malicious config value).
            if not field or value is None or not re.fullmatch(r'[A-Za-z0-9_]+', field):
                continue
            groups.setdefault(field, []).append(value)
        return list(groups.items())
    except Exception as e:
        print(f"Error processing location limit values: {e}")
        return []


def build_location_limit_filter(
    current_user,
    bind_vars: dict,
    alias: str = "doc",
    param_prefix: str = "locationValues",
    case_insensitive: bool = False,
):
    """Appends one bind var per restricted field and returns an AQL filter
    fragment ORing them together (`(doc.field1 IN @p0) OR (doc.field2 IN @p1)`),
    or None when the user has no location restriction. Mutates `bind_vars` in
    place, matching how callers already build up their AQL bind_vars dict.
    `alias` is the AQL document variable each caller's query already uses
    (`doc`, `va`, `dt`, `_fs`, ...); `case_insensitive` matches the LOWER()
    comparisons a couple of call sites used before this was a single field.
    """
    groups = get_location_limit_groups(current_user)
    if not groups:
        return None
    clauses = []
    for i, (field, values) in enumerate(groups):
        key = f"{param_prefix}{i}"
        if case_insensitive:
            bind_vars[key] = [v.lower() for v in values]
            clauses.append(f"LOWER({alias}.{field}) IN @{key}")
        else:
            bind_vars[key] = values
            clauses.append(f"{alias}.{field} IN @{key}")
    return "(" + " OR ".join(clauses) + ")"

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