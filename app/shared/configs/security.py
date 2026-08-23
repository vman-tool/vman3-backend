import base64
import json
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

def group_field_value_pairs(pairs):
    """Groups a flat [(field, value), ...] list into [(field, [values...]),
    ...], one entry per distinct field - the shared core behind both a
    user's access_limit and the dashboard's multi-level location filter,
    since both are "a set of field=value restrictions to OR together".

    Field names are interpolated directly into AQL by callers, so only
    identifier-shaped names are kept; that's normally guaranteed by them
    coming from configured location_level1-4 mappings, but this is cheap
    insurance against a malformed/malicious value reaching this far.
    """
    groups: dict = {}
    for field, value in pairs:
        if not field or value is None or not re.fullmatch(r'[A-Za-z0-9_]+', field):
            continue
        groups.setdefault(field, []).append(value)
    return list(groups.items())


def build_field_value_filter(
    pairs,
    bind_vars: dict,
    alias: str = "doc",
    param_prefix: str = "locationValues",
    case_insensitive: bool = False,
):
    """Appends one bind var per distinct field in `pairs` and returns an AQL
    filter fragment ORing them together (`(doc.field1 IN @p0) OR (doc.field2
    IN @p1)`), or None when `pairs` is empty. Mutates `bind_vars` in place,
    matching how callers already build up their AQL bind_vars dict. `alias`
    is the AQL document variable each caller's query already uses (`doc`,
    `va`, `dt`, `_fs`, ...); `case_insensitive` matches the LOWER()
    comparisons a couple of call sites need.
    """
    groups = group_field_value_pairs(pairs)
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
        pairs = [
            (item.get('field') or legacy_field, item.get('value'))
            for item in access_limit.get('limit_by', []) or []
        ]
        return group_field_value_pairs(pairs)
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
    """Same as build_field_value_filter, but deriving `pairs` from a user's
    own access_limit.limit_by (see get_location_limit_groups)."""
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


def parse_location_query_pairs(locations_json: str):
    """Parses the `locations` query param used by dashboard/statistics/export
    routes - a JSON-encoded array of {"field": ..., "value": ...} objects -
    into a flat [(field, value), ...] list. Returns [] on any malformed
    input rather than raising: this is a display filter, not an
    authorization boundary, so a bad filter should just show everything
    (still correctly scoped by the user's own access_limit elsewhere) rather
    than break the page.
    """
    if not locations_json:
        return []
    try:
        items = json.loads(locations_json)
        if not isinstance(items, list):
            return []
        return [
            (item.get('field'), item.get('value'))
            for item in items
            if isinstance(item, dict) and item.get('field') and item.get('value') is not None
        ]
    except Exception:
        return []


def build_locations_query_filter(
    locations_json: str,
    bind_vars: dict,
    alias: str = "doc",
    param_prefix: str = "userLocations",
    case_insensitive: bool = False,
):
    """Builds the AQL filter fragment for the `locations` query param (see
    parse_location_query_pairs), letting the dashboard/records/CCVA views be
    filtered by a combination of values spanning several admin levels at
    once, the same way access_limit restrictions can be."""
    return build_field_value_filter(
        parse_location_query_pairs(locations_json), bind_vars, alias, param_prefix, case_insensitive
    )

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