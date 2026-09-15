

import logging
from datetime import datetime, timedelta
from typing import Dict, List

from arango.database import StandardDatabase
from fastapi import BackgroundTasks, HTTPException, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.shared.configs.constants import db_collections
from app.shared.configs.models import Pager, ResponseMainModel, VManBaseModel
from app.shared.configs.security import (
    generate_token,
    get_token_payload,
    hash_password,
    is_password_strong_enough,
    load_user,
    str_decode,
    str_encode,
    verify_password,
)
from app.shared.configs.settings import get_settings
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.utils.database_utilities import record_exists, replace_object_values
from app.users.models.role import Role, UserAccessLimit, UserRole
from app.users.models.user import User, UserToken
from app.users.responses.user import RoleResponse, UserResponse, UserRolesResponse
from app.users.schemas.user import (
    AssignRolesRequest,
    RegisterUserRequest,
    RoleRequest,
    VerifyUserRequest,
)
from app.users.services.email import (
    send_account_activation_confirmation_email,
    send_password_reset_email,
)
from app.users.utils.string import unique_string
from app.utilits.data_validation import validate_privileges
from app.utilits.helpers import save_file
from app.shared.utils.cache import ttl_cache

settings = get_settings()


async def create_or_update_user_account(data: RegisterUserRequest, image: UploadFile = None, current_user: User = None,  db: StandardDatabase = None, background_tasks: BackgroundTasks = None):
    if data.password != data.confirm_password:
            raise HTTPException(status_code=400, detail="Password mismatch.")
    
    if data.confirm_password and not is_password_strong_enough(data.confirm_password):
            raise HTTPException(status_code=400, detail="Please provide a strong password.")
    
    if not data.uuid and not (data.password or data.confirm_password):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide password to create new user or uuid to update user.")
    
    existing_user = await User.get_many(filters={"email": data.email}, db=db)
    if not data.uuid and len(existing_user) > 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="This email address already exists.")

    if data.uuid:
        existing_users = await User.get_many(filters={"or_conditions": [{"uuid": data.uuid}, {"email": data.email}]}, db=db)
        if len(existing_users) > 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="This email address belongs to more than one user.")
        existing_user = existing_users[0]
        if existing_user:
            # Editing another user requires both the role privilege already
            # enforced on this route (check_privileges in users_routes.py)
            # AND that the target is within the editor's own org-unit scope
            # - a location-restricted admin must not be able to edit a user
            # outside their boundary even if their role otherwise permits it.
            scope_access_limit = (current_user or {}).get('access_limit') if current_user else None
            if scope_access_limit and scope_access_limit.get('limit_by'):
                target_limits = await UserAccessLimit.get_many(filters={"user": data.uuid}, db=db)
                target_access_limit = target_limits[0].get('access_limit') if target_limits else None
                if not await is_access_limit_within_scope(target_access_limit, scope_access_limit, db):
                    raise HTTPException(
                        status_code=403,
                        detail="You do not have permission to edit this user - they are outside your administrative boundary."
                    )

            hashed_password = None
            if data.confirm_password:
                hashed_password = hash_password(data.confirm_password)

            user_data = data.model_dump()

            if hashed_password:
                user_data['password'] = hashed_password

            update_user_data = replace_object_values(user_data, existing_user)

            if image:
                existing_image = update_user_data['image'] if "image" in update_user_data else None
                
                update_user_data['image'] = save_file(
                    file=image, 
                    valid_file_extensions = ['jpg', 'jpeg', 'png', 'ico', 'svg', 'gif', 'webp'], 
                    delete_extisting=existing_image
                )

            return await User(**update_user_data).update(updated_by = current_user["id"] if current_user and 'id' in current_user else None, db = db)
        else:
            raise HTTPException(status_code=404, detail="User not found.")

    user_data = {
        "name": data.name,
        "email": data.email,
        "password": hash_password(data.confirm_password),
        "is_active": data.is_active, # TODO: Change to false if you want to verify email first
        "verified_at":datetime.now().isoformat(), # TODO: Change to None if you want to verify email first
        "created_by": current_user["id"] if current_user and 'id' in current_user else None,
    }
    return await User(**user_data).save(db = db)
    
    
async def activate_user_account(data: VerifyUserRequest, db, background_tasks: BackgroundTasks):
    collection = db.collection(db_collections.USERS)
    user_cursor = await collection.find({'email': data.email}, limit=1).next()
    if not user_cursor:
        raise HTTPException(status_code=400, detail="This link is not valid.")

    user = user_cursor
    user_token = user.get('context_string', '')  # Adjust the context string retrieval as needed
    
    try:
        token_valid = verify_password(user_token, data.token)
    except Exception as verify_exec:
        logging.exception(verify_exec)
        token_valid = False

    if not token_valid:
        raise HTTPException(status_code=400, detail="This link either expired or not valid.")

    user['is_active'] = True
    user['updated_at'] = datetime.now().isoformat()
    user['verified_at'] = datetime.now().isoformat()

    await collection.update(user)
    
    # Activation confirmation email
    await send_account_activation_confirmation_email(user, background_tasks)
    return user


async def get_login_token(data, session):
    # Verify the email and password
    # Verify that user account is verified
    # Verify user account is active
    # Generate access_token and refresh_token and ttl
    
    user = await load_user(data.username, session)

    if not user:
        raise HTTPException(status_code=400, detail="Email is not registered with us.")

    if not verify_password(data.password, user['password']):
        raise HTTPException(status_code=400, detail="Incorrect email or password.")
    
    if not user['verified_at']:
        raise HTTPException(status_code=400, detail="Your account is not verified. Please check your email inbox to verify your account.")
    if not user['is_active']:
        raise HTTPException(status_code=400, detail="Your account has been deactivated. Please contact support.")
    
    # Generate the JWT Token
    res = await _generate_tokens(user, session)
    res["user"] = UserResponse(
            uuid=user["uuid"],
            id=user["_key"],
            name=user["name"],
            email=user["email"],
            is_active=user["is_active"],
            created_at=user.get("created_at"),
            created_by=user.get("created_by") if "created_by" in user else None,
            image=user.get("image") if "image" in user else None
        ).model_dump()
    return res
async def get_refresh_token(refresh_token: str, db: StandardDatabase):
    token_payload = get_token_payload(refresh_token, settings.SECRET_KEY, settings.JWT_ALGORITHM)
    if not token_payload:
        raise HTTPException(status_code=400, detail="Invalid Request.")
    

    refresh_key = token_payload.get('t')
    access_key = token_payload.get('a')
    user_id = str_decode(token_payload.get('sub'))

    filters = {
        'refresh_key': refresh_key,
        'access_key': access_key,
        'user_id': user_id,
        'expires_at': {'$gte': datetime.now().isoformat()}
    }

    user_token_cursor = await UserToken.get_many(
        limit=1,
        filters = filters, 
        db = db
    )


    if not user_token_cursor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Request.")

    user_token = user_token_cursor[0]
    user_token['expires_at'] = datetime.now().isoformat()
    updated_token = await UserToken(**user_token).update(user_token['user_id'], db)

    user = await User.get(doc_id = updated_token['user_id'], db = db)
    res = await _generate_tokens(user, db)
    res["user"] = UserResponse(
            uuid=user["uuid"],
            id=user["_key"],
            name=user["name"],
            email=user["email"],
            is_active=user["is_active"],
            created_at=user.get("created_at"),
            created_by=user.get("created_by") if "created_by" in user else None,
            image=user.get("image") if "image" in user else None
        ).model_dump()

    return res


async def _generate_tokens(user, db: StandardDatabase):
  
    refresh_key = unique_string(100)
    access_key = unique_string(50)
    rt_expires = timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)

    user_token = {
        "id": "",
        "user_id": user["_key"],
        "created_by": user["_key"],
        "refresh_key": refresh_key,
        "access_key": access_key,
        "expires_at": (datetime.now() + rt_expires).isoformat()
    }

    # collection = db.collection(db_collections.USER_TOKENS)
    # collection.insert(user_token)
    # collection =  db.collection(db_collections.USER_TOKENS)
    # insert_result =  collection.insert(user_token, return_new=True)
    # inserted_user_token = insert_result["new"]

    inserted_user_token = await UserToken(**user_token).save(db)

    at_payload = {
        "sub": str_encode(str(user["_key"])),
        'a': access_key,
        'r': str_encode(str(inserted_user_token["_key"])),
        'n': str_encode(f"{user['name']}")
    }

    at_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = generate_token(at_payload, settings.JWT_SECRET, settings.JWT_ALGORITHM, at_expires)

    rt_payload = {"sub": str_encode(str(user["_key"])), "t": refresh_key, 'a': access_key}
    refresh_token = generate_token(rt_payload, settings.SECRET_KEY, settings.JWT_ALGORITHM, rt_expires)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": at_expires.total_seconds(),
        "refresh_token_expires_in": rt_expires.total_seconds()
    }
    
async def email_forgot_password_link(data, background_tasks, session):
    
    user = await load_user(data.email, session)
    if not user.verified_at:
        raise HTTPException(status_code=400, detail="Your account is not verified. Please check your email inbox to verify your account.")
    
    if not user.is_active:
        raise HTTPException(status_code=400, detail="Your account has been dactivated. Please contact support.")
    
    await send_password_reset_email(user, background_tasks)
    
class ResetPasswordData(BaseModel):
    email: str
    token: str
    password: str
     
async def reset_user_password(data: ResetPasswordData, db):
    user = await load_user(data.email, db)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid request")

    if not user.get('verified_at'):
        raise HTTPException(status_code=400, detail="Invalid request")

    if not user.get('is_active'):
        raise HTTPException(status_code=400, detail="Invalid request")

    user_token = user.get('context_string')  # Adjust context string retrieval as needed
    try:
        token_valid = verify_password(user_token, data.token)
    except Exception as verify_exec:
        logging.exception(verify_exec)
        token_valid = False
    if not token_valid:
        raise HTTPException(status_code=400, detail="Invalid window.")

    user['password'] = hash_password(data.password)
    user['updated_at'] = datetime.now().isoformat()

    collection = db.collection(db_collections.USERS)
    await collection.update(user)

    # Notify user that password has been updated
    # Implement your notification logic here

    return {"msg": "Password updated successfully"}
    
    
async def fetch_user_detail(pk: str, current_user: dict = None, db: StandardDatabase = None):
    user = await User.get(doc_uuid=pk, db=db)

    if user:
        scope_access_limit = (current_user or {}).get('access_limit')
        if scope_access_limit and scope_access_limit.get('limit_by'):
            target_limits = await UserAccessLimit.get_many(filters={"user": pk}, db=db)
            target_access_limit = target_limits[0].get('access_limit') if target_limits else None
            if not await is_access_limit_within_scope(target_access_limit, scope_access_limit, db):
                raise HTTPException(status_code=403, detail="You do not have access to view this user.")

        return UserResponse(
        uuid=user["uuid"],
        id=user["_key"],
        name=user["name"],
        email=user["email"],
        is_active=user["is_active"],
        created_at=user.get("created_at"),
        created_by=user.get("created_by") if "created_by" in user else None,
        image=user.get("image") if "image" in user else None
    )
    raise HTTPException(status_code=400, detail="User does not exist.")

async def _fetch_roles_and_limits(user_uuids: List[str], db: StandardDatabase) -> Dict:
    if not user_uuids:
        return {}
    query = f"""
        FOR uuid IN @user_uuids
            LET access_info = (
                FOR a IN {db_collections.USER_ACCESS_LIMIT}
                    FILTER a.user == uuid AND a.is_deleted == false
                    RETURN a.access_limit
            )[0]
            LET role_list = (
                FOR ur IN {db_collections.USER_ROLES}
                    FILTER ur.user == uuid AND ur.is_deleted == false
                    LET role_doc = (
                        FOR r IN {db_collections.ROLES}
                            FILTER r.uuid == ur.role AND r.is_deleted == false
                            RETURN {{ uuid: r.uuid, name: r.name }}
                    )[0]
                    FILTER role_doc != null
                    RETURN role_doc
            )
            RETURN {{ user: uuid, roles: role_list, access_limit: access_info }}
    """
    results = await VManBaseModel.run_custom_query(query=query, bind_vars={'user_uuids': user_uuids}, db=db)
    return {item['user']: item for item in results} if results else {}


async def fetch_users(paging: bool = None, page_number: int = None, limit: int = None, search: str = None, current_user: dict = None, db: StandardDatabase = None):
    filters = {}
    if search:
        filters['like_conditions'] = [
            {'name': search},
            {'email': search}
        ]

    scope_access_limit = (current_user or {}).get('access_limit')
    scoped = bool(scope_access_limit and scope_access_limit.get('limit_by'))

    if scoped:
        # A location-restricted viewer must only ever see users within their
        # own org unit or below. Fetch every match unpaginated, filter by
        # each candidate's own access_limit, then paginate the filtered set
        # in Python - user tables are small (unlike VA records), so this is
        # the safe, easy-to-verify choice rather than pushing hierarchy
        # containment into AQL.
        all_users = await User.get_many(filters=filters, paging=False, db=db)
        if not all_users:
            raise HTTPException(status_code=400, detail="Users not found.")

        extras = await _fetch_roles_and_limits([u['uuid'] for u in all_users], db)

        in_scope_users = [
            u for u in all_users
            if await is_access_limit_within_scope(extras.get(u['uuid'], {}).get('access_limit'), scope_access_limit, db)
        ]
        if not in_scope_users:
            raise HTTPException(status_code=400, detail="Users not found.")

        total_users = len(in_scope_users)
        if paging and page_number and limit:
            start = (page_number - 1) * limit
            users = in_scope_users[start:start + limit]
        else:
            users = in_scope_users
    else:
        users = await User.get_many(
            limit=limit,
            page_number=page_number,
            paging=paging,
            filters=filters,
            db=db
        )

        total_users = await User.count(filters=filters, include_deleted=None, db=db)

        if not users:
            raise HTTPException(status_code=400, detail="Users not found.")

        extras = await _fetch_roles_and_limits([u['uuid'] for u in users], db)

    user_data = [
        UserResponse(
            uuid=user["uuid"],
            id=user["_key"],
            name=user["name"],
            email=user["email"],
            is_active=user["is_active"],
            created_at=user["created_at"],
            created_by=user.get("created_by"),
            image=user.get("image"),
            roles=extras.get(user['uuid'], {}).get('roles', []),
            access_limit=extras.get(user['uuid'], {}).get('access_limit'),
        ) for user in users
    ]

    pager = Pager(page=page_number, limit=limit) if paging else None
    return ResponseMainModel(
        data=user_data,
        message="Users fetched successfully",
        total=total_users,
        pager=pager,
    )

# async def fetch_user_detail(pk: str, db: StandardDatabase):
#     loop = asyncio.get_event_loop()
#     user = await loop.run_in_executor(None, sync_fetch_user_detail, pk, db)
#     if user:
#         return user
#     raise HTTPException(status_code=400, detail="User does not exist.")

# def sync_fetch_user_detail(pk: str, db: StandardDatabase):
#     collection = db.collection(db_collections.USERS)
#     cursor = collection.find({'_key': pk}, limit=1)
#     user_cursor = [doc for doc in cursor]
#     if user_cursor:
#         return user_cursor[0]
#     return None

# @app.get("/users/{pk}")
# async def get_user_detail(pk: str, db = Depends(get_arangodb_session)):
#     user = await fetch_user_detail(pk, db)
#     return user        return user_cursor
    raise HTTPException(status_code=400, detail="User does not exist.")

# @app.get("/users/{pk}")
# async def get_user_detail(pk: str, db = Depends(get_arangodb_session)):
#     user = await fetch_user_detail(pk, db)
#     return user    raise HTTPException(status_code=400, detail="User does not exist.")

# @app.get("/users/{pk}")
# async def get_user_detail(pk: str, db = Depends(get_arangodb_session)):
#     user = await fetch_user_detail(pk, db)
#     return user

async def fetch_roles(paging: bool = None, page_number: int = None, limit: int = None, filters: Dict = None, include_deleted: bool = False, db: StandardDatabase = None):
    try:
        roles = await Role.get_many(paging = paging, page_number = page_number, limit = limit, filters=filters, db=db)
        formarted_roles = []
        for role in roles:
            formarted_roles.append(await RoleResponse.get_structured_role(role = role, db=db))
        roles_count = await Role.count(filters=filters, db=db)
        pager = None
        if paging:
            pager = Pager(page = page_number, limit=limit)
        return ResponseMainModel(data = formarted_roles, message = "Role fetched successfully!", total = roles_count ,pager = pager)
    except Exception as e:
        raise e

async def save_role(data: RoleRequest = None, current_user: User = None, db: StandardDatabase = None):
    try:
        if validate_privileges(data.privileges):
            filters = { "or_conditions": [
                {"uuid": data.uuid}, 
                {"name": data.name}
            ]}
            existing_role = await Role.get_many(filters = filters, db=db)
            if len(existing_role) == 1:
                existing_role = existing_role[0]
                role_json = replace_object_values(data.model_dump(), existing_role)
                role = await Role(**role_json).update(updated_by = current_user.uuid, db=db)
                message = "Role updated successfully."
            elif len(existing_role) > 1:
                raise HTTPException(status_code=400, detail="Multiple roles found with the same name or UUID.")
            else:
                role_json = data.model_dump()
                role_json['created_by'] = current_user.uuid
                role = await Role(**role_json).save(db=db)
                message = "Role created successfully"
            return ResponseMainModel(data = await RoleResponse.get_structured_role(role = role, db=db), message=message)
        else:
            raise HTTPException(status_code=400, detail="Invalid privileges have been defined.")
    except Exception as e:
        raise e

async def delete_role(data: List[str] = [], current_user: User = None, db: StandardDatabase = None):
    try:
        for role in data:
            await Role.delete(doc_uuid=role, deleted_by = current_user['uuid'], db=db)            
    except Exception as e:
        raise e

# ---------------------------------------------------------------------------
# Location-boundary containment - shared by assign_roles (what access a
# scoped admin may grant) and by fetch_users/fetch_user_detail/
# create_or_update_user_account (who a scoped admin may see or edit). There
# is no separate admin-boundary table in this app: "is district X inside
# region Y" is answered by checking whether any real VA record carries both
# values at once (db_collections.VA_TABLE is the only source of truth for
# hierarchy). This was originally inline only inside assign_roles.
# ---------------------------------------------------------------------------

def _location_pairs(access_limit: dict) -> list:
    """Flattens an access_limit dict's limit_by entries into (field, value)
    tuples. Supports both the current per-item `field` shape (a user can be
    restricted across several admin levels at once) and the legacy single
    top-level `field` shared by every limit_by item."""
    if not access_limit:
        return []
    legacy_field = access_limit.get('field', '')
    return [
        (item.get('field') or legacy_field, item.get('value'))
        for item in access_limit.get('limit_by', [])
        if (item.get('field') or legacy_field) and item.get('value') is not None
    ]


async def _pair_within_scope(field: str, value: str, scope_pairs: list, field_to_level: dict, db: StandardDatabase) -> bool:
    """True if (field, value) is the same as, or a descendant of, at least
    one entry in scope_pairs. A level deeper than a scope pair (e.g.
    scope=district, candidate=ward) is only a descendant if a real VA
    record actually carries both values at once."""
    candidate_level = field_to_level.get(field, 0)
    if candidate_level == 0:
        return False
    for s_field, s_value in scope_pairs:
        s_level = field_to_level.get(s_field, 0)
        if s_level == 0 or candidate_level < s_level:
            continue
        if field == s_field:
            if value == s_value:
                return True
            continue

        def _check_descendant():
            cursor = db.aql.execute(
                f"FOR doc IN {db_collections.VA_TABLE} "
                f"FILTER doc.{s_field} == @sv AND doc.{field} == @cv "
                f"LIMIT 1 RETURN 1",
                bind_vars={"sv": s_value, "cv": value},
            )
            return next(cursor, None) is not None

        if await run_in_threadpool(_check_descendant):
            return True
    return False


async def is_access_limit_within_scope(candidate_access_limit: dict, scope_access_limit: dict, db: StandardDatabase) -> bool:
    """True if every location `candidate_access_limit` restricts to is
    within, or equal to, at least one location `scope_access_limit`
    restricts to.

    - scope has no pairs (an unrestricted viewer) -> always True, matching
      the "if current_user.get('access_limit') and ...limit_by" guard used
      elsewhere in this file to mean "no restriction, sees/grants
      everything".
    - candidate has no pairs (an unrestricted target, e.g. another admin)
      while scope IS restricted -> False. A location-restricted user must
      never see or edit an account with no location restriction of its own.
    - otherwise every candidate pair must pass _pair_within_scope against at
      least one scope pair (candidate pairs AND'd, scope pairs OR'd - the
      same semantics assign_roles already used for granting access).
    - if the ODK field mapping can't be resolved, fails closed (denies)
      rather than silently allowing an unverifiable boundary claim.
    """
    scope_pairs = _location_pairs(scope_access_limit)
    if not scope_pairs:
        return True

    candidate_pairs = _location_pairs(candidate_access_limit)
    if not candidate_pairs:
        return False

    try:
        config = await fetch_odk_config(db)
        fm = config.field_mapping
        field_to_level = {
            fm.location_level1: 1,
            fm.location_level2: 2,
            fm.location_level3: 3,
            fm.location_level4: 4,
        } if fm else {}
    except Exception:
        field_to_level = {}

    for field, value in candidate_pairs:
        if not await _pair_within_scope(field, value, scope_pairs, field_to_level, db):
            return False
    return True


async def assign_roles(data: AssignRolesRequest = None, current_user: User = None, current_user_privileges: List[str] = None, db: StandardDatabase = None):
    try:
        filters = {
            "in_conditions": [
                {'uuid': data.roles}
            ]
        }
        existing_roles = await Role.get_many(filters=filters, db=db)
        existing_user = await record_exists(collection_name = db_collections.USERS, uuid = data.user, db = db)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User does not exist.")

        if len(existing_roles) < len(data.roles or []):
            raise HTTPException(status_code=404, detail="Some roles do not exist.")

        # Privileges are admin-editable data, not fixed by role name, and
        # roles have no inherent ranking - "coder" or "read_only" could be
        # granted USERS_ASSIGN_ROLES at any time via role management. So the
        # only structurally sound rule is: you can only hand out a role
        # whose privileges are entirely covered by your own, whatever those
        # happen to be right now. A user with every privilege (e.g.
        # superuser) always passes this trivially.
        assigner_privileges = set(current_user_privileges or [])
        for role in existing_roles:
            missing = set(role.get('privileges') or []) - assigner_privileges
            if missing:
                raise HTTPException(
                    status_code=403,
                    detail=f"You cannot assign the '{role.get('name')}' role: it grants privileges you don't have ({', '.join(sorted(missing))})."
                )

        existing_user_roles = await UserRole.get_many(filters={"user": data.user}, db=db)
        for user_role in existing_user_roles:
            if user_role and data.roles and 'role' in user_role and user_role['role'] not in data.roles:
                await UserRole.delete(doc_uuid=user_role.get('uuid'), deleted_by=current_user['uuid'] if 'uuid' in current_user else None, db=db)

        if data.roles:
            for role in data.roles:
                existing_user_role = await UserRole.get_many(filters={"user": data.user, "role": role}, db=db)
                if len(existing_user_role) == 0:
                    user_role_json = {
                        "user": data.user,
                        "role": role,
                        "created_by": current_user["uuid"] if 'uuid' in current_user else None
                    }
                    user_role = await UserRole(**user_role_json).save(db=db)
                elif len(existing_user_role) == 1:
                    continue
        if current_user.get('access_limit') and current_user['access_limit'].get('limit_by'):
            # The creator's own access is itself restricted, so whatever they
            # grant this user must stay within that same boundary: never
            # broader, never a disjoint area at the same level (e.g. a
            # different district), and never left unrestricted altogether.
            try:
                new_pairs = _location_pairs(data.access_limit) if data.access_limit else []

                if not new_pairs:
                    raise HTTPException(
                        status_code=403,
                        detail="Your own account is access-limited, so you must restrict this user to at least one location within your own boundary."
                    )

                if not await is_access_limit_within_scope(data.access_limit, current_user['access_limit'], db):
                    raise HTTPException(
                        status_code=403,
                        detail="You can only grant access within your own administrative boundary."
                    )
            except HTTPException:
                raise
            except Exception:
                pass

        if data.access_limit:
            existing_access_limit = await UserAccessLimit.get_many(filters={"user": data.user}, db=db)
            if len(existing_access_limit) == 1:
                access_limit_json = replace_object_values(data.model_dump(), existing_access_limit[0])
                if data.access_limit:
                    await UserAccessLimit(**access_limit_json).update(updated_by=current_user['uuid'] if 'uuid' in current_user else None, db=db)
                else:
                    await UserAccessLimit.delete(doc_uuid = access_limit_json["uuid"], db=db)
            elif not existing_access_limit:
                await UserAccessLimit(**{
                    "user": data.user, 
                    "access_limit": data.access_limit,
                    "created_by": current_user['uuid'] if 'uuid' in current_user else None
                }).save(db=db)
        
        
        user_roles = await get_user_roles(data.user, current_user, db=db)

        return ResponseMainModel(data = user_roles.data, message="Roles were successfully assigned!")
    except Exception as e:
        raise e

async def unassign_roles(data: AssignRolesRequest = None, current_user: User = None, db: StandardDatabase = None):
    try:
        existing_roles = await Role.get_many(filters={"in_conditions": [{'uuid': data.roles}]}, db=db)
        if not record_exists(db_collections.USERS, data.user):
            raise HTTPException(status_code=404, detail="User does not exist.")
        
        if len(existing_roles) != len(data.roles):
            raise HTTPException(status_code=404, detail="Some roles do not exist.")

        for role in data.roles:
            existing_user_role = await UserRole.get_many(filters={"user": data.user, "role": role}, db=db)
            if len(existing_user_role) == 1:
                await UserRole.delete(doc_uuid=existing_user_role[0].get('uuid'), deleted_by=current_user['uuid'], db=db)
            else:
                raise HTTPException(status_code=404, detail="Could not finish role unassignment due to duplicate records")
        user_roles = await get_user_roles(data.user, current_user, db=db)

        return ResponseMainModel(data = user_roles.data, message="Roles were successfully unassigned!")         
    except Exception as e:
        raise e
    
# @ttl_cache(ttl=300, key_prefix="user_roles")
async def get_user_roles(user_uuid: str  = None, current_user: User = None, db: StandardDatabase = None):
    try:
        if user_uuid is None:
            user_uuid = current_user['uuid']

        query = f"""
            LET access_info = (
                FOR a IN {db_collections.USER_ACCESS_LIMIT}
                    FILTER a.user == @user_uuid AND a.is_deleted == false
                    RETURN a.access_limit
            )[0]

            LET user_role_object = (
                FOR user_role IN {db_collections.USER_ROLES}
                    FILTER user_role.user == @user_uuid AND user_role.is_deleted == false

                    // Fetch the role associated with each user_role
                    LET role = (
                        FOR r IN {db_collections.ROLES}
                            FILTER r.uuid == user_role.role
                            RETURN {{ uuid: r.uuid, name: r.name, privileges: r.privileges }}
                    )[0]

                    COLLECT user = user_role.user INTO roleGroups

                    RETURN {{
                        user: user,
                        roles: roleGroups[*].role,
                        access_limit: access_info
                    }}
            )

            // If user_role_object is empty, return a default structure
            RETURN LENGTH(user_role_object) > 0 
                ? user_role_object[0]
                : {{ user: @user_uuid, roles: [], access_limit: access_info }}

            """

        bind_vars = {
            "user_uuid": user_uuid
        }

        user_exists = await record_exists(db_collections.USERS, user_uuid, db=db)
        if not user_exists:
            raise HTTPException(status_code=404, detail="User does not exist.")

        user_roles_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        if user_roles_result:
            user_roles = [user_role for user_role in user_roles_result]
            user_role = user_roles[0]
            roles = []
            if 'roles' in user_role and len(user_role['roles']) > 0:
                for role in user_role["roles"]:
                    if role is not None:
                        roles.append(role)
                user_role["roles"] = roles            
            user_roles_response = await UserRolesResponse.get_structured_user_role(user_role=user_role, db=db)
            return ResponseMainModel(data=user_roles_response, message='User Roles Fetched successfully.')
        else:
            return ResponseMainModel(data=[], message='No roles found for this user.')
        
    except Exception as e:
        raise e
    