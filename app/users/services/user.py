

import logging
from datetime import datetime, timedelta
from typing import Dict, List

from arango.database import StandardDatabase
from fastapi import BackgroundTasks, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from app.shared.configs.constants import AccessPrivileges, db_collections
from app.shared.configs.models import BaseResponseModel, Pager, ResponseMainModel
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
from app.shared.utils.database_utilities import record_exists, replace_object_values
from app.shared.utils.response import populate_user_fields
from app.users.models.role import Role, UserAccessLimit, UserRole
from app.users.models.user import User, UserToken
from app.users.responses.user import RoleResponse, UserResponse, UserRolesResponse
from app.users.schemas.user import AssignRolesRequest, RegisterUserRequest, RoleRequest, VerifyUserRequest
from app.users.services.email import (
    send_account_activation_confirmation_email,
    send_password_reset_email,
)
from app.users.utils.string import unique_string
from app.utilits.data_validation import validate_privileges

settings = get_settings()


async def create_or_update_user_account(data: RegisterUserRequest, current_user: User,  db: StandardDatabase, background_tasks: BackgroundTasks):
    if data.password != data.confirm_password:
            raise HTTPException(status_code=400, detail="Password mismatch.")
    
    if data.confirm_password and not is_password_strong_enough(data.confirm_password):
            raise HTTPException(status_code=400, detail="Please provide a strong password.")
    
    if not data.uuid and not (data.password or data.confirm_password):
        raise HTTPException(status_code=400, detail="Provide password to create new user or uuid to update user.")
    
    existing_user = None
    if data.uuid:
        existing_user = await User.get(doc_uuid=data.uuid, db=db)

        if existing_user:
            hashed_password = None
            if data.confirm_password:
                hashed_password = hash_password(data.confirm_password)

            user_data = data.model_dump()

            if hashed_password:
                user_data['password'] = hashed_password

            update_user_data = replace_object_values(user_data, existing_user)
            
            return await User(**update_user_data).update(updated_by = current_user['uuid'], db = db)
        else:
            raise HTTPException(status_code=404, detail="User not found.")



    user_data = {
        "name": data.name,
        "email": data.email,
        "password": hash_password(data.confirm_password),
        "is_active": data.is_active, # TODO: Change to false if you want to verify email first
        "verified_at":datetime.now().isoformat(), # TODO: Change to None if you want to verify email first
        "created_by": current_user["_key"], # TODO: Change to the user id of the user creating the account
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
    user['updated_at'] = datetime.utcnow().isoformat()
    user['verified_at'] = datetime.utcnow().isoformat()

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
            created_at=user.get("created_at")
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
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Request.")

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
            created_at=user.get("created_at")
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
        "expires_in": at_expires.seconds
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
    
    
async def fetch_user_detail(pk: str, db):
    user = await User.get(doc_uuid=pk, db=db)

    if user:
        return UserResponse(
        uuid=user["uuid"],
        id=user["_key"],
        name=user["name"],
        email=user["email"],
        is_active=user["is_active"],
        created_at=user.get("created_at")
    )
    raise HTTPException(status_code=400, detail="User does not exist.")

async def fetch_users(paging: bool= None, page_number: int = None, limit: int = None, db: StandardDatabase = None):
    users = await User.get_many(
        limit=limit,
        page_number=page_number,
        paging=paging,
        db=db
    )

    total_users = await User.count(db=db)
    user_data = [
                UserResponse(
                    uuid=user["uuid"],
                    id=user["_key"],
                    name=user["name"],
                    email=user["email"],
                    is_active=user["is_active"],
                    created_at=user.get("created_at")
                ) for user in users
            ]
    
    if users:
        return ResponseMainModel(
                data=user_data, 
                message="Users fetched successfully",
                total= total_users,
                pager=Pager(
                    page=page_number,
                    limit=limit
                )
            )
    raise HTTPException(status_code=400, detail="Users not found.")

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
        return ResponseMainModel(data = formarted_roles, message = "Role fetched successfully!", total = roles_count ,pager = Pager(page = page_number, limit=limit))
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
                print(role_json)
                role = await Role(**role_json).update(updated_by = current_user['uuid'], db=db)
                message = "Role updated successfully."
            elif len(existing_role) > 1:
                raise HTTPException(status_code=400, detail="Multiple roles found with the same name or UUID.")
            else:
                role_json = data.model_dump()
                role_json['created_by'] = current_user['uuid']
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

async def assign_roles(data: AssignRolesRequest = None, current_user: User = None, db: StandardDatabase = None):
    try:
        filters = {
            "in_conditions": [
                {'uuid': data.roles}
            ]
        }
        existing_roles = await Role.get_many(filters=filters, db=db)
        if not record_exists(db_collections.USERS, data.user):
            raise HTTPException(status_code=404, detail="User does not exist.")
        
        if len(existing_roles) != len(data.roles):
            raise HTTPException(status_code=404, detail="Some roles do not exist.")
        
        existing_user_roles = await UserRole.get_many(filters={"user": data.user}, db=db)
        for user_role in existing_user_roles:
            if user_role['role'] not in data.roles:
                await UserRole.delete(doc_uuid=user_role.get('uuid'), deleted_by=current_user['uuid'], db=db)

        for role in data.roles:
            existing_user_role = await UserRole.get_many(filters={"user": data.user, "role": role}, db=db)
            if len(existing_user_role) == 0:
                user_role_json = {
                    "user": data.user,
                    "role": role,
                    "created_by": current_user['uuid']
                }
                user_role = await UserRole(**user_role_json).save(db=db)
            elif len(existing_user_role) == 1:
                continue
        
        existing_access_limit = await UserAccessLimit.get_many(filters={"user": data.user}, db=db)
        if len(existing_access_limit) == 1:
            await UserAccessLimit.update(doc_uuid=existing_access_limit[0].get('uuid'), updated_by=current_user['uuid'], db=db)
        elif not existing_access_limit:
            await UserAccessLimit(**{
                "user": data.user, 
                "access_limit": data.access_limit,
                "created_by": current_user['uuid']
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
    
async def get_user_roles(user_uuid: str  = None, current_user: User = None, db: StandardDatabase = None):
    try:
        if user_uuid is None:
            user_uuid = current_user['uuid']
        
        query = f"""
            LET access_limit = (
                FOR a IN {db_collections.USER_ACCESS_LIMIT}
                    FILTER a.user == @user_uuid
                    RETURN {{ access_limit: a.access_limit }}
            )
            FOR user_role IN {db_collections.USER_ROLES}
                FILTER user_role.user == @user_uuid AND user_role.is_deleted == false
                
                LET role = (
                    FOR r IN {db_collections.ROLES}
                        FILTER r.uuid == user_role.role
                        RETURN {{ uuid: r.uuid, name: r.name, privileges: r.privileges }}
                )[0]
                
                COLLECT user = user_role.user INTO roleGroups
                
                RETURN MERGE(
                    FIRST(roleGroups[*].user_role),
                    {{roles: roleGroups[*].role}},
                    {{access_limit: access_limit}}
                )
        """
        query = f"""
            LET access_info = (
                FOR a IN {db_collections.USER_ACCESS_LIMIT}
                    FILTER a.user == @user_uuid AND a.is_deleted == false
                    RETURN a.access_limit
                )[0]

            FOR user_role IN {db_collections.USER_ROLES}
                FILTER user_role.user == @user_uuid AND user_role.is_deleted == false

                // Fetch the role associated with the user_role
                LET role = (
                    FOR r IN {db_collections.ROLES}
                    FILTER r.uuid == user_role.role
                    RETURN {{ uuid: r.uuid, name: r.name, privileges: r.privileges }}
                )[0]

                COLLECT user = user_role.user INTO roleGroups
                
                RETURN MERGE(
                    FIRST(roleGroups[*].user_role),
                    {{roles: roleGroups[*].role}},
                    {{access_limit: access_info}}
                )
        """

        bind_vars = {
            "user_uuid": user_uuid
        }

        user_exists = await record_exists(db_collections.USERS, user_uuid, db=db)
        if not user_exists:
            raise HTTPException(status_code=404, detail="User does not exist.")

        user_roles_result = await UserRole.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        
        if user_roles_result:
            user_roles = user_roles_result.next()            
            user_roles_response = await UserRolesResponse.get_structured_user_role(user_role=user_roles, db=db)
            return ResponseMainModel(data=user_roles_response, message='User Roles Fetched successfully.')
        else:
            return ResponseMainModel(data=[], message='No roles found for this user.')
        
    except Exception as e:
        raise e
    