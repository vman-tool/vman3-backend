
from fastapi import HTTPException
from typing import Any, Dict, List, Optional, Union
from arango.database import StandardDatabase
from fastapi import APIRouter, BackgroundTasks, Depends, Header, Query, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

# from sqlalchemy.orm import Session
from app.shared.configs.arangodb import ArangoDBClient, get_arangodb_session
from app.shared.configs.constants import AccessPrivileges
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import check_privileges, get_current_user, oauth2_scheme
from app.users.responses.user import LoginResponse, UserResponse
from app.users.schemas.user import (AssignRolesRequest, EmailRequest, RegisterUserRequest,
                                    ResetRequest, RoleRequest, VerifyUserRequest)
from app.users.services import user

user_router = APIRouter(
    prefix="/users",
    tags=["Users"],
    responses={404: {"description": "Not found"}},
)

auth_router = APIRouter(
    prefix="/users",
    tags=["Users"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

guest_router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
    responses={404: {"description": "Not found"}},
)


# @created_by_decorator
@auth_router.post("/", status_code=status.HTTP_201_CREATED, response_model=UserResponse)

async def register_user(
    data: RegisterUserRequest, 
    background_tasks: BackgroundTasks, 
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_CREATE_USER])),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    return await user.create_or_update_user_account(data, current_user, db, background_tasks)

@auth_router.put("/", status_code=status.HTTP_200_OK, response_model=UserResponse)
async def update_user(
    data: RegisterUserRequest, 
    background_tasks: BackgroundTasks, 
    current_user = Depends(get_current_user),
    required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_UPDATE_USER, AccessPrivileges.USERS_DEACTIVATE_USER])),
    db: StandardDatabase = Depends(get_arangodb_session)):
    return await user.create_or_update_user_account(data, current_user, db, background_tasks)

@user_router.post("/verify", status_code=status.HTTP_200_OK)
async def verify_user_account(data: VerifyUserRequest, background_tasks: BackgroundTasks, session = Depends(get_arangodb_session)):
    await user.activate_user_account(data, session, background_tasks)
    return JSONResponse({"message": "Account is activated successfully."})

@guest_router.post("/login", status_code=status.HTTP_200_OK, response_model=LoginResponse)
async def user_login(data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_arangodb_session)):
    try:
        return await user.get_login_token(data, db)
    except Exception as e:
        raise e

@guest_router.post("/refresh", status_code=status.HTTP_200_OK, response_model=LoginResponse)
async def refresh_token(refresh_token = Header(), session = Depends(get_arangodb_session)):
    return await user.get_refresh_token(refresh_token, session)


@guest_router.post("/forgot-password", status_code=status.HTTP_200_OK)
async def forgot_password(data: EmailRequest, background_tasks: BackgroundTasks, session:ArangoDBClient = Depends(get_arangodb_session)):
    await user.email_forgot_password_link(data, background_tasks, session)
    return JSONResponse({"message": "A email with password reset link has been sent to you."})

@guest_router.put("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(data: ResetRequest, session = Depends(get_arangodb_session)):
    await user.reset_user_password(data, session)
    return JSONResponse({"message": "Your password has been updated."})

@auth_router.get("/me", status_code=status.HTTP_200_OK, response_model=UserResponse)
async def fetch_user(user = Depends(get_current_user)):
    return user


@auth_router.get("/{uuid}", status_code=status.HTTP_200_OK, response_model=UserResponse)
async def get_user_info(uuid, session = Depends(get_arangodb_session)):
    return await user.fetch_user_detail(uuid, session)

@auth_router.get("", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_users(
        paging: Optional[str] = Query(None, alias="paging"),
        page_number: Optional[int] = Query(1, alias="page_number"),
        limit: Optional[int] = Query(10, alias="limit"), 
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session)
    ):
    return await user.fetch_users(
        paging=paging, 
        page_number=page_number, 
        limit=limit, 
        db=session
    )

@user_router.get("/privileges", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any)
async def get_privileges(
        privilege: Optional[str] = Query(None, alias="privilege"),
        exact: Optional[bool] = Query(False, alias="exact"),
        current_user = Depends(get_current_user),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_VIEW_PRIVILEGES])),
        session = Depends(get_arangodb_session)
    ):

    try:
        return ResponseMainModel(data=AccessPrivileges.get_privileges(privilege, exact), message="Privileges fetched successfully")
    except HTTPException as e:
        raise e

@user_router.get("/roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any)
async def get_roles(
        paging: Optional[str] = Query(None, alias="paging"),
        page_number: Optional[int] = Query(1, alias="page_number"),
        limit: Optional[int] = Query(10, alias="limit"), 
        current_user = Depends(get_current_user), 
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_VIEW_ROLES, AccessPrivileges.USERS_VIEW_PRIVILEGES])),
        session = Depends(get_arangodb_session)
    ):

    try:
        return await user.fetch_roles(
            paging=paging, 
            page_number=page_number, 
            limit=limit,
            filters = {},
            db=session
        )
    except HTTPException as e:
        raise e

@user_router.post("/roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any, description="Include uuid or the same name to update any role. Make sure to include all privileges during update as they are being replaced completely")
async def create_or_update_roles(
        data: RoleRequest,
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_CREATE_ROLES])),
    ):
    try:
        return await user.save_role(data = data, current_user = current_user, db=session)
    except HTTPException as e:
        raise e

@user_router.delete("/roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any, description="Submit list of role uuids")
async def delete_roles(
        data: List[str],
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_DELETE_ROLES])),
    ):
    try:
        return await user.delete_role(data = data, current_user = current_user, db=session)
    except HTTPException as e:
        raise e

@user_router.post("/assign-roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any, description="Use uuid's for user as well as roles, roles not included in roles list will be automatically unsassigned")
async def assign_roles(
        data: AssignRolesRequest,
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_ASSIGN_ROLES])),
    ):
    try:
        return await user.assign_roles(data = data, current_user = current_user, db=session)
    except HTTPException as e:
        raise e

@user_router.post("/unassign-roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any, description="Use uuid's for user as well as roles")
async def unassign_roles(
        data: AssignRolesRequest,
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_ASSIGN_ROLES, AccessPrivileges.USERS_VIEW_ROLES])),
    ):
    try:
        return await user.unassign_roles(data = data, current_user = current_user, db=session)
    except HTTPException as e:
        raise e

@user_router.get("/user-roles", status_code=status.HTTP_200_OK, response_model=ResponseMainModel | Any, description="provide user uuid if you wish to get specific user roles otherwise, current user is used by default")
async def get_users_roles(
        user_uuid: Union[str, None] = None,
        current_user = Depends(get_current_user), 
        session = Depends(get_arangodb_session),
        required_privs: List[str] = Depends(check_privileges([AccessPrivileges.USERS_VIEW_ROLES])),
    ):
    try:
        return await user.get_user_roles(user_uuid=user_uuid, current_user = current_user, db=session)
    except HTTPException as e:
        raise e

