
from arango.database import StandardDatabase
from fastapi import APIRouter, BackgroundTasks, Depends, Header, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

# from sqlalchemy.orm import Session
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_session
from app.users.decorators.user import get_current_user
from app.users.responses.user import LoginResponse, UserResponse
from app.users.schemas.user import (EmailRequest, RegisterUserRequest,
                                    ResetRequest, VerifyUserRequest)
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
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

guest_router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
    responses={404: {"description": "Not found"}},
)


# @created_by_decorator
@auth_router.post("/", status_code=status.HTTP_201_CREATED, response_model=UserResponse)

async def register_user(data: RegisterUserRequest, background_tasks: BackgroundTasks, db: StandardDatabase = Depends(get_arangodb_session)):
    # data.created_by=creator['_key']
    return await user.create_user_account(data, db, background_tasks)

@user_router.post("/verify", status_code=status.HTTP_200_OK)
async def verify_user_account(data: VerifyUserRequest, background_tasks: BackgroundTasks, session = Depends(get_arangodb_session)):
    await user.activate_user_account(data, session, background_tasks)
    return JSONResponse({"message": "Account is activated successfully."})

@guest_router.post("/login", status_code=status.HTTP_200_OK, response_model=LoginResponse)
async def user_login(data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_arangodb_session)):
   
    return await user.get_login_token(data, db)

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


