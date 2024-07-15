

import logging
from datetime import datetime, timedelta

from arango.database import StandardDatabase
from fastapi import BackgroundTasks, HTTPException
from pydantic import BaseModel

from app.shared.configs.constants import db_collections
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
from app.users.models.user import User
from app.users.responses.user import UserResponse
from app.users.schemas.user import RegisterUserRequest, VerifyUserRequest
from app.users.services.email import (
    send_account_activation_confirmation_email,
    send_password_reset_email,
)
from app.users.utils.string import unique_string

settings = get_settings()


async def create_user_account(data: RegisterUserRequest, db: StandardDatabase, background_tasks: BackgroundTasks):
    print(data)
    
    collection = db.collection(db_collections.USERS)

    cursor = cursor =collection.find({'email': data.email}, limit=1)
    result = [doc for doc in cursor]
    
    user_exist =  result
    if user_exist:
        raise HTTPException(status_code=400, detail="Email already exists.")

    if not is_password_strong_enough(data.password):
        raise HTTPException(status_code=400, detail="Please provide a strong password.")

    user_data = {
        "name": data.name,
        "email": data.email,
        "password": hash_password(data.password),
        "is_active": True, # TODOS: Change to false if you want to verify email first
        "verified_at":datetime.now().isoformat(), # TODOS: Change to None if you want to verify email first
        "created_by": data.created_by, # TODOS: Change to the user id of the user creating the account
    }
    User(**user_data).save(db)
    # await User.save(user_data, 'users')

    # collection.insert(user_data)
    
    # Retrieve the inserted user to refresh session data
    cursor = collection.find({'email': data.email}, limit=1)
    user = [doc for doc in cursor]

    # Account Verification Email
    # await send_account_verification_email(user[0], background_tasks=background_tasks) #todos
    return {
        "id": user[0]["_key"],
        "name": user[0]["name"],
        "email": user[0]["email"],
        "is_active": user[0]["is_active"],
        "created_at":user[0]["created_at"]
    }
    
    
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
    print(res)
    
    return res
async def get_refresh_token(refresh_token: str, db, settings):
    token_payload = get_token_payload(refresh_token, settings.SECRET_KEY, settings.JWT_ALGORITHM)
    if not token_payload:
        raise HTTPException(status_code=400, detail="Invalid Request.")

    refresh_key = token_payload.get('t')
    access_key = token_payload.get('a')
    user_id = str_decode(token_payload.get('sub'))

    collection = db.collection(db_collections.USER_TOKENS)
    user_token_cursor = await collection.find({
        'refresh_key': refresh_key,
        'access_key': access_key,
        'user_id': user_id,
        'expires_at': {'$gt': datetime.utcnow().isoformat()}
    }, limit=1).next()

    if not user_token_cursor:
        raise HTTPException(status_code=400, detail="Invalid Request.")

    user_token = user_token_cursor
    user_token['expires_at'] = datetime.utcnow().isoformat()
    await collection.update(user_token)

    return _generate_tokens(user_token['user'], db)


async def _generate_tokens(user, db: StandardDatabase):
  
    refresh_key = unique_string(100)
    access_key = unique_string(50)
    rt_expires = timedelta(minutes=settings.REFRESH_TOKEN_EXPIRE_MINUTES)

    user_token = {
        "user_id": user["_key"],
        "refresh_key": refresh_key,
        "access_key": access_key,
        "expires_at": (datetime.utcnow() + rt_expires).isoformat()
    }

    # collection = db.collection(db_collections.USER_TOKENS)
    # collection.insert(user_token)
    collection =  db.collection(db_collections.USER_TOKENS)
    insert_result =  collection.insert(user_token, return_new=True)
    inserted_user_token = insert_result["new"]

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
    user['updated_at'] = datetime.utcnow().isoformat()

    collection = db.collection(db_collections.USERS)
    await collection.update(user)

    # Notify user that password has been updated
    # Implement your notification logic here

    return {"msg": "Password updated successfully"}
    
    
async def fetch_user_detail(pk: str, db):
    collection = db.collection(db_collections.USERS)
    cursor =  collection.find({'_key': pk}, limit=1)
    user_cursor = [doc for doc in cursor]

    
    if user_cursor:
        user= user_cursor[0]
        return      UserResponse(
        id=user["_key"],
        name=user["name"],
        email=user["email"],
        is_active=user["is_active"],
        created_at=user.get("created_at")
    )
    raise HTTPException(status_code=400, detail="User does not exist.")

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