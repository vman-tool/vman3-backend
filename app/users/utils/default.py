import logging

from decouple import config
from fastapi import BackgroundTasks

from app.shared.configs.arangodb_db import get_arangodb_session
from app.users.models.user import User
from app.users.schemas.user import RegisterUserRequest
from app.users.services import user

logger = logging.getLogger(__name__)

async def default_account_creation( ):
    try:
        db = None
        async for session in get_arangodb_session():
            
            db = session
            break  # Exit after the first yielded value

        if db is None:
            logger.error("Failed to get database session")
            return
        name = config("DEFAULT_ACCOUNT_NAME", default="admin")
        email = config("DEFAULT_ACCOUNT_EMAIL", default="admin@vman.net")
        password = config("DEFAULT_ACCOUNT_PASSWORD", default="Welcome2vman#")
        user_data = RegisterUserRequest(name=name, email=email, password=password, created_by="system")

        # Check if email already exists
        existing_users = await User.get_many(filters={'email': user_data.email})
        email_exists = existing_users.count() > 0
        if email_exists:
            return

        # Create a new user
        await user.create_user_account(user_data, db, BackgroundTasks())

    except Exception as e:
        logger.error(f"Error in default account creation: {e}")
        return