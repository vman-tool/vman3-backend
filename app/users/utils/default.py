import logging

from decouple import config
from fastapi import BackgroundTasks

from app.shared.configs.arangodb import get_arangodb_session
from app.users.models.user import User
from app.users.schemas.user import AssignRolesRequest, RegisterUserRequest, RoleRequest
from app.users.services import user
from app.shared.configs.constants import AccessPrivileges
from app.users.models.role import Role
from app.users.responses.user import UserResponse

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
        password = config("DEFAULT_ACCOUNT_PASSWORD", default="Welcome2vman$")
        user_data = RegisterUserRequest(name=name, email=email, password = password, confirm_password=password, created_by="")
        
        # Check if email already exists
        existing_users = await User.get_many(filters={'email': user_data.email}, db=db)
        email_exists = len(existing_users) > 0
        if not email_exists:
            current_user = await user.create_or_update_user_account(data = user_data, db = db, background_tasks = BackgroundTasks())
        else:
            current_user = existing_users[0]
        

        if len(existing_users) > 0:
            logger.info("User created successfully")
        
        current_user['id'] = current_user['_key']
        await create_default_roles(UserResponse(**current_user))

    except Exception as e:
        logger.error(f"Error in default account creation: {e}")
        return
    
async def create_default_roles(current_user: UserResponse = None):
    try:
        db = None
        async for session in get_arangodb_session():
            
            db = session
            break  # Exit after the first yielded value

        if db is None:
            logger.error("Failed to get database session")
            return
        
        read_only_privs = AccessPrivileges.get_privileges(search_term="view")
        read_only_privs.extend([AccessPrivileges.USERS_UPDATE_USER,AccessPrivileges.USERS_DEACTIVATE_USER])

        roles = [
            RoleRequest(
                name = "superuser",
                privileges = AccessPrivileges.get_privileges()
            ),
            RoleRequest(
                name = "read_only",
                privileges = read_only_privs
            )
        ]
        for role in roles:
            filters = { "or_conditions": [ 
                {"name": role.name}
            ]}
            existing_role = await Role.get_many(filters = filters, db=db)
            if len(existing_role) > 0:
                continue
            role_created = await user.save_role(data = role, current_user = current_user, db = db)
            
            if role_created.data is not None:
                logger.info(f"Role {role_created.data.name} created successfully")
            

            
            if role_created.data.name == 'superuser':
                role_assignment_request = AssignRolesRequest(user = current_user.uuid, roles = [role_created.data.uuid])
                user_role = await user.assign_roles(data = role_assignment_request, current_user = current_user, db = db)
                if user_role.data:
                    logger.info(f"User {current_user.name} assigned to superuser role successfully")
    except Exception as e:
        logger.error(f"Error in default roles creation: {e}")
        return