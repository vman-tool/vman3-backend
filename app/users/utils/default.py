from datetime import datetime
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
from app.shared.configs.models import ResponseMainModel
from app.utilits.db_logger import db_logger, log_to_db

logger = logging.getLogger(__name__)
#@log_to_db(context="default_account_creation", log_args=True)
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
        if not len(existing_users) > 0:
            current_user = await user.create_or_update_user_account(data = user_data, db = db, background_tasks = BackgroundTasks())
        else:
            current_user = existing_users[0]

        if len(existing_users) > 0:

            logger.info("User created successfully")
        
        
        current_user['id'] = current_user['_key'] if '_key' in current_user else current_user['id'] if "id" in current_user else None
        await create_default_roles(User(**current_user))

        # gunicorn runs two workers, each of which runs this initialisation.
        # Against an empty database both check for the account, both find
        # nothing, and both create one - so a fresh deployment ends up with two
        # identical superusers. An established database hides it, because the
        # account already exists by the time either check runs.
        await _remove_duplicate_accounts(email, db)

    except Exception as e:
        logger.error(f"Error in default account creation: {e}")
        return
    
async def _remove_duplicate_accounts(email: str, db):
    """Collapse duplicate accounts for one email down to the earliest.

    Both workers run this, so the record kept has to be chosen the same way by
    each of them - the earliest created_at, and the lowest uuid to break a tie.
    Whichever process gets there first wins and the other finds nothing to do.

    The duplicate's role assignments go with it: leaving them behind would put
    user_roles rows in the same orphaned state this codebase has had to clean
    up elsewhere.
    """
    try:
        duplicates = list(db.aql.execute(
            """
            FOR u IN users
                FILTER u.email == @email
                SORT u.created_at ASC, u.uuid ASC
                RETURN u
            """,
            bind_vars={"email": email},
        ))
        if len(duplicates) < 2:
            return

        keep, extras = duplicates[0], duplicates[1:]
        for extra in extras:
            db.aql.execute(
                "FOR r IN user_roles FILTER r.user == @uuid REMOVE r IN user_roles",
                bind_vars={"uuid": extra["uuid"]},
            )
            db.collection("users").delete(extra["_key"], ignore_missing=True)

        logger.warning(
            f"Removed {len(extras)} duplicate account(s) for {email}; "
            f"kept {keep['uuid']}"
        )
    except Exception as exc:
        # Never let cleanup stop the application from starting.
        logger.error(f"Could not de-duplicate accounts for {email}: {exc}")


async def create_default_roles(current_user: User = None):
    try:
        db = None
        async for session in get_arangodb_session():
            
            db = session
            break  # Exit after the first yielded value

        if db is None:
            logger.error("Failed to get database session")
            return
        
        
        read_only_privs = AccessPrivileges.get_privileges(search_term="view")
        read_only_privs.extend([AccessPrivileges.USERS_UPDATE_USER, AccessPrivileges.USERS_DEACTIVATE_USER])

        roles = [
            RoleRequest(
                name = "superuser",
                privileges = AccessPrivileges.get_privileges()
            ),
            RoleRequest(
                name = "read_only",
                privileges = read_only_privs
            ),
            RoleRequest(
                name = "coder",
                privileges = [AccessPrivileges.PCVA_MODULE_ACCESS, AccessPrivileges.PCVA_CODE_VA, AccessPrivileges.PCVA_VIEW_VA_RECORDS, AccessPrivileges.USERS_VIEW_PRIVILEGES, AccessPrivileges.USERS_VIEW_ROLES]
            )
        ]
        for role in roles:
            filters = { "or_conditions": [ 
                {"name": role.name}
            ]}
            existing_roles = await Role.get_many(filters = filters, db=db)
            role_created : ResponseMainModel = None
            if len(existing_roles) > 0:
                if not (len(existing_roles[0]['privileges'] if 'privileges' in existing_roles[0] else []) == len(role.privileges)):
            
                    role_created = await user.save_role(data = role, current_user = current_user, db = db)
                    
                    if role_created.data is not None:
                        logger.info(f"Role {role_created.data.name} created successfully")
            else:
                role_created = await user.save_role(data = role, current_user = current_user, db = db)
                    
                if role_created.data is not None:
                    logger.info(f"Role {role_created.data.name} created successfully")

            if (role_created and role_created.data.name == 'superuser') or (len(existing_roles) > 0 and existing_roles[0]['name'] == 'superuser'):
                role_to_assign = role_created.data.uuid if role_created else existing_roles[0]['uuid']
                role_assignment_request = AssignRolesRequest(user = current_user.uuid, roles = [role_to_assign])
                user_role = await user.assign_roles(data = role_assignment_request, current_user = current_user, db = db)
                if user_role.data:
                    logger.info(f"User {current_user.name} assigned to superuser role successfully")
    except Exception as e:
        logger.error(f"Error in default roles creation: {e}")
        return