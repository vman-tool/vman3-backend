from typing import List
import inspect
from app.shared.configs.constants import AccessPrivileges


def validate_privileges(to_validate_privileges: List[str] = []):
    """
    Validate that list of given privileges are defined in AccessPrivileges class. One being missing the function returns false
    """
    defined_privileges = {
        value for name, value in inspect.getmembers(AccessPrivileges)
        if not name.startswith('__') and isinstance(value, str)
    }

    missing_privileges = [priv for priv in to_validate_privileges if priv not in defined_privileges]
    return False if len(missing_privileges) > 0 else True
