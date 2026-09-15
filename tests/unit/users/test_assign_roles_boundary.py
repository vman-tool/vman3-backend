from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.users.services.user import assign_roles
from app.users.schemas.user import AssignRolesRequest
from tests.support.fakes import FakeDB

REGION_FIELD = "id10005r"

# assign_roles had zero test coverage before this change (it was only
# exercised through its inline boundary-check code, now extracted into
# is_access_limit_within_scope - see test_location_scope.py for that
# algorithm's own thorough coverage). These tests lock in assign_roles'
# observable behavior - the specific 403s it raises and when - across that
# refactor, not the full role-assignment side effects unrelated to it.


def _patched_role_assignment_plumbing(existing_access_limit=None):
    """Mocks everything assign_roles touches before/after the boundary
    check, so a request with roles=None exercises only that check."""
    return (
        patch("app.users.services.user.Role.get_many", new=AsyncMock(return_value=[])),
        patch("app.users.services.user.record_exists", new=AsyncMock(return_value=True)),
        patch("app.users.services.user.UserRole.get_many", new=AsyncMock(return_value=[])),
        patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock(return_value=existing_access_limit or [])),
        patch("app.users.services.user.UserAccessLimit.save", new=AsyncMock()),
        patch("app.users.services.user.get_user_roles", new=AsyncMock(return_value=SimpleNamespace(data=[]))),
    )


async def test_a_restricted_creator_must_grant_at_least_one_location():
    current_user = {"uuid": "creator", "access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
    data = AssignRolesRequest(user="target-uuid", roles=None, access_limit=None)

    mocks = _patched_role_assignment_plumbing()
    with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5]:
        with pytest.raises(HTTPException) as exc_info:
            await assign_roles(data=data, current_user=current_user, current_user_privileges=[], db=FakeDB())

    assert exc_info.value.status_code == 403
    assert "at least one location" in exc_info.value.detail


async def test_a_restricted_creator_cannot_grant_access_outside_their_own_boundary():
    current_user = {"uuid": "creator", "access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
    data = AssignRolesRequest(
        user="target-uuid", roles=None,
        access_limit={"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]},
    )

    mocks = _patched_role_assignment_plumbing()
    with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5]:
        with patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc_info:
                await assign_roles(data=data, current_user=current_user, current_user_privileges=[], db=FakeDB())

    assert exc_info.value.status_code == 403
    assert "administrative boundary" in exc_info.value.detail


async def test_a_restricted_creator_can_grant_access_within_their_own_boundary():
    current_user = {"uuid": "creator", "access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
    data = AssignRolesRequest(
        user="target-uuid", roles=None,
        access_limit={"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]},
    )

    mocks = _patched_role_assignment_plumbing()
    with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5]:
        with patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=True)):
            result = await assign_roles(data=data, current_user=current_user, current_user_privileges=[], db=FakeDB())

    assert result.message == "Roles were successfully assigned!"


async def test_an_unrestricted_creator_can_grant_any_access_limit_including_none():
    current_user = {"uuid": "creator", "access_limit": None}
    data = AssignRolesRequest(user="target-uuid", roles=None, access_limit=None)

    mocks = _patched_role_assignment_plumbing()
    with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5]:
        with patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock()) as boundary_check:
            result = await assign_roles(data=data, current_user=current_user, current_user_privileges=[], db=FakeDB())

    boundary_check.assert_not_awaited()
    assert result.message == "Roles were successfully assigned!"
