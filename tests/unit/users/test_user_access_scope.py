from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.users.services.user import (
    create_or_update_user_account,
    fetch_user_detail,
    fetch_users,
)
from app.users.schemas.user import RegisterUserRequest
from tests.support.fakes import FakeDB

REGION_FIELD = "id10005r"


def _user(uuid, name="User", email=None, is_active=True):
    # UserResponse.id must parse as an int (it's the ArangoDB numeric _key,
    # distinct from the string `uuid`) - a numeric-looking string like "1"
    # satisfies pydantic's int coercion without needing a real per-test key.
    return {
        "uuid": uuid, "_key": str(abs(hash(uuid)) % 100000), "name": name, "email": email or f"{uuid}@example.com",
        "is_active": is_active, "created_at": "2026-01-01T00:00:00", "created_by": None, "image": None,
    }


class TestFetchUsersScope:
    async def test_unrestricted_viewer_keeps_the_existing_db_paginated_path_untouched(self):
        users = [_user("u1"), _user("u2")]
        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=users)) as get_many, \
             patch("app.users.services.user.User.count", new=AsyncMock(return_value=2)), \
             patch("app.users.services.user._fetch_roles_and_limits", new=AsyncMock(return_value={})):
            result = await fetch_users(paging=True, page_number=1, limit=10, current_user={}, db=FakeDB())

        assert result.total == 2
        assert len(result.data) == 2
        # Unrestricted path still delegates paging to the DB query, not Python.
        get_many.assert_awaited_once()
        assert get_many.await_args.kwargs.get("paging") is True

    async def test_restricted_viewer_only_sees_in_scope_users(self):
        all_users = [_user("in-scope"), _user("out-of-scope")]
        extras = {
            "in-scope": {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}},
            "out-of-scope": {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}},
        }
        current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        async def fake_in_scope(candidate_access_limit, scope_access_limit, db):
            return candidate_access_limit == extras["in-scope"]["access_limit"]

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=all_users)), \
             patch("app.users.services.user._fetch_roles_and_limits", new=AsyncMock(return_value=extras)), \
             patch("app.users.services.user.is_access_limit_within_scope", new=fake_in_scope):
            result = await fetch_users(current_user=current_user, db=FakeDB())

        assert [u.uuid for u in result.data] == ["in-scope"]
        assert result.total == 1

    async def test_restricted_viewer_pagination_applies_to_the_filtered_set(self):
        all_users = [_user(f"u{i}") for i in range(5)]
        # u0, u2, u4 in scope; u1, u3 not.
        extras = {u["uuid"]: {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma" if i % 2 == 0 else "Mtwara"}]}} for i, u in enumerate(all_users)}
        current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        async def fake_in_scope(candidate_access_limit, scope_access_limit, db):
            return candidate_access_limit["limit_by"][0]["value"] == "Dodoma"

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=all_users)), \
             patch("app.users.services.user._fetch_roles_and_limits", new=AsyncMock(return_value=extras)), \
             patch("app.users.services.user.is_access_limit_within_scope", new=fake_in_scope):
            result = await fetch_users(paging=True, page_number=2, limit=1, current_user=current_user, db=FakeDB())

        # 3 in-scope users (u0, u2, u4); page 2 of limit=1 is the 2nd one, u2.
        assert result.total == 3
        assert [u.uuid for u in result.data] == ["u2"]

    async def test_restricted_viewer_with_zero_in_scope_users_raises_not_found(self):
        all_users = [_user("out-of-scope")]
        extras = {"out-of-scope": {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}}}
        current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=all_users)), \
             patch("app.users.services.user._fetch_roles_and_limits", new=AsyncMock(return_value=extras)), \
             patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc_info:
                await fetch_users(current_user=current_user, db=FakeDB())

        assert exc_info.value.status_code == 400


class TestFetchUserDetailScope:
    async def test_unrestricted_viewer_can_view_any_user(self):
        with patch("app.users.services.user.User.get", new=AsyncMock(return_value=_user("u1"))):
            result = await fetch_user_detail("u1", current_user={}, db=FakeDB())
        assert result.uuid == "u1"

    async def test_restricted_viewer_is_denied_an_out_of_scope_target(self):
        current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
        target_limit = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}}

        with patch("app.users.services.user.User.get", new=AsyncMock(return_value=_user("u1"))), \
             patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock(return_value=[target_limit])), \
             patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=False)):
            with pytest.raises(HTTPException) as exc_info:
                await fetch_user_detail("u1", current_user=current_user, db=FakeDB())

        assert exc_info.value.status_code == 403

    async def test_restricted_viewer_can_view_an_in_scope_target(self):
        current_user = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
        target_limit = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        with patch("app.users.services.user.User.get", new=AsyncMock(return_value=_user("u1"))), \
             patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock(return_value=[target_limit])), \
             patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=True)):
            result = await fetch_user_detail("u1", current_user=current_user, db=FakeDB())

        assert result.uuid == "u1"


class TestUpdateUserScope:
    def _update_request(self, target_uuid):
        return RegisterUserRequest(uuid=target_uuid, name="Target", email="target@example.com")

    async def test_editing_an_out_of_scope_user_is_rejected_even_with_privileges(self):
        target_uuid = "target-uuid"
        target_doc = _user(target_uuid, email="target@example.com")
        current_user = {"id": "editor", "uuid": "editor", "access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
        target_limit = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Mtwara"}]}}

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=[target_doc])), \
             patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock(return_value=[target_limit])), \
             patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=False)), \
             patch("app.users.services.user.User.update", new=AsyncMock()) as update:
            with pytest.raises(HTTPException) as exc_info:
                await create_or_update_user_account(
                    data=self._update_request(target_uuid), current_user=current_user, db=FakeDB()
                )

        assert exc_info.value.status_code == 403
        update.assert_not_awaited()

    async def test_editing_an_in_scope_user_proceeds(self):
        target_uuid = "target-uuid"
        target_doc = _user(target_uuid, email="target@example.com")
        current_user = {"id": "editor", "uuid": "editor", "access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}
        target_limit = {"access_limit": {"limit_by": [{"field": REGION_FIELD, "value": "Dodoma"}]}}

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=[target_doc])), \
             patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock(return_value=[target_limit])), \
             patch("app.users.services.user.is_access_limit_within_scope", new=AsyncMock(return_value=True)), \
             patch("app.users.services.user.User.update", new=AsyncMock(return_value={"uuid": target_uuid})) as update:
            await create_or_update_user_account(
                data=self._update_request(target_uuid), current_user=current_user, db=FakeDB()
            )

        update.assert_awaited_once()

    async def test_unrestricted_editor_skips_the_scope_check_entirely(self):
        target_uuid = "target-uuid"
        target_doc = _user(target_uuid, email="target@example.com")
        current_user = {"id": "editor", "uuid": "editor"}  # no access_limit at all

        with patch("app.users.services.user.User.get_many", new=AsyncMock(return_value=[target_doc])), \
             patch("app.users.services.user.UserAccessLimit.get_many", new=AsyncMock()) as target_limit_lookup, \
             patch("app.users.services.user.User.update", new=AsyncMock(return_value={"uuid": target_uuid})) as update:
            await create_or_update_user_account(
                data=self._update_request(target_uuid), current_user=current_user, db=FakeDB()
            )

        target_limit_lookup.assert_not_awaited()
        update.assert_awaited_once()
