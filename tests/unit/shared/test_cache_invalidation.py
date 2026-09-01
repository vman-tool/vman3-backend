from unittest.mock import AsyncMock, patch

from app.shared.utils.cache import invalidate_cache


async def test_deletes_the_exact_key_with_no_prefix_prepended():
    # Regression: this used to delete f"{FastAPICache.get_prefix()}:{key}",
    # but ttl_cache's own key builder (specific_key_builder, when given an
    # explicit key_prefix) uses that key_prefix as the literal cache key
    # with no prefix applied - so the two never agreed on what a given
    # cache entry was actually called. Every invalidate_cache() call was a
    # silent no-op deleting a key nothing had ever written to, so a save
    # (e.g. uploading a new system image) never actually invalidated the
    # read cache - the next GET kept serving up-to-an-hour-stale data.
    mock_redis = AsyncMock()
    mock_backend = type("Backend", (), {"redis": mock_redis})()

    with patch("app.shared.utils.cache.FastAPICache.get_backend", return_value=mock_backend), \
         patch("app.shared.utils.cache.FastAPICache.get_prefix", return_value="vman_cache"):
        await invalidate_cache("system_images")

    mock_redis.delete.assert_awaited_once_with("system_images")
