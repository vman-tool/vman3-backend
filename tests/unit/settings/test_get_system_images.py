from app.settings.services.odk_configs import get_system_images
from tests.support.fakes import FakeCursor, FakeDB


async def test_scopes_the_query_to_the_vman_config_document():
    # Regression: this used to be an unfiltered `FOR settings IN
    # system_configs RETURN settings.system_images`, scanning every
    # document in that collection - fine while 'vman_config' was the only
    # one, but other features (the DQA analytics scheduler) store their own
    # document there too under a different key. AQL's scan order over
    # unrelated documents isn't guaranteed, so this sometimes returned an
    # unrelated document's (absent) system_images as index 0 - which every
    # caller treats as "nothing configured", making a perfectly successful
    # image upload look like it silently reset everything.
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([{"favicon": "/uploads/f.ico"}]))

    # Bypass the @ttl_cache wrapper entirely - this is testing the query
    # itself, not the caching layer.
    await get_system_images.__wrapped__(db=fake_db)

    query = fake_db.aql.queries[0][0]
    assert 'FILTER settings._key == "vman_config"' in query
