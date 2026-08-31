from types import SimpleNamespace
from unittest.mock import patch

from app.data_quality.services.general_dqa import fetch_ics_value_sample
from tests.support.fakes import FakeCursor, FakeDB


def _fake_field_mapping():
    # Deliberately has no `date` attribute - it was removed from the
    # FieldMapping model (see settings.py) since it duplicated
    # interview_date. If this function ever regresses to referencing
    # fm.date again, this fake raises AttributeError instead of silently
    # returning a stale value.
    return SimpleNamespace(
        instance_id="instanceID", va_id="meta-instanceID", consent_id="",
        location_level1="region", location_level2="district", deceased_gender="id10019",
        is_adult="isadult", is_child="ischild", is_neonate="isneonatal",
        interviewer_name="id10010", interviewer_phone="", interviewer_sex="",
        submitted_date=None, birth_date=None, death_date="id10023",
        interview_date="id10012", table_name=None,
    )


async def test_builds_the_excluded_field_list_without_a_date_field():
    fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor([]))

    async def fake_fetch_odk_config(db, *args, **kwargs):
        return SimpleNamespace(field_mapping=_fake_field_mapping())

    with patch(
        "app.data_quality.services.general_dqa.fetch_odk_config",
        new=fake_fetch_odk_config,
    ):
        await fetch_ics_value_sample(fake_db)

    excluded_fields = fake_db.aql.queries[0][1]["excluded_fields"]
    assert "instanceID" in excluded_fields
    assert "id10023" in excluded_fields  # death_date
    assert "id10012" in excluded_fields  # interview_date
    assert None not in excluded_fields   # unset optional fields aren't included as-is
