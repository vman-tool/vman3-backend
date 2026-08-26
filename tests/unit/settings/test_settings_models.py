import pytest
from pydantic import ValidationError

from app.settings.models.settings import FieldMapping, SystemConfig


def _valid_system_config(**overrides):
    return {
        "app_name": "VMan3",
        "page_title": "Title",
        "page_subtitle": "Subtitle",
        "admin_level1": "Region",
        "admin_level2": "District",
        "admin_level3": "Ward",
        **overrides,
    }


class TestSystemConfig:
    def test_accepts_a_config_with_no_admin_level4_or_map_center(self):
        # Not every deployment configures a 4th admin level or a GPS map
        # center - both must be optional.
        config = SystemConfig(**_valid_system_config())

        assert config.admin_level4 is None
        assert config.map_center is None

    @pytest.mark.parametrize(
        "missing_field",
        ["app_name", "page_title", "page_subtitle", "admin_level1", "admin_level2", "admin_level3"],
    )
    def test_rejects_a_config_missing_any_of_the_six_required_fields(self, missing_field):
        data = _valid_system_config()
        del data[missing_field]

        with pytest.raises(ValidationError):
            SystemConfig(**data)

    def test_a_blank_string_satisfies_a_required_field(self):
        # Pydantic v2 note: a required `str` field only rejects a *missing*
        # or None value - an empty string "" still validates. The asterisks/
        # missing-field messaging in the frontend is what actually stops a
        # user from saving blank required fields; this model alone does not.
        config = SystemConfig(**_valid_system_config(app_name=""))

        assert config.app_name == ""


class TestFieldMapping:
    def _valid(self, **overrides):
        return {
            "instance_id": "instanceID",
            "va_id": "meta-instanceID",
            "consent_id": "",
            "location_level1": "region",
            "location_level2": "district",
            "deceased_gender": "",
            "is_adult": "isadult",
            "is_child": "ischild",
            "is_neonate": "isneonatal",
            "interviewer_name": "id10010",
            "interviewer_phone": "",
            "interviewer_sex": "",
            **overrides,
        }

    def test_no_longer_has_a_date_field(self):
        # Regression: "Today's Date" mapped ODK's `today` system field, which
        # is semantically the same as interview date - keeping both let a
        # form's today() and interview-date question map the same value
        # under two different labels. The field was removed from the model
        # entirely in favor of interview_date.
        assert "date" not in FieldMapping.model_fields

    def test_accepts_a_mapping_with_no_location_level_3_or_4(self):
        mapping = FieldMapping(**self._valid())

        assert mapping.location_level3 is None
        assert mapping.location_level4 is None

    def test_rejects_a_mapping_missing_instance_id(self):
        data = self._valid()
        del data["instance_id"]

        with pytest.raises(ValidationError):
            FieldMapping(**data)
