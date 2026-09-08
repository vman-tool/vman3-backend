from app.pcva.services.target_cause_categories import (
    classify_cause,
    get_cause_category_lookup,
)
from tests.support.fakes import FakeDB


class _FakeAllCollection:
    """python-arango's Collection.all() - a minimal stand-in, since
    get_cause_category_lookup reads whole collections rather than issuing
    AQL, unlike everything else in this codebase's tests."""
    def __init__(self, docs):
        self._docs = docs

    def all(self):
        return list(self._docs)


class _FakeCollectionsDB:
    def __init__(self, icd10, categories, types):
        self._by_name = {
            "icd10": _FakeAllCollection(icd10),
            "icd10_category": _FakeAllCollection(categories),
            "icd10_category_type": _FakeAllCollection(types),
        }

    def collection(self, name):
        return self._by_name[name]


TYPES = [
    {"uuid": "type-communicable", "name": "Group I: Communicable"},
    {"uuid": "type-ncd", "name": "Group II: Non-Communicable"},
    {"uuid": "type-injury", "name": "Group III: Injuries"},
]

CATEGORIES = [
    {"uuid": "cat-infectious", "name": "Infectious and parasitic diseases", "type": "type-communicable"},
    {"uuid": "cat-circulatory", "name": "Diseases of the circulatory system", "type": "type-ncd"},
    {"uuid": "cat-external", "name": "External causes of death", "type": "type-injury"},
]

ICD10 = [
    {"uuid": "i1", "code": "VAs-01.01", "name": "Sepsis", "category": "cat-infectious"},
    {"uuid": "i2", "code": "VAs-01.05", "name": "Malaria", "category": "cat-infectious"},
    {"uuid": "i3", "code": "VAs-04.02", "name": "Stroke", "category": "cat-circulatory"},
    {"uuid": "i4", "code": "VAs-12.09", "name": "Assault", "category": "cat-external"},
    # Trailing-space codes ("VAs-99 ") appear in the real seed data for the
    # bare (non ".SS") groups - must still parse correctly.
    {"uuid": "i5", "code": "VAs-99 ", "name": "Cause of death unknown", "category": None},
]


def _fake_db():
    return _FakeCollectionsDB(ICD10, CATEGORIES, TYPES)


class TestGetCauseCategoryLookup:
    async def test_maps_each_group_code_to_its_major_and_broad_category(self):
        group_to_category, _ = await get_cause_category_lookup(_fake_db())

        assert group_to_category["01"] == ("Infectious and parasitic diseases", "Group I: Communicable")
        assert group_to_category["04"] == ("Diseases of the circulatory system", "Group II: Non-Communicable")
        assert group_to_category["12"] == ("External causes of death", "Group III: Injuries")

    async def test_skips_an_icd10_entry_with_no_matching_category(self):
        group_to_category, _ = await get_cause_category_lookup(_fake_db())

        assert "99" not in group_to_category

    async def test_category_names_list_covers_every_category(self):
        _, category_names = await get_cause_category_lookup(_fake_db())

        names = {name for name, _ in category_names}
        assert names == {
            "Infectious and parasitic diseases",
            "Diseases of the circulatory system",
            "External causes of death",
        }


class TestClassifyCause:
    async def test_matches_an_interva5_cause_by_its_who_group_number(self):
        lookup = await get_cause_category_lookup(_fake_db())

        major, broad = classify_cause("Stroke", lookup)

        assert major == "Diseases of the circulatory system"
        assert broad == "Group II: Non-Communicable"

    async def test_two_interva5_causes_in_the_same_group_share_a_category(self):
        lookup = await get_cause_category_lookup(_fake_db())

        assert classify_cause("Sepsis (non-obstetric)", lookup)[0] == "Infectious and parasitic diseases"
        assert classify_cause("Malaria", lookup)[0] == "Infectious and parasitic diseases"

    async def test_falls_back_to_a_keyword_match_for_a_vaml10_cluster_label(self):
        lookup = await get_cause_category_lookup(_fake_db())

        # VManML10's _display_cause() produces names like this, close in
        # wording to the target list's own category name.
        major, broad = classify_cause("External Causes of Death", lookup)

        assert major == "External causes of death"
        assert broad == "Group III: Injuries"

    async def test_returns_none_none_when_nothing_matches(self):
        lookup = await get_cause_category_lookup(_fake_db())

        assert classify_cause("Some Unmapped Cause", lookup) == (None, None)

    async def test_returns_none_none_for_a_blank_cause(self):
        lookup = await get_cause_category_lookup(_fake_db())

        assert classify_cause(None, lookup) == (None, None)
        assert classify_cause("", lookup) == (None, None)
