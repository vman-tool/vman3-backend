from io import BytesIO
from unittest.mock import AsyncMock, patch

import pytest
from openpyxl import Workbook

from app.settings.services.expected_deaths import (
    _build_tree,
    _drop_empty_branches,
    get_expected_deaths_by_value,
    get_expected_deaths_total_for_nodes,
    get_expected_deaths_tree,
    get_total_expected_deaths_by_period,
    import_expected_deaths_from_xform,
    parse_expected_deaths_hierarchy,
    recompute_aggregates,
    update_expected_deaths_value,
)


def _workbook_bytes(choices_rows, sheet_names=("survey", "choices")) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    for name in sheet_names:
        wb.create_sheet(name)
    if "survey" in sheet_names:
        wb["survey"].append(["name", "type", "label::English(en)"])
        wb["survey"].append(["id10001", "text", "A question"])
    if "choices" in sheet_names:
        for row in choices_rows:
            wb["choices"].append(row)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


CHOICES_HEADER = [
    "list_name", "name", "label::English(en)", "parent",
    "expected_deaths::2023", "expected_deaths::2024",
]

SAMPLE_ROWS = [
    CHOICES_HEADER,
    ["region", "Arusha", "Arusha", None, None, None],
    ["region", "Dodoma", "Dodoma", None, None, None],
    ["district", "Arusha_DC", "Arusha District Council", "Arusha", None, None],
    ["district", "Kongwa_DC", "Kongwa District Council", "Dodoma", None, None],
    ["ward", "Sejeli_Ward", "Sejeli Ward", "Kongwa_DC", 100.4, 110],
    ["ward", "Zoissa_Ward", "Zoissa Ward", "Kongwa_DC", 200.6, 210],
]


class TestParseExpectedDeathsHierarchy:
    def test_returns_none_when_no_choices_sheet(self):
        content = _workbook_bytes([], sheet_names=("survey",))
        assert parse_expected_deaths_hierarchy(content) is None

    def test_returns_none_when_no_expected_deaths_columns(self):
        rows = [["list_name", "name", "label::English(en)", "parent"], ["region", "Arusha", "Arusha", None]]
        content = _workbook_bytes(rows)
        assert parse_expected_deaths_hierarchy(content) is None

    def test_returns_none_when_workbook_unreadable(self):
        assert parse_expected_deaths_hierarchy(b"not a real xlsx file") is None

    def test_builds_a_three_level_chain_linked_by_the_generic_parent_column(self):
        content = _workbook_bytes(SAMPLE_ROWS)
        nodes = parse_expected_deaths_hierarchy(content)

        assert nodes is not None
        by_value = {(n["list_name"], n["value"]): n for n in nodes}

        # Arusha_DC has no wards in the sample - it's pruned (empty branch),
        # but the region and Kongwa_DC (which does have wards) survive.
        assert ("region", "Dodoma") in by_value
        assert ("district", "Kongwa_DC") in by_value
        assert ("ward", "Sejeli_Ward") in by_value
        assert ("district", "Arusha_DC") not in by_value

        kongwa = by_value[("district", "Kongwa_DC")]
        dodoma = by_value[("region", "Dodoma")]
        assert kongwa["parent_key"] == dodoma["_key"]

        sejeli = by_value[("ward", "Sejeli_Ward")]
        assert sejeli["parent_key"] == kongwa["_key"]

    def test_reads_multiple_periods_and_rounds_to_integers(self):
        content = _workbook_bytes(SAMPLE_ROWS)
        nodes = parse_expected_deaths_hierarchy(content)
        sejeli = next(n for n in nodes if n["value"] == "Sejeli_Ward")

        # 100.4 -> 100, 110 stays 110 - decimals dropped, ints only.
        assert sejeli["raw_expected_deaths"] == {"2023": 100, "2024": 110}

    def test_supports_a_single_bare_expected_deaths_column_as_period_total(self):
        rows = [
            ["list_name", "name", "label::English(en)", "parent", "expected_deaths"],
            ["region", "Dodoma", "Dodoma", None, None],
            ["ward", "Sejeli_Ward", "Sejeli Ward", "Dodoma", 100],
        ]
        content = _workbook_bytes(rows)
        nodes = parse_expected_deaths_hierarchy(content)
        sejeli = next(n for n in nodes if n["value"] == "Sejeli_Ward")
        assert sejeli["raw_expected_deaths"] == {"total": 100}

    def test_same_named_placeholder_under_different_parents_are_kept_as_separate_nodes(self):
        # Each carries its own raw value directly, sidestepping the separate,
        # documented ambiguity of a *child* trying to point at one of two
        # same-named parents via a single shared value.
        rows = SAMPLE_ROWS + [
            ["district", "other", "Other", "Arusha", 5, 5],
            ["district", "other", "Other", "Dodoma", 7, 7],
        ]
        content = _workbook_bytes(rows)
        nodes = parse_expected_deaths_hierarchy(content)

        others = [n for n in nodes if n["value"] == "other"]
        assert len(others) == 2
        assert len({n["_key"] for n in others}) == 2


class TestDropEmptyBranches:
    def test_drops_a_leaf_with_no_value_in_any_period(self):
        nodes = [
            {"_key": "a", "level": 1, "parent_key": None, "raw_expected_deaths": {}},
            {"_key": "b", "level": 2, "parent_key": "a", "raw_expected_deaths": {}},
        ]
        result = _drop_empty_branches(nodes)
        assert result == []

    def test_keeps_an_ancestor_with_no_value_of_its_own_but_a_surviving_child(self):
        nodes = [
            {"_key": "a", "level": 1, "parent_key": None, "raw_expected_deaths": {}},
            {"_key": "b", "level": 2, "parent_key": "a", "raw_expected_deaths": {"2023": 5}},
        ]
        result = _drop_empty_branches(nodes)
        assert {n["_key"] for n in result} == {"a", "b"}

    def test_keeps_a_node_with_its_own_value_even_if_childless(self):
        nodes = [{"_key": "a", "level": 1, "parent_key": None, "raw_expected_deaths": {"2023": 5}}]
        assert _drop_empty_branches(nodes) == nodes


class TestRecomputeAggregates:
    def test_sums_children_bottom_up_per_period(self):
        by_key = {
            "region": {"_key": "region", "level": 1, "parent_key": None, "raw_expected_deaths": {}},
            "district": {"_key": "district", "level": 2, "parent_key": "region", "raw_expected_deaths": {}},
            "ward1": {"_key": "ward1", "level": 3, "parent_key": "district", "raw_expected_deaths": {"2023": 100, "2024": 110}},
            "ward2": {"_key": "ward2", "level": 3, "parent_key": "district", "raw_expected_deaths": {"2023": 200}},
        }
        recompute_aggregates(by_key)
        assert by_key["district"]["expected_deaths"] == {"2023": 300, "2024": 110}
        assert by_key["region"]["expected_deaths"] == {"2023": 300, "2024": 110}

    def test_a_nodes_own_raw_value_wins_over_summing_children(self):
        by_key = {
            "district": {"_key": "district", "level": 1, "parent_key": None, "raw_expected_deaths": {"2023": 999}},
            "ward1": {"_key": "ward1", "level": 2, "parent_key": "district", "raw_expected_deaths": {"2023": 100}},
        }
        recompute_aggregates(by_key)
        assert by_key["district"]["expected_deaths"] == {"2023": 999}

    def test_empty_input_does_not_raise(self):
        recompute_aggregates({})


class TestBuildTree:
    def test_nests_by_parent_key_and_flags_leaves(self):
        by_key = {
            "r": {"_key": "r", "level": 1, "parent_key": None, "value": "Dodoma", "label": "Dodoma", "expected_deaths": {"2023": 300}},
            "d": {"_key": "d", "level": 2, "parent_key": "r", "value": "Kongwa_DC", "label": "Kongwa District Council", "expected_deaths": {"2023": 300}},
        }
        tree = _build_tree(by_key)
        assert len(tree) == 1
        assert tree[0]["is_leaf"] is False
        assert tree[0]["children"][0]["is_leaf"] is True
        assert tree[0]["children"][0]["expected_deaths"] == {"2023": 300}


class TestImportExpectedDeathsFromXform:
    @pytest.mark.asyncio
    async def test_returns_none_when_workbook_has_no_expected_deaths_columns(self):
        content = _workbook_bytes([["list_name", "name", "label::English(en)", "parent"], ["region", "Arusha", "Arusha", None]])
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value={})), \
             patch("app.settings.services.expected_deaths._write_all", new=AsyncMock()) as write_mock:
            result = await import_expected_deaths_from_xform(content, db=None)

        assert result is None
        write_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_first_import_creates_surviving_nodes_and_reports_periods(self):
        content = _workbook_bytes(SAMPLE_ROWS)
        written = {}

        async def fake_write_all(db, docs):
            written.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value={})), \
             patch("app.settings.services.expected_deaths._write_all", new=AsyncMock(side_effect=fake_write_all)):
            result = await import_expected_deaths_from_xform(content, db=None)

        assert result["nodes_created"] == 4  # Dodoma, Kongwa_DC, Sejeli_Ward, Zoissa_Ward (Arusha_DC pruned)
        assert set(result["periods"]) == {"2023", "2024"}
        dodoma = next(d for d in written.values() if d["value"] == "Dodoma")
        # 100.4 rounds to 100, 200.6 rounds to 201 -> 301, not a naive 300.
        assert dodoma["expected_deaths"] == {"2023": 301, "2024": 320}

    @pytest.mark.asyncio
    async def test_reimport_preserves_an_existing_period_but_adds_a_genuinely_new_one(self):
        content = _workbook_bytes(SAMPLE_ROWS)
        nodes = parse_expected_deaths_hierarchy(content)
        sejeli = next(n for n in nodes if n["value"] == "Sejeli_Ward")

        existing = {n["_key"]: dict(n) for n in nodes}
        # Simulate a manual edit to the already-stored 2023 figure.
        existing[sejeli["_key"]]["raw_expected_deaths"] = {"2023": 999999}
        recompute_aggregates(existing)

        written = {}

        async def fake_write_all(db, docs):
            written.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=existing)), \
             patch("app.settings.services.expected_deaths._write_all", new=AsyncMock(side_effect=fake_write_all)):
            result = await import_expected_deaths_from_xform(content, db=None)

        assert result["nodes_created"] == 0
        # 2023 kept the manually-edited value; 2024 is new to this node, so it comes from the file.
        assert written[sejeli["_key"]]["raw_expected_deaths"] == {"2023": 999999, "2024": 110}

    @pytest.mark.asyncio
    async def test_a_genuinely_new_year_column_in_a_reupload_is_added_to_every_existing_node(self):
        # Exact user scenario: the same file is re-uploaded, unchanged except
        # for a brand new expected_deaths::2025 column with real numbers.
        v1_content = _workbook_bytes(SAMPLE_ROWS)

        store: dict = {}

        async def fake_fetch_all(db):
            return dict(store)

        async def fake_write_all(db, docs):
            store.clear()
            store.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=fake_fetch_all), \
             patch("app.settings.services.expected_deaths._write_all", new=fake_write_all):
            v1_result = await import_expected_deaths_from_xform(v1_content, db=None)

        assert set(v1_result["periods"]) == {"2023", "2024"}
        sejeli_key = next(k for k, d in store.items() if d["value"] == "Sejeli_Ward")
        assert "2025" not in store[sejeli_key]["raw_expected_deaths"]

        v2_rows = [row[:] for row in SAMPLE_ROWS]
        v2_rows[0] = CHOICES_HEADER + ["expected_deaths::2025"]
        # Sejeli_Ward row gains a 2025 figure; everything else is unchanged.
        for row in v2_rows[1:]:
            if row[1] == "Sejeli_Ward":
                row.append(130)
            else:
                row.append(None)
        v2_content = _workbook_bytes(v2_rows)

        with patch("app.settings.services.expected_deaths._fetch_all", new=fake_fetch_all), \
             patch("app.settings.services.expected_deaths._write_all", new=fake_write_all):
            v2_result = await import_expected_deaths_from_xform(v2_content, db=None)

        assert set(v2_result["periods"]) == {"2023", "2024", "2025"}
        assert v2_result["nodes_created"] == 0  # no new administrative units this round
        # Old periods survive untouched, and the new one is now present.
        assert store[sejeli_key]["raw_expected_deaths"] == {"2023": 100, "2024": 110, "2025": 130}
        # The new period rolls up through the ancestors too.
        kongwa_key = next(k for k, d in store.items() if d["value"] == "Kongwa_DC")
        dodoma_key = next(k for k, d in store.items() if d["value"] == "Dodoma")
        assert store[kongwa_key]["expected_deaths"]["2025"] == 130
        assert store[dodoma_key]["expected_deaths"]["2025"] == 130

    @pytest.mark.asyncio
    async def test_a_brand_new_administrative_unit_in_a_reupload_is_created(self):
        # V1: Arusha_DC has no wards at all, so per the empty-branch pruning
        # rule it is never stored.
        v1_content = _workbook_bytes(SAMPLE_ROWS)

        store: dict = {}

        async def fake_fetch_all(db):
            return dict(store)

        async def fake_write_all(db, docs):
            store.clear()
            store.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=fake_fetch_all), \
             patch("app.settings.services.expected_deaths._write_all", new=fake_write_all):
            await import_expected_deaths_from_xform(v1_content, db=None)

        assert not any(d["value"] == "Arusha_DC" for d in store.values())
        # Arusha (region) has no other district either, so it is pruned too.
        assert not any(d["value"] == "Arusha" for d in store.values())

        # V2: the same file, but Arusha_DC now has a brand new ward with data.
        v2_rows = SAMPLE_ROWS + [
            ["ward", "Themi_Ward", "Themi Ward", "Arusha_DC", 42, 45],
        ]
        v2_content = _workbook_bytes(v2_rows)

        with patch("app.settings.services.expected_deaths._fetch_all", new=fake_fetch_all), \
             patch("app.settings.services.expected_deaths._write_all", new=fake_write_all):
            v2_result = await import_expected_deaths_from_xform(v2_content, db=None)

        # Arusha (region), Arusha_DC (district), Themi_Ward - the whole chain
        # up from the new ward is created, not just the ward itself.
        assert v2_result["nodes_created"] == 3
        arusha_region = next(d for d in store.values() if d["value"] == "Arusha")
        arusha_dc = next(d for d in store.values() if d["value"] == "Arusha_DC")
        themi = next(d for d in store.values() if d["value"] == "Themi_Ward")
        assert arusha_dc["parent_key"] == arusha_region["_key"]
        assert themi["parent_key"] == arusha_dc["_key"]
        assert arusha_dc["expected_deaths"] == {"2023": 42, "2024": 45}
        assert arusha_region["expected_deaths"] == {"2023": 42, "2024": 45}
        # Existing Dodoma branch is untouched by the new Arusha_DC ward.
        dodoma = next(d for d in store.values() if d["value"] == "Dodoma")
        assert dodoma["expected_deaths"] == {"2023": 301, "2024": 320}


class TestGetExpectedDeathsTree:
    @pytest.mark.asyncio
    async def test_reports_unconfigured_when_nothing_has_been_imported(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value={})):
            result = await get_expected_deaths_tree(db=None)
        assert result.data["configured"] is False
        assert result.data["tree"] == []
        assert result.data["periods"] == []

    @pytest.mark.asyncio
    async def test_returns_the_built_tree_and_periods_when_data_exists(self):
        by_key = {
            "r": {"_key": "r", "level": 1, "parent_key": None, "value": "Dodoma", "label": "Dodoma", "expected_deaths": {"2023": 300, "2024": 320}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            result = await get_expected_deaths_tree(db=None)
        assert result.data["configured"] is True
        assert result.data["periods"] == ["2023", "2024"]


class TestUpdateExpectedDeathsValue:
    def _tree(self):
        return {
            "r": {"_key": "r", "level": 1, "parent_key": None, "value": "Dodoma", "label": "Dodoma", "raw_expected_deaths": {}},
            "d": {"_key": "d", "level": 2, "parent_key": "r", "value": "Kongwa_DC", "label": "Kongwa District Council", "raw_expected_deaths": {}},
            "w1": {"_key": "w1", "level": 3, "parent_key": "d", "value": "Sejeli_Ward", "label": "Sejeli Ward", "raw_expected_deaths": {"2023": 100}},
            "w2": {"_key": "w2", "level": 3, "parent_key": "d", "value": "Zoissa_Ward", "label": "Zoissa Ward", "raw_expected_deaths": {"2023": 200}},
        }

    @pytest.mark.asyncio
    async def test_updates_one_period_of_a_childless_node_and_recomputes_ancestors(self):
        by_key = self._tree()
        written = {}

        async def fake_write_all(db, docs):
            written.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)), \
             patch("app.settings.services.expected_deaths._write_all", new=AsyncMock(side_effect=fake_write_all)):
            result = await update_expected_deaths_value("w1", "2023", 500, db=None)

        assert written["w1"]["raw_expected_deaths"] == {"2023": 500}
        assert written["d"]["expected_deaths"] == {"2023": 700}   # 500 + 200
        assert written["r"]["expected_deaths"] == {"2023": 700}
        assert result.data["configured"] is True

    @pytest.mark.asyncio
    async def test_rounds_a_decimal_value_to_an_integer(self):
        by_key = self._tree()
        written = {}

        async def fake_write_all(db, docs):
            written.update({d["_key"]: d for d in docs})

        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)), \
             patch("app.settings.services.expected_deaths._write_all", new=AsyncMock(side_effect=fake_write_all)):
            await update_expected_deaths_value("w1", "2023", 500.7, db=None)

        assert written["w1"]["raw_expected_deaths"]["2023"] == 501

    @pytest.mark.asyncio
    async def test_rejects_a_negative_value(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=self._tree())):
            with pytest.raises(Exception):
                await update_expected_deaths_value("w1", "2023", -5, db=None)

    @pytest.mark.asyncio
    async def test_rejects_editing_a_node_that_has_children(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=self._tree())):
            with pytest.raises(Exception):
                await update_expected_deaths_value("d", "2023", 5, db=None)

    @pytest.mark.asyncio
    async def test_rejects_an_unknown_key(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=self._tree())):
            with pytest.raises(Exception):
                await update_expected_deaths_value("does-not-exist", "2023", 5, db=None)


class TestGetExpectedDeathsByValue:
    @pytest.mark.asyncio
    async def test_indexes_every_node_by_level_and_value(self):
        by_key = {
            "r": {"_key": "r", "level": 1, "value": "Dodoma", "expected_deaths": {"2023": 300}},
            "d": {"_key": "d", "level": 2, "value": "Kongwa_DC", "expected_deaths": {"2023": 300}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            index = await get_expected_deaths_by_value(db=None)

        assert index[(1, "Dodoma")] == {"2023": 300}
        assert index[(2, "Kongwa_DC")] == {"2023": 300}
        assert (2, "Dodoma") not in index  # wrong level for this value

    @pytest.mark.asyncio
    async def test_empty_collection_gives_an_empty_index(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value={})):
            index = await get_expected_deaths_by_value(db=None)
        assert index == {}


class TestGetTotalExpectedDeathsByPeriod:
    @pytest.mark.asyncio
    async def test_sums_only_top_level_nodes_per_period(self):
        by_key = {
            "r1": {"_key": "r1", "level": 1, "expected_deaths": {"2023": 300, "2024": 320}},
            "r2": {"_key": "r2", "level": 1, "expected_deaths": {"2023": 100}},
            # A level-2 node with its own total should NOT be double-counted
            # on top of its region's already-aggregated total.
            "d1": {"_key": "d1", "level": 2, "expected_deaths": {"2023": 300}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            totals = await get_total_expected_deaths_by_period(db=None)

        assert totals == {"2023": 400, "2024": 320}

    @pytest.mark.asyncio
    async def test_empty_collection_gives_empty_totals(self):
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value={})):
            totals = await get_total_expected_deaths_by_period(db=None)
        assert totals == {}


class TestGetExpectedDeathsTotalForNodes:
    @pytest.mark.asyncio
    async def test_sums_the_given_nodes_own_already_aggregated_totals(self):
        by_key = {
            "r1": {"_key": "r1", "level": 1, "value": "Dodoma", "expected_deaths": {"2023": 300, "2024": 320}},
            "r2": {"_key": "r2", "level": 1, "value": "Arusha", "expected_deaths": {"2023": 100}},
            "d1": {"_key": "d1", "level": 2, "value": "Kongwa_DC", "expected_deaths": {"2023": 50}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            # A region plus an unrelated district - not the region's own
            # child, so no double counting to worry about here.
            totals = await get_expected_deaths_total_for_nodes(db=None, nodes=[(1, "Dodoma"), (2, "Kongwa_DC")])

        assert totals == {"2023": 350, "2024": 320}

    @pytest.mark.asyncio
    async def test_falls_back_to_the_country_wide_total_when_no_nodes_given(self):
        by_key = {
            "r1": {"_key": "r1", "level": 1, "value": "Dodoma", "expected_deaths": {"2023": 300}},
            "r2": {"_key": "r2", "level": 1, "value": "Arusha", "expected_deaths": {"2023": 100}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            totals = await get_expected_deaths_total_for_nodes(db=None, nodes=[])

        assert totals == {"2023": 400}

    @pytest.mark.asyncio
    async def test_a_node_with_no_expected_deaths_match_contributes_nothing(self):
        by_key = {
            "r1": {"_key": "r1", "level": 1, "value": "Dodoma", "expected_deaths": {"2023": 300}},
        }
        with patch("app.settings.services.expected_deaths._fetch_all", new=AsyncMock(return_value=by_key)):
            totals = await get_expected_deaths_total_for_nodes(db=None, nodes=[(2, "Unmapped_DC")])

        assert totals == {}
