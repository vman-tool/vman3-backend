from app.data_quality.services.dqa_analytics_service import (
    _pool_ici,
    _pool_stat_blocks,
)
from app.data_quality.services.general_dqa import _location_snapshot_key


def _block(count, avg, stddev, p50, min_v, max_v):
    return {"avg": avg, "min_v": min_v, "max_v": max_v, "stddev": stddev, "p50": p50, "count": count}


class TestPoolStatBlocks:
    def test_single_group_is_exact_including_median(self):
        block = _block(count=10, avg=5.0, stddev=2.0, p50=4.5, min_v=1.0, max_v=9.0)

        result = _pool_stat_blocks([block])

        assert result["avg"] == 5.0
        assert result["stddev"] == 2.0
        assert result["p50"] == 4.5
        assert result["count"] == 10
        assert result["p50_approximate"] is False

    def test_two_groups_pool_mean_and_variance_exactly(self):
        # Hand-verified: group A n=2 [10,20] -> mean=15, var=25;
        # group B n=3 [5,5,5] -> mean=5, var=0. Raw combined [10,20,5,5,5]
        # -> mean=9, var=34 (population variance, ddof=0).
        a = _block(count=2, avg=15.0, stddev=5.0, p50=15.0, min_v=10.0, max_v=20.0)
        b = _block(count=3, avg=5.0, stddev=0.0, p50=5.0, min_v=5.0, max_v=5.0)

        result = _pool_stat_blocks([a, b])

        assert result["count"] == 5
        assert result["avg"] == 9.0
        assert result["stddev"] == 34 ** 0.5
        assert result["min_v"] == 5.0
        assert result["max_v"] == 20.0

    def test_multi_group_median_is_a_flagged_approximation(self):
        a = _block(count=2, avg=15.0, stddev=5.0, p50=15.0, min_v=10.0, max_v=20.0)
        b = _block(count=3, avg=5.0, stddev=0.0, p50=5.0, min_v=5.0, max_v=5.0)

        result = _pool_stat_blocks([a, b])

        # Count-weighted average of the two medians: (2*15 + 3*5) / 5 = 9.
        assert result["p50"] == 9.0
        assert result["p50_approximate"] is True

    def test_empty_blocks_are_skipped(self):
        a = _block(count=10, avg=5.0, stddev=1.0, p50=5.0, min_v=1.0, max_v=9.0)
        empty = {"avg": None, "min_v": None, "max_v": None, "stddev": None, "p50": None, "count": 0}

        result = _pool_stat_blocks([a, empty])

        assert result["count"] == 10
        assert result["p50_approximate"] is False

    def test_all_empty_returns_a_well_shaped_zero_block(self):
        empty = {"avg": None, "min_v": None, "max_v": None, "stddev": None, "p50": None, "count": 0}

        result = _pool_stat_blocks([empty, None])

        assert result["count"] == 0
        assert result["avg"] is None
        assert result["p50_approximate"] is False


class TestPoolIci:
    def test_sums_counts_and_rederives_percentage_exactly(self):
        doc_a = {
            "ici": {
                "checks_applied": ["rule1"],
                "interviewers": [{"interviewer": "Alice", "total": 10, "passed": 8, "errors": 2, "ici": 80.0}],
            }
        }
        doc_b = {
            "ici": {
                "checks_applied": ["rule1"],
                "interviewers": [{"interviewer": "Alice", "total": 5, "passed": 5, "errors": 0, "ici": 100.0}],
            }
        }

        result = _pool_ici([doc_a, doc_b])

        assert result["overall_total"] == 15
        assert result["overall_passed"] == 13
        assert result["overall_ici"] == (13 * 100.0 / 15)
        assert result["interviewers"][0]["interviewer"] == "Alice"
        assert result["interviewers"][0]["total"] == 15
        assert result["interviewers"][0]["passed"] == 13

    def test_merges_different_interviewers_across_docs(self):
        doc_a = {"ici": {"checks_applied": [], "interviewers": [
            {"interviewer": "Alice", "total": 10, "passed": 10, "errors": 0, "ici": 100.0},
        ]}}
        doc_b = {"ici": {"checks_applied": [], "interviewers": [
            {"interviewer": "Bob", "total": 4, "passed": 0, "errors": 4, "ici": 0.0},
        ]}}

        result = _pool_ici([doc_a, doc_b])

        names = {row["interviewer"] for row in result["interviewers"]}
        assert names == {"Alice", "Bob"}
        assert result["overall_total"] == 14

    def test_no_interviewers_returns_zeroed_shape(self):
        result = _pool_ici([{"ici": {"checks_applied": [], "interviewers": []}}])

        assert result["overall_total"] == 0
        assert result["overall_ici"] is None
        assert result["interviewers"] == []


class TestLocationSnapshotKey:
    def test_deterministic(self):
        assert _location_snapshot_key("id10005r", "Dodoma") == _location_snapshot_key("id10005r", "Dodoma")

    def test_different_values_produce_different_keys(self):
        assert _location_snapshot_key("id10005r", "Dodoma") != _location_snapshot_key("id10005r", "Dar es Salaam")

    def test_only_arango_safe_characters_even_for_free_text_values(self):
        key = _location_snapshot_key("id10005r", "Dar es Salaam / Zone-B")
        assert key.startswith("loc::")
        assert all(c.isalnum() or c in "_-:" for c in key)
