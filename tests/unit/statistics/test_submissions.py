from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.statistics.services.submissions import (
    _compute_completeness,
    _compute_coverage_months,
    _compute_expected_deaths,
    _months_spanned,
    fetch_submissions_statistics,
)
from tests.support.fakes import FakeCursor, FakeDB


def _fake_config():
    return SimpleNamespace(
        field_mapping=SimpleNamespace(
            location_level1="region",
            location_level2="district",
            location_level3="ward",
            is_adult="isadult",
            is_child="ischild",
            is_neonate="isneonatal",
            death_date="id10023",
            submitted_date="today",
            interview_date="id10012",
            deceased_gender="id10019",
        )
    )


def _patch_config():
    async def fake_fetch_odk_config(db, *args, **kwargs):
        return _fake_config()
    return patch("app.statistics.services.submissions.fetch_odk_config", new=fake_fetch_odk_config)


class TestMonthsSpanned:
    def test_same_month_counts_as_one(self):
        from datetime import date
        assert _months_spanned(date(2024, 3, 5), date(2024, 3, 28)) == {2024: 1}

    def test_spans_a_full_calendar_year(self):
        from datetime import date
        result = _months_spanned(date(2024, 1, 1), date(2024, 12, 31))
        assert result == {2024: 12}

    def test_spans_across_a_year_boundary(self):
        from datetime import date
        # Nov 2024 through Feb 2025 -> Nov, Dec (2024) + Jan, Feb (2025)
        result = _months_spanned(date(2024, 11, 10), date(2025, 2, 3))
        assert result == {2024: 2, 2025: 2}


class TestComputeExpectedDeaths:
    def test_single_year_uses_that_years_annual_total_over_twelve(self):
        # 12 months of 2024 at an annual total of 1200 -> 100/month * 12
        expected = _compute_expected_deaths("2024-01-01", "2024-12-31", {"2023": 1100, "2024": 1200})
        assert expected == 100.0 * 12

    def test_missing_year_falls_back_to_the_average_of_available_years(self):
        # Only 2023 and 2025 on file; 2024 (all 12 months) uses their average.
        expected = _compute_expected_deaths(
            "2024-01-01", "2024-12-31", {"2023": 1200, "2025": 2400}
        )
        assert expected == 1800.0  # avg(1200,2400)=1800 annual -> *12/12

    def test_returns_none_when_the_unit_has_no_expected_deaths_data(self):
        assert _compute_expected_deaths("2024-01-01", "2024-12-31", {}) is None

    def test_returns_none_when_a_date_cannot_be_parsed(self):
        assert _compute_expected_deaths(None, "2024-12-31", {"2024": 1200}) is None
        assert _compute_expected_deaths("2024-01-01", "not-a-date", {"2024": 1200}) is None

    def test_tolerates_full_iso_datetime_strings(self):
        expected = _compute_expected_deaths(
            "2024-01-01T08:30:00Z", "2024-01-15T22:10:00Z", {"2024": 1200}
        )
        assert expected == 100.0  # one month touched

    def test_swaps_reversed_dates(self):
        expected = _compute_expected_deaths("2024-12-31", "2024-01-01", {"2024": 1200})
        assert expected == 1200.0

    def test_single_bare_total_period_is_used_for_every_month_touched(self):
        expected = _compute_expected_deaths("2024-01-01", "2024-02-28", {"total": 1200})
        assert expected == 200.0  # 2 months * (1200/12)

    def test_rounds_a_fractional_result_to_a_whole_number(self):
        # 5 months of a 1300 annual total -> 5 * (1300/12) = 541.66... -> 542.
        # Only completeness (a ratio) needs decimal precision, not this.
        expected = _compute_expected_deaths("2024-01-01", "2024-05-31", {"2024": 1300})
        assert expected == 542
        assert isinstance(expected, int)


class TestComputeCompleteness:
    def test_basic_percentage(self):
        assert _compute_completeness(50, 100.0) == 50.0

    def test_none_when_expected_is_none(self):
        assert _compute_completeness(50, None) is None

    def test_none_when_expected_is_zero(self):
        assert _compute_completeness(50, 0.0) is None

    def test_can_exceed_100_percent(self):
        assert _compute_completeness(150, 100.0) == 150.0

    def test_rounds_to_two_decimal_places(self):
        assert _compute_completeness(1, 3) == 33.33


class TestComputeCoverageMonths:
    def test_same_month_is_one(self):
        assert _compute_coverage_months("2024-03-05", "2024-03-28") == 1

    def test_spans_a_full_calendar_year(self):
        assert _compute_coverage_months("2024-01-01", "2024-12-31") == 12

    def test_spans_across_a_year_boundary(self):
        assert _compute_coverage_months("2024-11-10", "2025-02-03") == 4

    def test_swaps_reversed_dates(self):
        assert _compute_coverage_months("2024-12-31", "2024-01-01") == 12

    def test_returns_none_when_a_date_cannot_be_parsed(self):
        assert _compute_coverage_months(None, "2024-12-31") is None
        assert _compute_coverage_months("2024-01-01", "not-a-date") is None

    def test_populates_even_when_the_admin_unit_has_no_expected_deaths_data(self):
        # Coverage only needs the two dates - unlike expected/completeness,
        # it doesn't depend on an expected_deaths match for this unit.
        assert _compute_coverage_months("2024-01-01", "2024-06-15") == 6


class TestFetchSubmissionsStatisticsExpectedColumn:
    async def test_attaches_expected_and_completeness_per_row_at_the_grouped_level(self):
        rows = [
            {"region": "Dodoma", "district": "Kongwa_DC", "count": 60,
             "firstSubmission": "2024-01-01", "lastSubmission": "2024-12-31",
             "adults": 60, "children": 0, "neonates": 0, "male": 30, "female": 30},
        ]
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(list(rows)))

        with _patch_config(), \
             patch("app.statistics.services.submissions.get_expected_deaths_by_value",
                   new=AsyncMock(return_value={(2, "Kongwa_DC"): {"2024": 1200}})):
            result = await fetch_submissions_statistics.__wrapped__(current_user={}, group_level=2, db=fake_db)

        row = result.data[0]
        assert row["expected"] == 1200.0
        assert row["completeness"] == 5.0  # 60 / 1200 * 100
        assert row["coverage"] == 12

    async def test_row_with_no_matching_admin_unit_gets_none_expected_and_completeness(self):
        rows = [
            {"region": "Dodoma", "district": "Unmapped_DC", "count": 10,
             "firstSubmission": "2024-01-01", "lastSubmission": "2024-01-31",
             "adults": 10, "children": 0, "neonates": 0, "male": 5, "female": 5},
        ]
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(list(rows)))

        with _patch_config(), \
             patch("app.statistics.services.submissions.get_expected_deaths_by_value",
                   new=AsyncMock(return_value={})):
            result = await fetch_submissions_statistics.__wrapped__(current_user={}, group_level=2, db=fake_db)

        row = result.data[0]
        assert row["expected"] is None
        assert row["completeness"] is None
        # Coverage doesn't depend on an expected_deaths match, so it's still
        # populated even though this admin unit isn't mapped.
        assert row["coverage"] == 1

    async def test_looks_up_by_the_deepest_configured_group_level(self):
        # group_level=1 (region only) - the lookup must be keyed at level 1,
        # against the region's own raw value, not the district's.
        rows = [
            {"region": "Dodoma", "count": 5,
             "firstSubmission": "2024-01-01", "lastSubmission": "2024-01-31",
             "adults": 5, "children": 0, "neonates": 0, "male": 2, "female": 3},
        ]
        fake_db = FakeDB(responder=lambda query, bind_vars: FakeCursor(list(rows)))

        captured = {}

        async def fake_index(db):
            return {(1, "Dodoma"): {"2024": 120}}

        with _patch_config(), \
             patch("app.statistics.services.submissions.get_expected_deaths_by_value", new=fake_index):
            result = await fetch_submissions_statistics.__wrapped__(current_user={}, group_level=1, db=fake_db)

        assert result.data[0]["expected"] == 10.0  # 1 month * 120/12
