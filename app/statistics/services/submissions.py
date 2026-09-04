from datetime import date
from typing import Dict, Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.settings.services.expected_deaths import get_expected_deaths_by_value
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import build_location_limit_filter, build_locations_query_filter
from app.shared.utils.cache import ttl_cache


def _parse_date(value) -> Optional[date]:
    """Tolerant of a bare date or a full ISO datetime string - only the
    first 10 characters (YYYY-MM-DD) are ever used."""
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _months_spanned(first: date, last: date) -> Dict[int, int]:
    """Number of calendar months touched by [first, last], per year - a
    partial month at either end still counts as one whole month."""
    months: Dict[int, int] = {}
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        months[year] = months.get(year, 0) + 1
        month += 1
        if month > 12:
            month, year = 1, year + 1
    return months


def _compute_expected_deaths(first_raw, last_raw, expected_by_period: Dict[str, int]) -> Optional[int]:
    """Expected VA count over [first_raw, last_raw]: for every calendar
    month the range touches, that month's share (1/12) of its own year's
    expected deaths - falling back to the average of whichever years this
    admin unit *does* have, so a year missing from the upload doesn't zero
    out its months. None (shown as "-") when the range can't be parsed, or
    this unit has no expected_deaths data at all. Rounded to a whole number -
    only completeness (a ratio) needs decimal precision.
    """
    first, last = _parse_date(first_raw), _parse_date(last_raw)
    if not first or not last:
        return None
    if last < first:
        first, last = last, first

    months_by_year = _months_spanned(first, last)

    numeric_periods = {int(p): v for p, v in expected_by_period.items() if str(p).isdigit()}
    if not numeric_periods:
        if "total" in expected_by_period:
            total_months = sum(months_by_year.values())
            return round(total_months * (expected_by_period["total"] / 12))
        return None

    fallback_annual = sum(numeric_periods.values()) / len(numeric_periods)
    expected = sum(months * (numeric_periods.get(year, fallback_annual) / 12) for year, months in months_by_year.items())
    return round(expected)


def _compute_completeness(submitted: int, expected: Optional[float]) -> Optional[float]:
    if not expected:
        return None
    return round((submitted / expected) * 100, 2)


def _compute_coverage_months(first_raw, last_raw) -> Optional[int]:
    """Number of calendar months between the first and last record - the
    same "months touched" count that feeds _compute_expected_deaths, but
    exposed on its own since it only needs the two dates, not this admin
    unit's expected_deaths data (so it still populates for units with no
    matching expected_deaths node)."""
    first, last = _parse_date(first_raw), _parse_date(last_raw)
    if not first or not last:
        return None
    if last < first:
        first, last = last, first
    return sum(_months_spanned(first, last).values())


@ttl_cache(ttl=30)
async def fetch_submissions_statistics( current_user: dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[str] = None,date_type:Optional[str]=None, group_level: int = 2, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        region_field = config.field_mapping.location_level1
        district_field = config.field_mapping.location_level2
        ward_field = config.field_mapping.location_level3
        is_adult_field = config.field_mapping.is_adult
        is_child_field = config.field_mapping.is_child
        is_neonte_field = config.field_mapping.is_neonate
        death_date = config.field_mapping.death_date or 'id10023'
        submitted_date = config.field_mapping.submitted_date or 'today'or  'submissiondate'
        interview_date = config.field_mapping.interview_date or 'id10012'
            
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = submitted_date
            elif date_type == 'death_date':
                today_field = death_date
            elif date_type == 'interview_date':
                today_field = interview_date
            else:
                today_field = interview_date
        else:
            today_field = interview_date

        deceased_gender = config.field_mapping.deceased_gender


        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"""
            FOR doc IN {collection.name}
        """
        bind_vars = {}
        filters = []
        ## filter by location limits
        location_limit_filter = build_location_limit_filter(current_user, bind_vars)
        if location_limit_filter:
            filters.append(location_limit_filter)
        ##
        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)

        locations_filter = build_locations_query_filter(locations, bind_vars)
        if locations_filter:
            filters.append(locations_filter)

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        # group_level picks how many admin levels the table drills into - 1
        # (region only) through 3 (region/district/ward). A level is only
        # included if its field is actually mapped, so an unconfigured
        # location_level3 degrades to level 2 instead of interpolating an
        # empty field name into the query.
        group_fields = [("region", region_field)]
        if group_level >= 2 and district_field:
            group_fields.append(("district", district_field))
        if group_level >= 3 and ward_field:
            group_fields.append(("ward", ward_field))

        collect_clause = ", ".join(f"{alias} = doc.{field}" for alias, field in group_fields)
        return_group_fields = ",\n              ".join(alias for alias, _ in group_fields)

        query += f"""
             COLLECT {collect_clause} INTO grouped
            LET count = LENGTH(grouped)
            LET firstSubmission = MIN(grouped[*].doc.{today_field})
            LET lastSubmission = MAX(grouped[*].doc.{today_field})

            // LET adults = grouped[*].doc.{is_adult_field} ? TO_NUMBER(grouped[*].doc.{is_adult_field}) : []
            // LET children = grouped[*].doc.{is_child_field} ? TO_NUMBER(grouped[*].doc.{is_child_field}) : []
            // LET neonates = grouped[*].doc.{is_neonte_field} ? TO_NUMBER(grouped[*].doc.{is_neonte_field}) : []

            LET children = LENGTH(FOR sub IN grouped[*].doc
                            FILTER TO_STRING(sub.{is_child_field}) == "1"
                            RETURN sub)
            LET adults = LENGTH(FOR sub IN grouped[*].doc
                            FILTER TO_STRING(sub.{is_adult_field}) == "1"
                            RETURN sub)
            LET neonates = LENGTH(FOR sub IN grouped[*].doc
                            FILTER TO_STRING(sub.{is_neonte_field}) == "1"
                            RETURN sub)
            LET male = LENGTH(
              FOR sub IN grouped[*].doc
              FILTER sub.{deceased_gender} == "male"
              RETURN sub
            )
            LET female = LENGTH(
              FOR sub IN grouped[*].doc
              FILTER sub.{deceased_gender} == "female"
              RETURN sub
            )
            RETURN {{
              {return_group_fields},
              count,
              firstSubmission,
              lastSubmission,
              adults,
              children,
              neonates,
              male,
              female
            }}
        """

        # if paging and page_number and limit:
        #     query += "LIMIT @offset, @size "
        #     bind_vars.update({
        #         "offset": (page_number - 1) * limit,
        #         "size": limit
        #     })

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)

        # Expected VA count and completeness (submitted / expected * 100),
        # per row - looked up by (level, value) against whichever admin unit
        # this table is currently grouped down to (region/district/ward),
        # against the expected_deaths hierarchy imported under Settings >
        # Configuration > Data Dictionary > Expected Number of Deaths.
        expected_index = await get_expected_deaths_by_value(db)
        deepest_alias = group_fields[-1][0]
        deepest_level = len(group_fields)
        for row in data:
            expected_by_period = expected_index.get((deepest_level, row.get(deepest_alias)), {})
            expected = _compute_expected_deaths(row.get("firstSubmission"), row.get("lastSubmission"), expected_by_period)
            row["expected"] = expected
            row["completeness"] = _compute_completeness(row.get("count") or 0, expected)
            row["coverage"] = _compute_coverage_months(row.get("firstSubmission"), row.get("lastSubmission"))

        # # Fetch total count of documents
        # count_query = f"""
        #     FOR doc IN {collection.name}
        #     COLLECT region = doc.id10005r, district = doc.id10005d INTO grouped
        #     RETURN LENGTH(grouped)
        # """
        # total_records_cursor = db.aql.execute(count_query)
        # total_records = total_records_cursor.next()

        return ResponseMainModel(
            data=data,
            message="Records fetched successfully",
            # total=total_records
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message="Failed to fetch records",
            error=str(e),
            total=None
        )