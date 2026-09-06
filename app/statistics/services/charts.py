from datetime import date
from typing import Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.settings.services.expected_deaths import get_expected_deaths_total_for_nodes
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import (
    build_location_limit_filter,
    build_locations_query_filter,
    parse_location_query_pairs,
)


def _resolve_location_nodes(locations_json, field_mapping):
    """Maps the dashboard's `locations` filter - raw field/value pairs like
    [("region", "Dar_es_Salaam")] - onto (level, value) pairs matching the
    expected_deaths hierarchy's own level numbering (1=top), using this
    deployment's field_mapping to know which raw field is which level. A
    pair for an unmapped field is dropped rather than raising, matching
    parse_location_query_pairs' own "bad filter shows everything"
    tolerance.
    """
    field_to_level = {}
    for level in range(1, 5):
        field = getattr(field_mapping, f"location_level{level}", None)
        if field:
            field_to_level[field] = level
    return [
        (field_to_level[field], value)
        for field, value in parse_location_query_pairs(locations_json)
        if field in field_to_level
    ]


from app.shared.utils.cache import ttl_cache
from fastapi_cache.decorator import cache


# @cache(namespace="charts_statistics", expire=3000) # Cache for 5 minutes
async def fetch_charts_statistics( current_user: dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[str] = None,  date_type:Optional[str]=None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        print("Fetching charts statistics")
        config = await fetch_odk_config(db, True)
        region_field = config.field_mapping.location_level1
        district_field = config.field_mapping.location_level2
        is_adult_field = config.field_mapping.is_adult
        is_child_field = config.field_mapping.is_child
        is_neonate_field = config.field_mapping.is_neonate
        #
        print(date_type)
        death_date = config.field_mapping.death_date 
        submitted_date = config.field_mapping.submitted_date 
        interview_date = config.field_mapping.interview_date 
        

        if date_type is not None:
            if date_type == 'submission_date':
                today_field = submitted_date
            elif date_type == 'death_date':
                today_field = death_date
            elif date_type == 'interview_date':
                today_field = interview_date
            else:
                today_field = submitted_date
        else:
            today_field = submitted_date

        deceased_gender = config.field_mapping.deceased_gender

        print(date_type, today_field, 'today_field')

        collection = db.collection(db_collections.VA_TABLE)   # Use the actual collection name here
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
            
            print(bind_vars)

        locations_filter = build_locations_query_filter(locations, bind_vars)
        if locations_filter:
            filters.append(locations_filter)

        filter_query = "FILTER " + " AND ".join(filters) + " " if filters else ""

        # location_level2 (district) isn't always mapped - some deployments
        # only configure admin level 1. Interpolating a blank field name as
        # `doc.` is invalid AQL and fails this whole combined query, taking
        # every chart on the dashboard down with it (not just the geo stats),
        # so district is only added to the query when it's actually mapped.
        geo_district_field = f", district: doc.{district_field}" if district_field else ""
        distinct_districts_expr = "LENGTH(geoData)" if district_field else "0"

        combined_query = f"""
            LET monthlySubmissions = (
                FOR doc IN {collection.name}
                {filter_query}
                COLLECT month = DATE_MONTH(DATE_TIMESTAMP(doc.{today_field})), year = DATE_YEAR(DATE_TIMESTAMP(doc.{today_field})) INTO grouped
                LET count = LENGTH(grouped)
                SORT year, month
                RETURN {{ month, year, count }}
            )

            LET distributionByAge = (
                LET ageGroups = [
                    {{ ageGroup: "adult", count: 0 }},
                    {{ ageGroup: "child", count: 0 }},
                    {{ ageGroup: "neonatal", count: 0 }}
                ]

                LET results = (
                   
                    FOR doc IN {collection.name}
                    {filter_query}
    LET ageGroup = FIRST(
        FOR key IN ["adult", "child", "neonatal"]
            FILTER (key == "adult" AND TO_STRING(doc.{is_adult_field}) == '1') OR
                   (key == "child" AND TO_STRING(doc.{is_child_field}) == '1') OR
                   (key == "neonatal" AND TO_STRING(doc.{is_neonate_field}) == '1')
            RETURN key
    ) || "unknown"

    COLLECT group = ageGroup WITH COUNT INTO count
    RETURN {{ group, count }}
                )

                RETURN MERGE(
                    FOR ageGroup IN ageGroups
                        LET matched = FIRST(FOR result IN results FILTER result.group == ageGroup.ageGroup RETURN result)
                        RETURN {{ [ageGroup.ageGroup]: matched != null ? matched.count : ageGroup.count }}
                )
            )

            LET genderDistribution = (
                LET genderGroups = [
                    {{ gender: "male", count: 0 }},
                    {{ gender: "female", count: 0 }},
                    {{ gender: "other", count: 0 }}
                ]

                LET genderResults = (
                    FOR doc IN {collection.name}
                    {filter_query}
                    LET gender = FIRST(
                        FOR key IN ["male", "female", "other"]
                            FILTER (key == "male" AND doc.{deceased_gender} == "male") OR 
                                (key == "female" AND doc.{deceased_gender} == "female") OR 
                                (key == "other" AND doc.{deceased_gender} == "other")
                            RETURN key
                    ) || "unknown"

                    COLLECT group = gender WITH COUNT INTO count
                    RETURN {{ group, count }}
                )

                RETURN MERGE(
                    FOR genderGroup IN genderGroups
                        LET matched = FIRST(FOR result IN genderResults FILTER result.group == genderGroup.gender RETURN result)
                        RETURN {{ [genderGroup.gender]: matched != null ? matched.count : genderGroup.count }}
                )
            )

            LET dataOverview = FIRST(
                FOR doc IN {collection.name}
                {filter_query}
                COLLECT AGGREGATE
                    total_count = COUNT(),
                    first_date  = MIN(doc.{today_field}),
                    last_date   = MAX(doc.{today_field})
                RETURN {{
                    total: total_count,
                    first_submission: first_date,
                    last_submission: last_date
                }}
            )

            LET geoData = (
                FOR doc IN {collection.name}
                {filter_query}
                RETURN DISTINCT {{ region: doc.{region_field}{geo_district_field} }}
            )
            LET distinctRegions   = LENGTH(UNIQUE(geoData[*].region))
            LET distinctDistricts = {distinct_districts_expr}

            RETURN {{
                monthly_submissions: monthlySubmissions,
                distribution_by_age: distributionByAge,
                gender_distribution: genderDistribution,
                data_overview: {{
                    total: dataOverview.total,
                    first_submission: dataOverview.first_submission,
                    last_submission: dataOverview.last_submission,
                    distinct_regions: distinctRegions,
                    distinct_districts: distinctDistricts
                }}
            }}
        """
        

        # print(combined_query)
        # Execute the combined query
        def execute_query():
            cursor = db.aql.execute(combined_query, bind_vars=bind_vars, cache=True)
            return cursor.next()

        result = await run_in_threadpool(execute_query)

        # A flat monthly target line for the Monthly Submissions chart: the
        # annual expected deaths for whichever period is most relevant,
        # divided by 12 - scoped to the dashboard's location filter when one
        # is applied (e.g. filtering to "Dar es Salaam" targets just that
        # region's own expected deaths, not the whole country), and to the
        # country-wide total (every top-level admin unit) otherwise. There's
        # no year filter on this chart yet (that's the planned global date
        # filter), so the most recent year with data is used as a stand-in
        # for "the current target" - falls back to a single-period ("total")
        # file, or None when no expected_deaths data has been imported at
        # all (or none matches the current location filter).
        location_nodes = _resolve_location_nodes(locations, config.field_mapping)
        totals_by_period = await get_expected_deaths_total_for_nodes(db, location_nodes)
        numeric_years = [p for p in totals_by_period if p.isdigit()]
        monthly_target = None
        if numeric_years:
            monthly_target = totals_by_period[max(numeric_years)] / 12
        elif "total" in totals_by_period:
            monthly_target = totals_by_period["total"] / 12

        monthly_submissions_data = result['monthly_submissions']
        distribution_by_age_data = result['distribution_by_age'][0]
        distribution_by_gender = result['gender_distribution'][0]
        data_overview = result.get('data_overview', {})

        # Structure the combined response
        response_data = {
            "monthly_submissions": monthly_submissions_data,
            "monthly_target": monthly_target,
            "distribution_by_age": {
                "neonates": distribution_by_age_data["neonatal"],
                "children": distribution_by_age_data["child"],
                "adults": distribution_by_age_data["adult"],
            },
            "distribution_by_gender": distribution_by_gender,
            "data_overview": data_overview,
        }

        return ResponseMainModel(
            data=response_data,
            message="Statistics fetched successfully",
            total=None
        )

    except Exception as e:
        print(e)
        return ResponseMainModel(
            data=None,
            message="Failed to fetch statistics",
            error=str(e),
            total=None
        )