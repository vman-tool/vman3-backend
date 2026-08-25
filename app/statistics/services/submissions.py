from datetime import date
from typing import Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import build_location_limit_filter, build_locations_query_filter
from app.shared.utils.cache import ttl_cache


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
                today_field = config.field_mapping.date 
        else:
            today_field = config.field_mapping.date 

            
        # today_field = config.field_mapping.date
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