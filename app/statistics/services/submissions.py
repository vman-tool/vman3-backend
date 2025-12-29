from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values
from app.shared.utils.cache import ttl_cache


@ttl_cache(ttl=300, key_prefix='submissions_statistics')
async def fetch_submissions_statistics( current_user: dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None,date_type:Optional[str]=None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        region_field = config.field_mapping.location_level1
        district_field = config.field_mapping.location_level2
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


        locationKey, locationLimitValues = get_location_limit_values(current_user)

        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"""
            FOR doc IN {collection.name}
        """
        bind_vars = {}
        filters = []
        ## filter by location limits
        if locationLimitValues and locationKey:
            filters.append(f"doc.{locationKey} IN @locationValues")
            bind_vars["locationValues"] = locationLimitValues
        ##
        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)

        if locations:
            filters.append(f"doc.{region_field} IN @locations")
            bind_vars["locations"] = locations

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        query += f"""
             COLLECT region = doc.{region_field}, district = doc.{district_field} INTO grouped
            LET count = LENGTH(grouped)
            LET lastSubmission = MAX(grouped[*].doc.{today_field})
           
            // LET adults = grouped[*].doc.{is_adult_field} ? TO_NUMBER(grouped[*].doc.{is_adult_field}) : []
            // LET children = grouped[*].doc.{is_child_field} ? TO_NUMBER(grouped[*].doc.{is_child_field}) : []
            // LET neonates = grouped[*].doc.{is_neonte_field} ? TO_NUMBER(grouped[*].doc.{is_neonte_field}) : []

            LET children = LENGTH(FOR sub IN grouped[*].doc
                            FILTER sub.{is_child_field} == "1"
                            RETURN sub)
            LET adults = LENGTH(FOR sub IN grouped[*].doc
                            FILTER sub.{is_adult_field} == "1"
                            RETURN sub)
            LET neonates = LENGTH(FOR sub IN grouped[*].doc
                            FILTER sub.{is_neonte_field} == "1"
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
              region,
              district,
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
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
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