from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values


async def fetch_va_map_records(
    current_user: dict,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 1000000,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[List[str]] = None,
    date_type: Optional[str] = None,
    db: StandardDatabase = None
) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
          # locationLevel1
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1
        district_field = config.field_mapping.location_level2
        interviewer_field = config.field_mapping.interviewer_name


        today_field = config.field_mapping.date

        locationKey, locationLimitValues = get_location_limit_values(current_user)

        query = f"""
            FOR doc IN {collection.name}
            FILTER doc.coordinates != null AND LENGTH(doc.coordinates) == 3 
            
        
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
            query += " AND " + " AND ".join(filters) + " "

        if paging and limit and locations is None:
            query += f"LIMIT {limit} "
    

        query += f"""
        
            RETURN {{
            id: doc._key,
            location: doc.{region_field},
            coordinates: doc.coordinates,
            district: doc.{district_field},
            date: doc.{today_field},
            interviewer: doc.{interviewer_field},
            deviceid: doc.deviceid
            }}
        """

        def execute_map_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_map_query)
        
        
        # Remove duplicates based on coordinates, since it is useless in the frontend
        unique_data = {}
        for record in data:
            coordinates = tuple(record['coordinates'])
            if coordinates not in unique_data:
                unique_data[coordinates] = record
        data = list(unique_data.values())

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        
        def execute_map_count_query():
            total_records_cursor = db.aql.execute(count_query, cache=True)
            return total_records_cursor.next()

        total_records = await run_in_threadpool(execute_map_count_query)

        return ResponseMainModel(
            data=data,
            message="Records fetched successfully",
            total=total_records
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message="Failed to fetch records",
            error=str(e),
            total=None
        )