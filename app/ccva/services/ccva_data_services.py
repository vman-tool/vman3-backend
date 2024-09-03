from datetime import date
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException


async def fetch_ccva_records(paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1

        today_field = config.field_mapping.date
        collection = db.collection(db_collections.INTERVA5)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

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

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += "RETURN doc"
        # print(query)
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        data = [document for document in cursor]

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        total_records_cursor = db.aql.execute(count_query)
        total_records = total_records_cursor.next()

        return ResponseMainModel(
            data=data,
            message="CCVA fetched successfully",
            total=total_records
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
    
    
    
    
    
async def fetch_ccva_processed(paging: bool = True, page_number: int = 1, limit: int = 1, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:

        collection = db.collection(db_collections.CCVA_RESULTS)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

        if start_date:
            filters.append("doc.range.start >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append("doc.range.end <= @end_date")
            bind_vars["end_date"] = str(end_date)

    

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "
        query += "SORT doc.created_at DESC "

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += "RETURN doc"
        print(query)
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        data = [document for document in cursor]
        print(data)

        # Fetch total count of documents
        

        return ResponseMainModel(
            data=data,
            message="Processed CCVA fetched successfully",
            total=None
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
        