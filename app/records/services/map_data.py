from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values


async def fetch_va_map_records(current_user:dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        locationKey = current_user['access_limit'].get('field', '')  # locationLevel1

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
            filters.append("doc.today >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append("doc.today <= @end_date")
            bind_vars["end_date"] = str(end_date)

        if locations:
            filters.append("doc.id10005r IN @locations")
            bind_vars["locations"] = locations

        if filters:
            query += " AND " + " AND ".join(filters) + " "

        # if paging and page_number and limit:
        #     query += "LIMIT @offset, @size "
        #     bind_vars.update({
        #         "offset": (page_number - 1) * limit,
        #         "size": limit
        #     })

        query += """
            RETURN {
                id: doc._key,
                location: doc.id10005r,
                coordinates: doc.coordinates,
                district: doc.id10005d,
                date: doc.id10012,
                interviewer: doc.id10007,
                deviceid: doc.deviceid
            }
        """

        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        data = [document for document in cursor]

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        total_records_cursor = db.aql.execute(count_query,cache=True)
        total_records = total_records_cursor.next()

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