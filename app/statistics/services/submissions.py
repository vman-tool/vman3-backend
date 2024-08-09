from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def fetch_submissions_statistics(paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"""
            FOR doc IN {collection.name}
        """
        bind_vars = {}
        filters = []

        if start_date:
            filters.append("doc.id10012 >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append("doc.id10012 <= @end_date")
            bind_vars["end_date"] = str(end_date)

        if locations:
            filters.append("doc.id10005r IN @locations")
            bind_vars["locations"] = locations

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        query += """
            COLLECT region = doc.id10005r, district = doc.id10005d INTO grouped
            LET count = LENGTH(grouped)
            LET lastSubmission = MAX(grouped[*].doc.id10012)
            LET adults = SUM(grouped[*].doc.isadult)
            LET children = SUM(grouped[*].doc.ischild)
            LET neonates = SUM(grouped[*].doc.isneonatal)
            LET male = LENGTH(
              FOR sub IN grouped[*].doc
              FILTER sub.id10010b == "male"
              RETURN sub
            )
            LET female = LENGTH(
              FOR sub IN grouped[*].doc
              FILTER sub.id10010b == "female"
              RETURN sub
            )
            RETURN {
              region,
              district,
              count,
              lastSubmission,
              adults,
              children,
              neonates,
              male,
              female
            }
        """

        # if paging and page_number and limit:
        #     query += "LIMIT @offset, @size "
        #     bind_vars.update({
        #         "offset": (page_number - 1) * limit,
        #         "size": limit
        #     })

        cursor = db.aql.execute(query, bind_vars=bind_vars)
        data = [document for document in cursor]

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