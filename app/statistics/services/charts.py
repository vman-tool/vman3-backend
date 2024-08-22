from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def fetch_charts_statistics(paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.VA_TABLE)   # Use the actual collection name here
        bind_vars = {}
        filters = []

        if start_date:
            filters.append("DATE_TIMESTAMP(doc.today) >= @start_date")
            bind_vars["start_date"] = str(start_date)

        if end_date:
            filters.append("DATE_TIMESTAMP(doc.today) <= @end_date")
            bind_vars["end_date"] = str(end_date)

        if locations:
            filters.append("doc.id10005r IN @locations")
            bind_vars["locations"] = locations

        filter_query = "FILTER " + " AND ".join(filters) + " " if filters else ""

        combined_query = f"""
            LET monthlySubmissions = (
              FOR doc IN {collection.name}
              {filter_query}
              COLLECT month = DATE_MONTH(DATE_TIMESTAMP(doc.today)), year = DATE_YEAR(DATE_TIMESTAMP(doc.today)) INTO grouped
              LET count = LENGTH(grouped)
              SORT year, month
              RETURN {{ month, year, count }}
            )

            LET distributionByAge = (
              FOR doc IN {collection.name}
              {filter_query}
              COLLECT ageGroup = doc.age_group INTO grouped
              LET count = LENGTH(grouped)
              RETURN {{ ageGroup, count }}
            )

            RETURN {{
              monthly_submissions: monthlySubmissions,
              distribution_by_age: distributionByAge
            }}
        """

        # Execute the combined query
        cursor = db.aql.execute(combined_query, bind_vars=bind_vars)
        result = cursor.next()

        monthly_submissions_data = result['monthly_submissions']
        distribution_by_age_data = {item['ageGroup']: item['count'] for item in result['distribution_by_age']}

        # Structure the combined response
        response_data = {
            "monthly_submissions": monthly_submissions_data,
            "distribution_by_age": {
                "neonates": distribution_by_age_data.get("neonates", 0),
                "children": distribution_by_age_data.get("children", 0),
                "adults": distribution_by_age_data.get("adults", 0)
            }
        }

        return ResponseMainModel(
            data=response_data,
            message="Statistics fetched successfully",
            total=None
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message="Failed to fetch statistics",
            error=str(e),
            total=None
        )