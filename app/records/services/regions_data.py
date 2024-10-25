from arango.database import StandardDatabase

from app.shared.configs.models import ResponseMainModel


def get_unique_regions(db: StandardDatabase, current_user:dict):
    locationKey=current_user['access_limit']['field'] or None ## locationLevel1

    # locationLimitValues =current_user['access_limit']['limit_by'] or None ## [{value: "value", label: "label"}]
    locationLimitValues = [item['value'] for item in current_user['access_limit']['limit_by']] if current_user['access_limit']['limit_by'] else None

        
    try:
        query = f"""
        FOR dt IN form_submissions
                  {f'FILTER dt.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
               
          COLLECT uniqueRegion = dt.id10005r
          RETURN uniqueRegion
        """
        print(query, "query")
        cursor = db.aql.execute(query,cache=True)
        unique_regions = [region for region in cursor]

        return ResponseMainModel(
            data=unique_regions,
            message="Records fetched successfully",
            total=None
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message=f"Failed to fetch unique regions: {e}",
            error=str(e),
            total=None
        )