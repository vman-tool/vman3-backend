from arango.database import StandardDatabase

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values


async def get_unique_regions(db: StandardDatabase, current_user:dict):
    config = await fetch_odk_config(db)
    region_field = config.field_mapping.location_level1
    locationKey, locationLimitValues = get_location_limit_values(current_user)


        
    try:
        query = f"""
        FOR dt IN form_submissions
          {f'FILTER dt.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
               
          COLLECT uniqueRegion = dt.{region_field}
          RETURN uniqueRegion
        """

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