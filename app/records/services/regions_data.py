from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import build_location_limit_filter


from app.shared.utils.cache import ttl_cache

# @ttl_cache(ttl=300, key_prefix="unique_regions") # Cache for 5 mins
async def get_unique_regions(db: StandardDatabase, current_user:dict):
    config = await fetch_odk_config(db)
    region_field = config.field_mapping.location_level1

    try:
        bind_vars = {}
        location_filter = ''
        location_limit_filter = build_location_limit_filter(current_user, bind_vars, alias='dt', case_insensitive=True)
        if location_limit_filter:
            location_filter = f'FILTER {location_limit_filter}'

        query = f"""
        FOR dt IN form_submissions
          {location_filter}
          FILTER dt.{region_field} != null AND dt.{region_field} != ""
          COLLECT uniqueRegion = dt.{region_field}
          RETURN uniqueRegion
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            return [region for region in cursor]

        unique_regions = await run_in_threadpool(execute_query)

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