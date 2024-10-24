from arango.database import StandardDatabase

from app.shared.configs.models import ResponseMainModel


def get_unique_regions(db: StandardDatabase):
    try:
        query = """
        FOR dt IN form_submissions
          COLLECT uniqueRegion = dt.id10005r
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