
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel, VManBaseModel


async def get_imports_va_by_import_detail(
    import_detail_uuid: str,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 10,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    try:
        paginator = ""
        bind_vars = {"import_detail_uuid": import_detail_uuid}

        if paging:
            paginator = "LIMIT @offset, @limit"
            offset = (page_number - 1) * limit
            bind_vars.update({"offset": offset, "limit": limit})

        query = f"""
            FOR doc IN {db_collections.IMPORTS_VA_TABLE}
            FILTER doc.detail_uuid == @import_detail_uuid
            {paginator}
            RETURN doc
        """

        va_records = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        records = [record for record in va_records]

        count_query = f"""
            FOR doc IN {db_collections.IMPORTS_VA_TABLE}
            FILTER doc.detail_uuid == @import_detail_uuid
            COLLECT WITH COUNT INTO length
            RETURN length
        """
        count_cursor = await VManBaseModel.run_custom_query(query=count_query, bind_vars={"import_detail_uuid": import_detail_uuid}, db=db)
        total = count_cursor.next() if count_cursor else len(records)

        return ResponseMainModel(
            data=records,
            total=total,
            message="VA records fetched successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch VA records: {e}")
