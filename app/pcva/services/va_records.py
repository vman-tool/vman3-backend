from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.shared.configs.constants import db_collections

async def fetch_va_records(paging: bool = True,  page_number: int = 1, page_size: int = 10, db: StandardDatabase = None):
    try:
        collection = db.collection(db_collections.SUBMISSIONS)
        query = f"FOR doc IN {collection.name} "
            
        if paging and page_number and page_size:
            bind_vars = {}
        
            query += "LIMIT @offset, @size RETURN doc"
        
            bind_vars.update({
                "offset": (page_number - 1) * page_size,
                "size": page_size
            })


        cursor = db.aql.execute(query, bind_vars=bind_vars)

        data = [document for document in cursor]
        
        return {
            "page": page_number,
            "size": page_size,
            "data": data
        }
    
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")