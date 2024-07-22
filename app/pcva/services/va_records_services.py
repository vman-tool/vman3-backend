from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import AssignedVA
from app.pcva.requests.va_request_classes import AssignVARequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass
from app.shared.configs.constants import db_collections
from app.shared.utils.database_utilities import record_exists
from app.users.models.user import User

async def fetch_va_records(paging: bool = True,  page_number: int = 1, page_size: int = 10, db: StandardDatabase = None):
    try:
        collection = db.collection(db_collections.SUBMISSIONS)
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
            
        if paging and page_number and page_size:
        
            query += "LIMIT @offset, @size RETURN doc"
        
            bind_vars.update({
                "offset": (page_number - 1) * page_size,
                "size": page_size
            })
        else:
            query += "RETURN doc"

        cursor = db.aql.execute(query, bind_vars=bind_vars)

        data = [document for document in cursor]
        
        
        return {
            "page_number": page_number,
            "page_size": page_size,
            "data": data
        } if paging else data
    
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
    

async def assign_va_service(va_records: AssignVARequestClass, user,  db: StandardDatabase = None):
    vaIds  = va_records.vaIds
    va_assignment_data = []
    for vaId in vaIds:
        va_data = {
            "vaId": vaId,
            "coder1": va_records.coder1,
            "coder2": va_records.coder2,
            "created_by": user["uuid"]
        }
        va_object = AssignedVA(**va_data)
        if not record_exists("form_submissions", custom_fields={"__id": vaId}, db = db):
            continue
        existing_va_assignment_data = await AssignedVA.get_many(
            filters= {
                "vaId": vaId
            },
            db = db
        )
        if existing_va_assignment_data:
            va_object.uuid = existing_va_assignment_data[0]["uuid"]
            saved_object = await va_object.update(user["uuid"], db = db)
        else:
            saved_object = await va_object.save(db = db)
        va_assignment_data.append(await AssignVAResponseClass.get_structured_assignment(assignment=saved_object, db = db))
    
    return va_assignment_data
    