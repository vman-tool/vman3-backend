from typing import Dict
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import ICD10, AssignedVA, CodedVA
from app.pcva.requests.va_request_classes import AssignVARequestClass, CodeAssignedVARequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass
from app.shared.configs.constants import db_collections
from app.shared.utils.database_utilities import record_exists, replace_object_values
from app.users.models.user import User

async def fetch_va_records(paging: bool = True,  page_number: int = 1, page_size: int = 10, db: StandardDatabase = None):
    try:
        collection = db.collection(db_collections.VA_TABLE)
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
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
    

async def assign_va_service(va_records: AssignVARequestClass, user,  db: StandardDatabase = None):
    try:
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
            is_valid_va = await record_exists(db_collections.VA_TABLE, custom_fields={"__id": vaId}, db = db)
            if not is_valid_va:
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to assign va: {e}")


async def get_va_assignment_service(paging: bool, page_number: int = None, page_size: int = None, include_deleted: bool = None, filters: Dict = {}, user = None, db: StandardDatabase = None):
    
    try:
        assigned_vas = await AssignedVA.get_many(paging, page_number, page_size, filters, include_deleted, db)
        
        return {
            "page_number": page_number,
            "page_size": page_size,
            "data": [await AssignVAResponseClass.get_structured_assignment(assignment=va, db = db) for va in assigned_vas]
        } if paging else [await AssignVAResponseClass.get_structured_assignment(assignment=va, db = db) for va in assigned_vas]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get assigned va: {e}")
    

async def code_assigned_va_service(coded_va: CodeAssignedVARequestClass = None, current_user: User = None, db: StandardDatabase = None):
        try:
            # 1. Check if va exists in the db and is already assigned
            is_va_existing = await record_exists(db_collections.VA_TABLE, custom_fields = { "__id": coded_va.assigned_va}, db = db)
            is_va_assigned = await record_exists(db_collections.ASSIGNED_VA, custom_fields = { "vaId": coded_va.assigned_va}, db = db)
            if not is_va_existing:
                raise HTTPException(status_code=404, detail="VA does not exist.")
            if not is_va_assigned:
                raise HTTPException(status_code=400, detail="VA is not assigned.")
            if not coded_va.clinical_notes:
                raise HTTPException(status_code=400, detail="Clinical Notes must be written.")
            
            # 2. Check all causes of dealths exists and grab their uuid values
            in_conditions = {
                "uuid": []
            }
            if coded_va.immediate_cod:
                in_conditions["uuid"].append(coded_va.immediate_cod)
            if coded_va.intermediate1_cod:
                in_conditions["uuid"].append(coded_va.intermediate1_cod)
            if coded_va.intermediate2_cod:
                in_conditions["uuid"].append(coded_va.intermediate2_cod)
            if coded_va.underlying_cod:
                in_conditions["uuid"].append(coded_va.underlying_cod)

            for contributory in coded_va.contributory_cod:
                if contributory:
                    in_conditions["uuid"].append(contributory)

            in_conditions = {field: values for field, values in in_conditions.items() if values}
            
            icd10_query_filters = {
                "in_conditions": in_conditions
            }
                
            # 3. Confirm existence of contributory cods before saving them as comma separated values
            icd10_codes = []
            if in_conditions["uuid"]:
                icd10_codes = await ICD10.get_many(paging = False, filters=icd10_query_filters, db = db)

            # 4. Confirm VA Assignment for this user
            is_user_assigned = await record_exists(
                collection_name = db_collections.ASSIGNED_VA, 
                custom_fields= {
                    "vaId": coded_va.assigned_va,
                    "or_conditions": [
                        {"coder1": current_user.uuid},
                        {"coder2": current_user.uuid}
                            
                    ]
                }, 
                db = db
            )
            if is_user_assigned:
                # 5. Check if the record needs to be updated or inserted
                existing_coded_va = await CodedVA.get_many(
                    paging = False, 
                    filters = {
                        "assigned_va": coded_va.assigned_va,
                        "created_by": current_user.uuid
                    },
                    db = db
                )

                if existing_coded_va and len(existing_coded_va) == 1:
                    updated_va = replace_object_values(coded_va.model_dump(), existing_coded_va[0], force = True)
                    saved_coded_va = await CodedVA(**updated_va).update(current_user.uuid, db)
                else:
                    coded_va_object = coded_va.model_dump()
                    coded_va_object["created_by"] = current_user.uuid
                    saved_coded_va = await CodedVA(**coded_va_object).save(db)
                return saved_coded_va
            else:
                raise HTTPException(status_code=400, detail="This VA was not assigned to this user")
        except Exception as e:
            raise e    
    