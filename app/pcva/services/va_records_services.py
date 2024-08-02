from typing import Dict
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import ICD10, AssignedVA, CodedVA
from app.pcva.requests.va_request_classes import AssignVARequestClass, CodeAssignedVARequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass, CodedVAResponseClass
from app.shared.configs.constants import db_collections
from app.shared.utils.database_utilities import record_exists, replace_object_values
from app.users.models.user import User

async def fetch_va_records(paging: bool = True,  page_number: int = 1, limit: int = 10, include_assignment: bool = False, db: StandardDatabase = None):
    va_table_collection = db.collection(db_collections.VA_TABLE)
    assignment_collection_exists = db.has_collection(db_collections.ASSIGNED_VA)
    bind_vars = {}
    
    try:
        if not include_assignment or not assignment_collection_exists:
            query = f"FOR doc IN {va_table_collection.name} "
            bind_vars = {}
                
            if paging and page_number and limit:
            
                query += "LIMIT @offset, @size RETURN doc"
            
                bind_vars.update({
                    "offset": (page_number - 1) * limit,
                    "size": limit
                })
            else:
                query += "RETURN doc"

            cursor = db.aql.execute(query, bind_vars=bind_vars)

            data = [document for document in cursor]
            
            
            return {
                "page_number": page_number,
                "limit": limit,
                "data": data
            } if paging else data

        else:
            query = f"""
                // Step 1: Get paginated va records
                LET paginatedVa = (
                    FOR v IN {db_collections.VA_TABLE}
                        
                """
            
            if paging and page_number and limit:
        
                query += """LIMIT @offset, @size RETURN v)"""
            
            else:
                query += "RETURN v"
                
            query += f"""
                // Step 2: Get the va IDs for the paginated results
                LET vaIds = (
                    FOR v IN paginatedVa
                        RETURN v.__id
                )

                // Step 3: Get assignments for the va IDs
                LET assignments = (
                    FOR a IN {db_collections.ASSIGNED_VA}
                        FILTER a.vaId IN vaIds AND a.is_deleted == false
                        RETURN {{
                            vaId: a.vaId,
                            coder: a.coder
                        }}
                )

                LET formattedAssignments = (
                    FOR a in assignments
                        FOR user in {db_collections.USERS}
                        FILTER user.uuid == a.coder
                        RETURN {{
                            vaId: a.vaId,
                            coder: {{
                                uuid: user.uuid,
                                name: user.name
                            }}
                        }}
                )

                // Step 4: Combine the paginated va records with their assignments
                LET vaWithAssignments = (
                    FOR v IN paginatedVa
                        LET vAssignments = (
                            FOR a IN formattedAssignments
                                FILTER a.vaId == v.__id
                                RETURN a
                        )
                        RETURN MERGE(v, {{ assignments: vAssignments }})
                )

                RETURN vaWithAssignments
            """

            if paging and page_number and limit:

                bind_vars.update({
                    "offset": (page_number - 1) * limit,
                    "size": limit
                })
            
            cursor = db.aql.execute(query, bind_vars=bind_vars)

            data = [document for document in cursor]
            
            return {
                "page_number": page_number,
                "limit": limit,
                "data": data[0]
            } if paging else data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
    

async def assign_va_service(va_records: AssignVARequestClass, user: User,  db: StandardDatabase = None):
    try:
        vaIds  = va_records.vaIds
        va_assignment_data = []
        for vaId in vaIds:
            if va_records.new_coder:
                va_data = {
                    "vaId": vaId,
                    "coder": va_records.new_coder
                }
            else:
                va_data = {
                    "vaId": vaId,
                    "coder": va_records.coder,
                    "created_by": user.uuid
                }
            va_object = AssignedVA(**va_data)
            is_valid_va = await record_exists(db_collections.VA_TABLE, custom_fields={"__id": vaId}, db = db)
            if not is_valid_va:
                continue
            existing_va_assignment_data = await AssignedVA.get_many(
                filters= {
                    "vaId": vaId,
                    "coder": va_records.coder
                },
                db = db
            )
            if existing_va_assignment_data:
                if len(existing_va_assignment_data) > 1:
                    raise HTTPException(status_code=400, detail=f"Multiple assignments found for this VA with this coder")
                if va_records.new_coder:
                    va_object.uuid = existing_va_assignment_data[0]["uuid"]
                    saved_object = await va_object.update(user.uuid, db = db)
                else:
                    raise HTTPException(status_code=400, detail=f"Include new coder Id to update existing assignment.")
            else:
                saved_object = await va_object.save(db = db)
            va_assignment_data.append(await AssignVAResponseClass.get_structured_assignment(assignment=saved_object, db = db))
        
        return va_assignment_data
    except Exception as e:
        raise e


async def get_va_assignment_service(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, filters: Dict = {}, user = None, db: StandardDatabase = None):
    
    try:
        assigned_vas = await AssignedVA.get_many(
            paging = paging, 
            page_number = page_number, 
            limit = limit, 
            filters = filters, 
            include_deleted = include_deleted, 
            db = db
        )
        
        return {
            "page_number": page_number,
            "limit": limit,
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
            if coded_va.intermediate3_cod:
                in_conditions["uuid"].append(coded_va.intermediate3_cod)
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
                    "coder": current_user.uuid
                }, 
                db = db
            )
            if is_user_assigned:
                # 5. Check if the record needs to be updated or inserted
                # existing_coded_va = await CodedVA.get_many(
                #     paging = False, 
                #     filters = {
                #         "assigned_va": coded_va.assigned_va,
                #         "created_by": current_user.uuid
                #     },
                #     db = db
                # )

                # if existing_coded_va and len(existing_coded_va) == 1:
                #     updated_va = replace_object_values(coded_va.model_dump(), existing_coded_va[0], force = True)
                #     saved_coded_va = await CodedVA(**updated_va).update(current_user.uuid, db)
                # else:
                coded_va_object = coded_va.model_dump()
                coded_va_object["created_by"] = current_user.uuid
                saved_coded_va = await CodedVA(**coded_va_object).save(db)
                return await CodedVAResponseClass.get_structured_codedVA(coded_va = saved_coded_va, db = db)
            else:
                raise HTTPException(status_code=400, detail="This VA was not assigned to this user")
        except Exception as e:
            raise e 

async def get_coded_va_service(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, filters: Dict = {}, user = None, db: StandardDatabase = None):
    
    coded_vas = await CodedVA.get_many(
        paging = paging, 
        page_number = page_number, 
        limit = limit, 
        filters = filters, 
        include_deleted = include_deleted, 
        db = db
    )
    try:
        
        return {
            "page_number": page_number,
            "limit": limit,
            "data": [await CodedVAResponseClass.get_structured_codedVA(coded_va=va, db = db) for va in coded_vas]
        } if paging else [await CodedVAResponseClass.get_structured_codedVA(coded_va=va, db = db) for va in coded_vas]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coded va: {e}")
    

async def get_concordants_va_service(user, db: StandardDatabase = None):
    query = f"""
        LET assigned_vas = (
            FOR doc in {db_collections.ASSIGNED_VA}
            FILTER doc.coder == @user_id
            RETURN doc
        )

        LET coded_assigned_vas = (
            FOR assigned_va in assigned_vas
              FOR coded_va in {db_collections.CODED_VA}
                FILTER coded_va.assigned_va == assigned_va.vaId
                SORT coded_va.datetime DESC
                COLLECT assigned_va_id = coded_va.assigned_va INTO grouped
                LET latest_coded_vas = (
                    FOR g IN grouped[*].coded_va
                        COLLECT created_by = g.created_by INTO coder_group
                        LET latest_record = FIRST(coder_group[* FILTER CURRENT.datetime == MAX(coder_group[*].datetime)])
                        RETURN latest_record.g
                )
                RETURN latest_coded_vas
        )

        
        FOR coded_assigned_va in coded_assigned_vas
            RETURN coded_assigned_va[*]
    """
    bind_vars = {
        "user_id": user["uuid"]
    }
    
    codedVAs = await CodedVA.run_custom_query(query, bind_vars, db)
    
    formattedCodedVA = []
    for codedVA_group in codedVAs:
        codedVA_group_temp = []
        for codedVA in codedVA_group:
            codedVA_group_temp.append(await CodedVAResponseClass.get_structured_codedVA(codedVA, db))
        formattedCodedVA.append(codedVA_group_temp)
    
    return formattedCodedVA
     
    