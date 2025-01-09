from datetime import datetime
from typing import Dict
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.pcva_models import ICD10, AssignedVA, CodedVA, PCVAResults
from app.pcva.requests.va_request_classes import AssignVARequestClass, CodeAssignedVARequestClass, PCVAResultsRequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass, CodedVAResponseClass, CoderResponseClass, PCVAResultsResponseClass, VAQuestionResponseClass
from app.shared.configs.constants import AccessPrivileges, db_collections
from app.shared.utils.database_utilities import add_query_filters, record_exists, replace_object_values
from app.users.models.user import User
from app.pcva.utilities.va_records_utils import format_va_record
from app.shared.configs.models import Pager, ResponseMainModel, VManBaseModel
from app.odk.models.questions_models import VA_Question
from app.settings.services.odk_configs import fetch_odk_config
from app.users.models.role import Role, UserRole
   

async def assign_va_service(va_records: AssignVARequestClass, user: User,  db: StandardDatabase = None):
    try:
        if not va_records.coder and not va_records.new_coder:
            raise HTTPException(status_code=400, detail=f"Coder or new coder must be specified")
        
        if va_records.coder or va_records.new_coder:
            is_valid_coder = True
            is_valid_new_coder = True

            if va_records.coder:
                is_valid_coder = await record_exists(db_collections.USERS, uuid = va_records.coder, db = db)
            if va_records.new_coder:
                is_valid_new_coder = await record_exists(db_collections.USERS, uuid = va_records.new_coder, db = db)
            
            if not (is_valid_coder and is_valid_new_coder):
                raise HTTPException(status_code=400, detail=f"Invalid {'new coder' if va_records.new_coder else 'coder'} specified")

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
                    "created_by": user.uuid,
                    "created_at": datetime.now().isoformat(),
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
            print("Does it exists: ", existing_va_assignment_data)
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
                print("Object saved successfully: ", saved_object)
            va_assignment_data.append(await AssignVAResponseClass.get_structured_assignment(assignment=saved_object, db = db))
        
        return va_assignment_data
    except Exception as e:
        raise e


async def get_va_assignment_service(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, filters: Dict = {}, user = None, db: StandardDatabase = None):
    
    try:
        config = await fetch_odk_config(db)

        offset = (page_number - 1) * limit if paging else 0
        
        query = ""
        paginator = ""
        bind_vars = {}
        if paging:
            paginator = f"LIMIT @offset, @limit"
            bind_vars.update({
                "offset": offset,
                "limit": limit
            })

        query = f"""
            LET count_assigned_vas = LENGTH(
                FOR va IN {db_collections.ASSIGNED_VA}
                    FILTER va.coder == @coder 
                    AND va.is_deleted == false 
                    AND (va.vaId NOT IN (
                        FOR coded_va IN {db_collections.PCVA_RESULTS}
                            FILTER coded_va.created_by == @coder 
                            AND coded_va.is_deleted == false
                            SORT coded_va.datetime DESC
                            COLLECT assigned_va = coded_va.assigned_va
                            INTO latest_records = coded_va.assigned_va
                            RETURN FIRST(latest_records)
                    ))
                    RETURN va.vaId
            )
            LET assigned_vas = (
                FOR va_record IN {db_collections.VA_TABLE}
                FILTER va_record.instanceid IN (
                    FOR va IN {db_collections.ASSIGNED_VA}
                        FILTER va.coder == @coder 
                        AND va.is_deleted == false 
                        AND (va.vaId NOT IN (
                            FOR coded_va IN {db_collections.PCVA_RESULTS}
                                FILTER coded_va.created_by == @coder 
                                AND coded_va.is_deleted == false
                                SORT coded_va.datetime DESC
                                COLLECT assigned_va = coded_va.assigned_va
                                INTO latest_records = coded_va.assigned_va
                                RETURN FIRST(latest_records)
                        ))
                        {paginator}
                        RETURN va.vaId
                )
                RETURN va_record
            )

            RETURN MERGE({{
                totalCount: count_assigned_vas,
                assigned_vas: assigned_vas
            }})
        """

        if len(filters.items()) > 0:
            for key, value in filters.items():
                bind_vars.update({key: value})
        

        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        query_data = query_result.next()
        assignments = [format_va_record(va, config) for va in query_data['assigned_vas']]
        
        return ResponseMainModel(data=assignments, message="Assignments fetched successfully!", total = query_data['totalCount'], pager=Pager(page=page_number, limit=limit))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get assigned va: {e}")
    

async def code_assigned_va_service(coded_va: PCVAResultsRequestClass = None, current_user: User = None, db: StandardDatabase = None):
        try:
            # 1. Check if va exists in the db and is already assigned
            is_va_existing = await record_exists(db_collections.VA_TABLE, custom_fields = { "__id": coded_va.assigned_va}, db = db)
            is_user_assigned = await record_exists(
                collection_name = db_collections.ASSIGNED_VA, 
                custom_fields= {
                    "vaId": coded_va.assigned_va,
                    "coder": current_user.uuid
                }, 
                db = db
            )
            if not is_va_existing:
                raise HTTPException(status_code=404, detail="VA does not exist.")
            if not is_user_assigned:
                raise HTTPException(status_code=400, detail="VA is not assigned to this user.")
            
            # Check if clinical notes are available (reserve code)
            # if not coded_va.clinical_notes:
            #     raise HTTPException(status_code=400, detail="Clinical Notes must be written.")
            
            # 2. Check all causes of dealths exists and grab their uuid values
            in_conditions = {
                "uuid": []
            }

            icdcount = 0
            if coded_va.frameA.a:
                in_conditions["uuid"].append(coded_va.frameA.a)
                icdcount += 1
            if coded_va.frameA.b:
                in_conditions["uuid"].append(coded_va.frameA.b)
                icdcount += 1
            if coded_va.frameA.c:
                in_conditions["uuid"].append(coded_va.frameA.c)
                icdcount += 1
            if coded_va.frameA.d:
                in_conditions["uuid"].append(coded_va.frameA.d)
                icdcount += 1

            if coded_va.frameA.contributories:
                for contributory in coded_va.frameA.contributories:
                    if contributory:
                        in_conditions["uuid"].append(contributory)
                        icdcount += 1

            in_conditions = {field: values for field, values in in_conditions.items() if values}
            
            icd10_query_filters = {
                "in_conditions": [in_conditions]
            }
                
            if icdcount == 0:
                raise HTTPException(status_code=400, detail="Kindly make sure to code this VA before submitting.")
            
            icd10_codes_count = await ICD10.count(filters=icd10_query_filters, db = db)
            
            
            if icd10_codes_count != icdcount:
                raise HTTPException(status_code=400, detail="Some ICD 10 Codes haven't been added in the system configurations.")
            

            # # 4. Check if the record needs to be updated or inserted (Reserved code)
            # existing_coded_va = await PCVAResults.get_many(
            #     paging = False, 
            #     filters = {
            #         "assigned_va": coded_va.assigned_va,
            #         "created_by": current_user.uuid
            #     },
            #     db = db
            # )

            # if existing_coded_va and len(existing_coded_va) == 1:
            #     updated_va = replace_object_values(coded_va.model_dump(), existing_coded_va[0], force = True)
            #     saved_coded_va = await PCVAResults(**updated_va).update(current_user.uuid, db)
            # else:
            coded_va_object = coded_va.model_dump()
            coded_va_object["created_by"] = current_user.uuid
            saved_coded_va = await PCVAResults(**coded_va_object).save(db)
            data = await PCVAResultsResponseClass.get_structured_codedVA(pcva_result = saved_coded_va, db = db)
            return ResponseMainModel(data = data, message = "VA Coded successfully!") 
        except Exception as e:
            raise e 

async def get_coded_va_service(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, va: str = None, coder: str = None, current_user = None, db: StandardDatabase = None):
    try:
    
        offset = (page_number - 1) * limit if paging else 0

        query = ""
        paginator = ""
        bind_vars = {}
        if paging:
            paginator = f"LIMIT @offset, @limit"
            bind_vars.update({
                "offset": offset,
                "limit": limit
            })
        if coder:
            query = f"""
                FOR va_record IN {db_collections.VA_TABLE}
                    FILTER va_record.instanceid IN (
                        FOR va IN {db_collections.PCVA_RESULTS}
                            FILTER va.created_by == @coder AND va.is_deleted == false
                            SORT va.datetime DESC
                            COLLECT assigned_va = va.assigned_va
                            INTO latest_records = va.assigned_va
                            {paginator}
                            RETURN FIRST(latest_records)
                    )
                    RETURN va_record
            """

            bind_vars.update({
                "coder": coder,
            })
        coded_vas = await VManBaseModel.run_custom_query(query = query, bind_vars = bind_vars, db = db)
        config = await fetch_odk_config(db)
        coded_data = [format_va_record(va, config) for va in coded_vas]
        return ResponseMainModel(data = coded_data, total=len(coded_data), message="Coded VAs fetched successfully!", pager=Pager(page=page_number, limit=limit)) 
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coded vas: {e}")
    
async def get_coded_va_details(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, va: str = None, coder: str = None, current_user = None, db: StandardDatabase = None):
    try:
    
        offset = (page_number - 1) * limit if paging else 0

        query = ""
        paginator = ""
        bind_vars = {}
        if paging:
            paginator = f"LIMIT @offset, @limit"
            bind_vars.update({
                "offset": offset,
                "limit": limit
            })
        

        if coder:
            query = f"""
                    FOR va IN {db_collections.PCVA_RESULTS}
                    FILTER va.assigned_va == @va va.created_by == @coder AND va.is_deleted == false
                    SORT va.datetime DESC
                    COLLECT assigned_va = va.assigned_va
                    INTO latest_records = va
                    {paginator}
                    RETURN FIRST(latest_records)
            """

            bind_vars.update({
                "coder": coder,
            })
        
        else:
            query = f"""
                    FOR va IN {db_collections.PCVA_RESULTS}
                    FILTER va.assigned_va == @va AND va.is_deleted == false
                    SORT va.datetime DESC
                    {paginator}
                    RETURN va
            """

        bind_vars.update({
            "va": va,
        })
        coded_vas = await VManBaseModel.run_custom_query(query = query, bind_vars = bind_vars, db = db)
        coded_data = [await PCVAResultsResponseClass.get_structured_codedVA(pcva_result = coded_va, db = db) for coded_va in coded_vas] 
        return ResponseMainModel(data = coded_data, total=len(coded_data), message="Coded VAs fetched successfully!", pager=Pager(page=page_number, limit=limit)) 
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coded vas: {e}")
    

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
    
    codedVAs = await VManBaseModel.run_custom_query(query, bind_vars, db)
    
    formattedCodedVA = []
    for codedVA_group in codedVAs:
        codedVA_group_temp = []
        for codedVA in codedVA_group:
            codedVA_group_temp.append(await CodedVAResponseClass.get_structured_codedVA(codedVA, db))
        formattedCodedVA.append(codedVA_group_temp)
    
    return formattedCodedVA

async def get_form_questions_service(filters: Dict = None, db: StandardDatabase = None):
    questions = await VA_Question.get_many(paging=False, filters=filters, db=db)

    questions = [VAQuestionResponseClass(**question).model_dump() for question in questions]
    questions = { question['name']: question for question in questions} if len(questions) else []
    
     
    return ResponseMainModel(data=questions, message="Questions fetched successfully", total=len(questions))


async def get_coders(
        paging: int = None,
        page_number: int = None,
        limit: int = None,
        include_deleted: bool = False,
        current_user: User = None,
        db: StandardDatabase = None
):
    try:
        coders_privileges = [
            AccessPrivileges.PCVA_VIEW_VA_RECORDS,
            AccessPrivileges.PCVA_CODE_VA
        ]

        offset = (page_number - 1) * limit if paging else 0
        
        query = f"""

            LET coder_role = (
                FOR doc IN {db_collections.ROLES}
                FILTER @coders_privileges ALL IN doc.privileges AND doc.is_deleted == false
                RETURN doc.uuid
            )


            LET coders_assigned = (
                FOR user_role IN {db_collections.USER_ROLES}
                FILTER user_role.role IN coder_role AND user_role.is_deleted == @include_deleted
                LIMIT @offset, @limit
                RETURN user_role.user
            )

            LET coders = (
                FOR user IN {db_collections.USERS}
                    FILTER user.uuid IN coders_assigned AND user.is_deleted == false
                    LET assigned_va_count = (
                        FOR va IN {db_collections.ASSIGNED_VA}
                            FILTER va.coder == user.uuid AND va.is_deleted == false
                            COLLECT WITH COUNT INTO count
                        RETURN count
                    )[0]
                    LET coded_va_count = LENGTH(
                        FOR va IN {db_collections.PCVA_RESULTS}
                            FILTER va.created_by == user.uuid AND va.is_deleted == false
                            COLLECT assigned_va = va.assigned_va
                            RETURN assigned_va
                    )
                    RETURN MERGE (user, {{
                        assigned_va: assigned_va_count,
                        coded_va: coded_va_count
                    }})
            )

            RETURN coders
        """
        
        params = {
            "coders_privileges": coders_privileges,
            "include_deleted": include_deleted,
            "limit": limit,
            "offset": offset
        }

        coders = await VManBaseModel.run_custom_query(
            query = query,
            bind_vars = params,
            db = db
        )
        
        coders_data = [CoderResponseClass(**coder) for coder in coders.next()]

        return ResponseMainModel(data = coders_data, total=len(coders_data), message="Coders fetched successfully!", pager=Pager(page=page_number, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coders: {e}")

