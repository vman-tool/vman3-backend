from datetime import datetime
from io import BytesIO
from typing import Dict
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.responses import FileResponse, StreamingResponse

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

import pandas as pd
   


async def get_unassigned_va_service(paging: bool = True, page_number: int = 0, limit: int = 10, format_records: bool = True, coder: str = None, db: StandardDatabase = None):
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

        
        # TODO: Every instanceid field used has to come from settings since it's the unique VA ID identified in the VA Data records

        query = f"""
            LET result = (
                FOR doc IN {db_collections.VA_TABLE}
                    LET coders_ids = (
                        FOR va IN {db_collections.ASSIGNED_VA}
                        FILTER va.vaId == doc.instanceid 
                        AND va.is_deleted == false
                        RETURN va.coder
                    )

                    LET coders = (
                        FOR user IN {db_collections.USERS}
                        FILTER user.uuid IN coders_ids
                        RETURN {{
                            uuid: user.uuid,
                            name: user.name
                        }}
                    )
                    
                    LET assignmentCount = (
                        FOR va IN {db_collections.ASSIGNED_VA}
                        FILTER va.vaId == doc.instanceid 
                        AND va.is_deleted == false
                        COLLECT WITH COUNT INTO count
                        RETURN count
                    )[0]

                    FILTER doc.instanceid NOT IN (
                        FOR va IN {db_collections.ASSIGNED_VA}
                        FILTER { 'va.coder == @coder AND' if coder else ''} va.is_deleted == false
                        COLLECT vaId = va.vaId WITH COUNT INTO coderCount
                        FILTER coderCount <= @maximum_assignment
                        RETURN vaId
                    )
                    //{paginator}
                    RETURN MERGE(doc, {{assignments: assignmentCount , coders: coders}})
                )
            LET data = {'SLICE(result, @offset, @limit)' if paging else 'result'} 
                

            RETURN {{
                total: LENGTH(result),
                data: data
            }}
        """

        if coder:
            bind_vars.update({
                "coder": coder
            })
        bind_vars.update({
            "maximum_assignment": 2 # TODO: This number has to come from PCVA Settings
        })

        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        query_result = query_result.next()
        unassigned_data = [format_va_record(va, config) for va in query_result['data']]
        return ResponseMainModel(data=unassigned_data, message="Unassigned VAs fetched successfully!", total = query_result["total"], pager=Pager(page=page_number, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get unassigned va: {e}")

async def get_va_for_unnassignment_service(paging: bool = True, page_number: int = 0, limit: int = 10, format_records: bool = True, coder: str = None, db: StandardDatabase = None):
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

        
        # TODO: Every instanceid field used has to come from settings since it's the unique VA ID identified in the VA Data records

        query = f"""
            FOR doc IN form_submissions
                LET coders_ids = (
                    FOR va IN {db_collections.ASSIGNED_VA}
                    FILTER va.vaId == doc.instanceid AND va.coder != @coder
                    AND va.is_deleted == false
                    RETURN va.coder
                )
                
                LET coders = (
                    FOR user IN {db_collections.USERS}
                    FILTER user.uuid IN coders_ids
                    RETURN user.name
                )
                
                LET assignmentCount = (
                    FOR va IN {db_collections.ASSIGNED_VA}
                    FILTER va.vaId == doc.instanceid 
                    AND va.is_deleted == false
                    COLLECT WITH COUNT INTO count
                    RETURN count
                )[0]
                
                FILTER doc.instanceid IN (
                    FOR va IN {db_collections.ASSIGNED_VA}
                    FILTER va.coder == @coder AND va.is_deleted == false
                    AND va.vaId NOT IN (
                        FOR result IN {db_collections.PCVA_RESULTS}
                        FILTER result.created_by == @coder
                        RETURN result.{db_collections.ASSIGNED_VA}
                    )
                    COLLECT vaId = va.vaId WITH COUNT INTO coderCount
                    FILTER coderCount <= @maximum_assignment
                    RETURN vaId
                )
                RETURN MERGE(doc, {{assignments: assignmentCount , coders: coders}})
        """

        if coder:
            bind_vars.update({
                "coder": coder
            })
        bind_vars.update({
            "maximum_assignment": 2 # TODO: This number has to come from PCVA Settings
        })

        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        unassigned_data = [format_va_record(va, config) for va in query_result]
        return ResponseMainModel(data=unassigned_data, message="Unassigned VAs fetched successfully!", total = len(unassigned_data), pager=Pager(page=page_number, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get unassigned va: {e}")
    

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
            # if not coded_va.clinicalNotes:
            #     raise HTTPException(status_code=400, detail="Clinical Notes must be written.")
            
            # 2. Check all causes of dealths exists and grab their uuid values
            in_conditions = {
                "uuid": []
            }

            icdcount = 0
            if coded_va.frameA.a and coded_va.frameA.a not in in_conditions["uuid"]:
                in_conditions["uuid"].append(coded_va.frameA.a)
                icdcount += 1
            if coded_va.frameA.b and coded_va.frameA.b not in in_conditions["uuid"]:
                in_conditions["uuid"].append(coded_va.frameA.b)
                icdcount += 1
            if coded_va.frameA.c and coded_va.frameA.c not in in_conditions["uuid"]:
                in_conditions["uuid"].append(coded_va.frameA.c)
                icdcount += 1
            if coded_va.frameA.d and coded_va.frameA.d not in in_conditions["uuid"]:
                in_conditions["uuid"].append(coded_va.frameA.d)
                icdcount += 1

            if coded_va.frameA.contributories:
                for contributory in coded_va.frameA.contributories:
                    if contributory and contributory not in in_conditions["uuid"]:
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
                            COLLECT assignecount_assigned_vasd_va = va.assigned_va
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
    
async def get_coded_va_details(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, va: str = None, coder: str = None, include_history: bool = False, current_user = None, db: StandardDatabase = None):
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
                    FILTER va.assigned_va == @va AND va.created_by == @coder {'AND va.is_deleted == false' if not include_deleted else ''}
                    SORT va.datetime DESC
                    COLLECT assigned_va = va.assigned_va
                    INTO latest_records = va
                    {paginator}
                    RETURN {'latest_records[*]' if include_history else 'FIRST(latest_records)'}
            """

            bind_vars.update({
                "coder": coder,
            })
        
        else:
            query = f"""
                    FOR va IN {db_collections.PCVA_RESULTS}
                    FILTER va.assigned_va == @va {'AND va.is_deleted == false' if not include_deleted else ""}
                    SORT va.datetime DESC
                    {paginator}
                    RETURN va
            """

        bind_vars.update({
            "va": va,
        })
        coded_vas = await VManBaseModel.run_custom_query(query = query, bind_vars = bind_vars, db = db)

        coded_vas = coded_vas.next() if include_history else coded_vas

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
    

async def export_pcva_results(db: StandardDatabase = None):
    try:
        query = f"""
            LET coded_vas = (
                FOR doc IN {db_collections.PCVA_RESULTS}
                SORT doc.datetime DESC
                COLLECT created_by = doc.created_by, assigned_va = doc.assigned_va
                INTO grouped = doc
                RETURN FIRST(
                    FOR result IN grouped
                    LIMIT 1
                    
                    LET cause_a = (
                        FOR code IN {db_collections.ICD10}
                        FILTER code.uuid == result.frameA.a
                        RETURN CONCAT("(", code.code, ") ", code.name)
                    )[0]
                    LET cause_b = (
                        FOR code IN {db_collections.ICD10}
                        FILTER code.uuid == result.frameA.b
                        RETURN CONCAT("(", code.code, ") ", code.name)
                    )[0]
                    LET cause_c = (
                        FOR code IN {db_collections.ICD10}
                        FILTER code.uuid == result.frameA.c
                        RETURN CONCAT("(", code.code, ") ", code.name)
                    )[0]
                    LET cause_d = (
                        FOR code IN {db_collections.ICD10}
                        FILTER code.uuid == result.frameA.d
                        RETURN CONCAT("(", code.code, ") ", code.name)
                    )[0]
                    LET contributory_causes = (
                        FOR contrib_id IN result.frameA.contributories
                        FOR code IN {db_collections.ICD10}
                        FILTER code.uuid == contrib_id
                        RETURN CONCAT("(", code.code, ") ", code.name)
                    )
                    
                    RETURN {{
                        coder: result.created_by,
                        va: result.assigned_va,
                        cause_a: cause_a,
                        cause_b: cause_b,
                        cause_c: cause_c,
                        cause_d: cause_d,
                        contributory_causes: contributory_causes
                    }}
                )
            )
            
            FOR doc IN coded_vas
            COLLECT va = doc.va INTO coders = doc
            
            LET numbered_causes = (
                FOR coder IN coders
                    FOR i IN 1..LENGTH(coders)
                    
                    // For numbered contributory causes (Uncomment the following lines - Not good for export data or tabular view of the data)
                    //LET contributory_causes = (
                    //    FOR j IN 1..LENGTH(coder.contributory_causes)
                    //    RETURN {{
                    //        [CONCAT('coder', TO_STRING(i), '_contributory_cause_', TO_STRING(j))]: coder.contributory_causes[j-1]
                    //    }}
                    //)
                    
                    RETURN MERGE(
                    {{
                        [CONCAT('coder', TO_STRING(i), '_cause_a')]: coder.cause_a,
                        [CONCAT('coder', TO_STRING(i), '_cause_b')]: coder.cause_b,
                        [CONCAT('coder', TO_STRING(i), '_cause_c')]: coder.cause_c,
                        [CONCAT('coder', TO_STRING(i), '_cause_d')]: coder.cause_d,

                        // For numbered contributory causes (Remove this line)
                        [CONCAT('coder', TO_STRING(i), '_contributory_causes')]: CONCAT_SEPARATOR(", ", coder.contributory_causes)
                    }}

                    // For numbered contributory causes (Uncomment the following line)
                    // ,
                    // MERGE(contributory_causes[*])
                )
            )
            
            LET merged_result = MERGE(
                {{ assigned_va: va, coders: LENGTH(coders) }},
                MERGE(numbered_causes[*])
            )
            
            LET sorted_objects = (
                FOR key IN ATTRIBUTES(merged_result)
                SORT key ASC
                RETURN [key, merged_result[key]]
            )
            
            RETURN ZIP(
                sorted_objects[*][0],
                sorted_objects[*][1]
            )

        """

        results = await VManBaseModel.run_custom_query(
            query = query,
            db = db
        )
        results = [result for result in results]
        sorted_results = sorted(results, key=lambda x: x['coders'], reverse=True)
        columns = list(sorted_results[0].keys())

        df = pd.DataFrame(sorted_results, columns=columns)

        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        string_columns = df.select_dtypes(include=['object']).columns

        df[numeric_columns] = df[numeric_columns].fillna('')
        df[string_columns] = df[string_columns].fillna('')

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        output.seek(0) 

        headers = {"Content-Disposition": "attachment; filename=pcva_results.xlsx"}
        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export PCVA results: {e}")


