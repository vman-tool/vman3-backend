from datetime import datetime
from typing import Dict
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.pcva_models import ICD10, AssignedVA, CodedVA
from app.pcva.requests.va_request_classes import AssignVARequestClass, CodeAssignedVARequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass, CodedVAResponseClass, CoderResponseClass, VAQuestionResponseClass
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
        if not va_records.coder and not va_backend.records.new_coder:
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
        # assigned_vas = await AssignedVA.get_many(
        #     paging = paging, 
        #     page_number = page_number, 
        #     limit = limit, 
        #     filters = filters, 
        #     include_deleted = include_deleted, 
        #     db = db
        # )
        config = await fetch_odk_config(db)

        filters.update({
            "is_deleted": include_deleted 
        })

        bind_vars = {}

        filters_list, vars = add_query_filters(filters, {}, 'a')

        bind_vars.update(vars)

        filters_string = ""

        if filters_list:
            filters_string += " FILTER " + " AND ".join(filters_list)
        query = f"""
                // Step 1: Get paginated va assignments
                LET paginatedVaAssignments = (
                    FOR a IN {db_collections.ASSIGNED_VA}
                    {filters_string}   
                """
            
        if paging and page_number and limit:
    
            query += """LIMIT @offset, @size RETURN a)"""
        
        else:
            query += "RETURN a)"
            
        query += f"""
            LET totalCount = LENGTH(
                FOR a IN {db_collections.ASSIGNED_VA}
                {filters_string}
                RETURN 1
            )

            LET vaIds = (
                FOR v IN paginatedVaAssignments
                    RETURN v.vaId
            )

            LET vas = (
                FOR v IN {db_collections.VA_TABLE}
                    FILTER v.__id IN vaIds
                    RETURN v
            )

            LET assignmentsWithVa = (
                FOR a IN paginatedVaAssignments
                    LET assignedVas = (
                        FOR v IN vas
                            FILTER v.__id == a.vaId
                            RETURN v
                    )
                    RETURN MERGE(a, {{ vaId: assignedVas[0] }})
            )

            // FOR assignment IN assignmentsWithVa
            //     RETURN assignment

            RETURN {{
                totalCount: totalCount, 
                assignments: assignmentsWithVa
            }}
        """
        

        if paging and page_number and limit:

            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })
        



        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        assigned_vas = query_result.next()
        assignments = []
        for assignment in assigned_vas['assignments']:
            assignment["vaId"] = format_va_record(assignment["vaId"], config)
            assignments.append(await AssignVAResponseClass.get_structured_assignment(assignment=assignment, db = db))
        
        return ResponseMainModel(data=assignments, message="Assignments fetched successfully!", total = assigned_vas['totalCount'], pager=Pager(page=page_number, limit=limit))
    
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
                    LET coded_va_count = (
                        FOR va IN {db_collections.PCVA_RESULTS}
                            FILTER va.created_by == user.uuid AND va.is_deleted == false
                            COLLECT assigned_va = va.assigned_va WITH COUNT INTO count
                        RETURN count
                    )[0]
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

