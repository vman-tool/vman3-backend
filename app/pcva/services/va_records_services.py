from datetime import datetime
from io import BytesIO
import json
from typing import Dict
from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.pcva.models.pcva_models import ICD10, AssignedVA, PCVAConfigurations, PCVAMessages, PCVAResults
from app.pcva.requests.va_request_classes import AssignVARequestClass, PCVAResultsRequestClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass, CoderResponseClass, PCVAResultsResponseClass, VAQuestionResponseClass
from app.shared.configs.constants import AccessPrivileges, data_sources, db_collections
from app.shared.utils.database_utilities import record_exists, replace_object_values
from app.users.models.user import User
from app.pcva.utilities.va_records_utils import format_va_record, get_categorised_pcva_results
from app.shared.configs.models import Pager, ResponseMainModel, VManBaseModel
from app.odk.models.questions_models import VA_Question
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.services.va_records import get_distinct_va_field_names

import pandas as pd

from app.pcva.requests.configurations_request_classes import PCVAConfigurationsRequest
from app.pcva.utilities.pcva_utils import fetch_pcva_settings
from app.shared.utils.response import populate_user_fields
   


async def get_unassigned_va_service(
    paging: bool = True,
    page_number: int = 0,
    limit: int = 10,
    format_records: bool = True,
    coder: str = None,
    location: str = None,
    start_date: str = None,
    end_date: str = None,
    data_source: str = None,
    assigned_to: str = None,
    db: StandardDatabase = None,
):
    """Unassigned VA records, optionally narrowed before assignment.

    The filters exist so a coder can be given a meaningful batch - one
    district, one week, one import, or the workload of another coder - rather
    than whatever happens to sit on the first page. All are optional; omitting
    them returns everything, which is the previous behaviour.

    Location and date filter on the fields named in the ODK field mapping, not
    on hard-coded column names, so they follow whatever the deployment has
    configured.

    :param coder: the coder being assigned to. Records they already hold are
        excluded regardless of any filter, which is what stops the same VA
        being given to one coder twice.
    :param assigned_to: keep only records already assigned to this *other*
        coder. A VA may be held by several coders, so this supports handing
        one coder's batch to a second reviewer.
    """
    try:
        config = await fetch_odk_config(db)
        pcva_config = await fetch_pcva_settings(db)

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

        location_field = config.field_mapping.location_level1
        date_field = config.field_mapping.interview_date or 'id10012'

        record_filters = ""
        if location:
            record_filters += f"\n                    FILTER doc.{location_field} == @location"
            bind_vars["location"] = location
        # Dates are stored as ISO-8601 strings, for which a lexical comparison
        # is also a chronological one.
        if start_date:
            record_filters += f"\n                    FILTER doc.{date_field} >= @start_date"
            bind_vars["start_date"] = start_date
        if end_date:
            record_filters += f"\n                    FILTER doc.{date_field} <= @end_date"
            bind_vars["end_date"] = end_date
        if data_source:
            # Records stored before provenance was tracked carry no marker at
            # all; they came from ODK, so treat a missing value as odk_api.
            if data_source == data_sources.ODK_API:
                record_filters += (
                    "\n                    FILTER doc.vman_data_source == @data_source"
                    " OR doc.vman_data_source == null"
                )
            else:
                record_filters += "\n                    FILTER doc.vman_data_source == @data_source"
            bind_vars["data_source"] = data_source
        if assigned_to:
            # agg.coders holds every coder currently assigned to this VA, so
            # this narrows to one coder's workload. Records the target coder
            # already holds stay excluded by the under_assigned filter above -
            # reassigning coder 1's batch to coder 2 therefore skips whatever
            # coder 2 already has rather than duplicating it.
            record_filters += "\n                    FILTER agg != null AND @assigned_to IN agg.coders"
            bind_vars["assigned_to"] = assigned_to


        # TODO: Every instanceid field used has to come from settings since it's the unique VA ID identified in the VA Data records

        query = f"""
            LET assigned_agg = (
                FOR va IN {db_collections.ASSIGNED_VA}
                    FILTER va.is_deleted == false
                    COLLECT vaId = va.vaId, coder = va.coder WITH COUNT INTO count
                    RETURN {{ vaId, coder, assignmentCount: count }}
            )

            // Two corrections here, both of which had silently emptied this
            // lookup. MERGE, not [0]: the subquery yields one single-key
            // object per VA, so taking element [0] kept exactly one of them.
            // And `INTO groups = a`, not a bare `INTO groups`: the bare form
            // makes each element {{ a: {{...}} }}, so groups[*].coder read null
            // and the map came out as {{assignmentCount: 0, coders: [null]}}.
            // Between them the Coders column was always blank, the assignment
            // count always 0, and any filter on agg.coders matched nothing.
            LET va_map = MERGE(
                FOR a IN assigned_agg
                    COLLECT vaId = a.vaId INTO groups = a
                    RETURN {{
                        [vaId]: {{
                            assignmentCount: SUM(groups[*].assignmentCount),
                            coders: UNIQUE(groups[*].coder)
                        }}
                    }}
            )

            LET under_assigned = (
                FOR a IN assigned_agg
                    FILTER a.coder == @coder AND a.assignmentCount <= @maximum_assignment
                    RETURN a.vaId
            )

            LET result = (
                FOR doc IN {db_collections.VA_TABLE}
                    LET vaId = doc.{config.field_mapping.instance_id}
                    LET agg = va_map[vaId]

                    FILTER vaId NOT IN under_assigned
                    {record_filters}

                    LET coders = (
                        FOR user IN {db_collections.USERS}
                            FILTER user.uuid IN (agg ? agg.coders : [])
                            RETURN {{ uuid: user.uuid, name: user.name }}
                    )
                    
                    RETURN MERGE(doc, {{
                        assignments: agg ? agg.assignmentCount : 0,
                        coders: coders
                    }})
            )

            LET data = {'SLICE(result, @offset, @limit)' if paging else 'result'}

            RETURN {{ total: LENGTH(result), data: data }}
            """

        if coder:
            bind_vars.update({
                "coder": coder
            })
        bind_vars.update({
            "maximum_assignment": pcva_config.vaAssignmentLimit
        })

        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        query_result = query_result.next()
        unassigned_data = [format_va_record(va, config) for va in query_result['data']]
        return ResponseMainModel(data=unassigned_data, message="Unassigned VAs fetched successfully!", total = query_result["total"], pager=Pager(page=page_number, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get unassigned va: {e}")

async def get_uncoded_assignment_service(paging: bool = True, page_number: int = 0, limit: int = 10, format_records: bool = True, coder: str = None, db: StandardDatabase = None):
    try:
        config = await fetch_odk_config(db)
        pcva_config = await fetch_pcva_settings(db)
        offset = (page_number - 1) * limit if paging else 0

        query = ""
        bind_vars = {}
        if paging:
            
            bind_vars.update({
                "offset": offset,
                "limit": limit
            })

        
        # TODO: Every instanceid field used has to come from settings since it's the unique VA ID identified in the VA Data records

        query = f"""
            LET result = (
                FOR doc IN form_submissions
                    LET coders_ids = (
                        FOR va IN {db_collections.ASSIGNED_VA}
                        FILTER va.vaId == doc.{config.field_mapping.instance_id} AND va.coder != @coder
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
                        FILTER va.vaId == doc.{config.field_mapping.instance_id} 
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
            "maximum_assignment": pcva_config.vaAssignmentLimit
        })

        query_result = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        query_result = query_result.next()
        unassigned_data = [format_va_record(va, config) for va in query_result['data']]
        return ResponseMainModel(data=unassigned_data, message="Assigned VAs fetched successfully!", total = query_result["total"], pager=Pager(page=page_number, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get assigned va: {e}")
    

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

            config = await fetch_odk_config(db)
            is_valid_va = await record_exists(db_collections.VA_TABLE, custom_fields={config.field_mapping.instance_id: vaId}, db = db)
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
        
        return ResponseMainModel(data=va_assignment_data, message="VA Assigned successfully!")
    except Exception as e:
        raise e

async def unassign_va_service(va_records: AssignVARequestClass, user: User,  db: StandardDatabase = None):
    try:
        if not va_records.coder:
            raise HTTPException(status_code=400, detail=f"Coder not specified specified")
        
        is_valid_coder = await record_exists(db_collections.USERS, uuid = va_records.coder, db = db)
        
        if not (is_valid_coder):
            raise HTTPException(status_code=400, detail=f"Invalid coder specified")

        vaIds  = va_records.vaIds
        failed_vas = []
        for vaId in vaIds:

            config = await fetch_odk_config(db)
            is_valid_va = await record_exists(db_collections.VA_TABLE, custom_fields={config.field_mapping.instance_id: vaId}, db = db)
            
            if not is_valid_va:
                failed_vas.append({
                    "va": vaId,
                    "error": "This va id does not exist."
                })
                continue

            existing_va_assignment_data = await AssignedVA.get_many(
                filters= {
                    "vaId": vaId,
                    "coder": va_records.coder
                },
                db = db
            )
            if existing_va_assignment_data:
                if not len(existing_va_assignment_data) == 1:
                    failed_vas.append({
                        "va": vaId,
                        "error": "This va id for this coder has multiple existence."
                    })
                    continue
                existing_object = AssignedVA(**existing_va_assignment_data[0])
                await AssignedVA.delete(doc_uuid = existing_object.uuid, deleted_by = user.uuid, db = db)
            else:
                failed_vas.append({
                    "va": vaId,
                    "error": "This va assignment for this coder does not exist."
                })
                continue
        
        return ResponseMainModel(data=failed_vas, message="VA Unassigned successfully!")
    except Exception as e:
        raise e


async def get_va_assignment_service(paging: bool, page_number: int = None, limit: int = None, include_deleted: bool = None, filters: Dict = {}, user = None, db: StandardDatabase = None):
    
    try:
        config = await fetch_odk_config(db)
        pcva_config = await fetch_pcva_settings(db)

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
                FILTER va_record.{config.field_mapping.instance_id} IN (
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
            config = await fetch_odk_config(db)
            is_va_existing = await record_exists(db_collections.VA_TABLE, custom_fields = {config.field_mapping.instance_id: coded_va.assigned_va}, db = db)
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
        if coder:
            # Each row carries the coding this coder made for it, so the list
            # can show the underlying cause without a second round trip.
            #
            # The underlying cause is the last completed line of Frame A
            # (d, else c, else b, else a) - the same rule the concordance
            # calculation in va_records_utils.get_categorised_pcva_results
            # uses, so the two can never disagree about what was coded.
            #
            # This list means "VAs I have coded", regardless of who else coded
            # them. It used to filter on `coder_count == 1`, which was not a
            # concordance test but a count of coders: raising vaAssignmentLimit
            # above 1 would have made any VA a second coder touched vanish from
            # the first coder's list. Agreement between coders is a separate
            # question, answered by the Concordant VA view.
            query = f"""
                FOR va_record IN {db_collections.VA_TABLE}
                    LET vaId = va_record.{config.field_mapping.instance_id}

                    LET latest = FIRST(
                        FOR r IN {db_collections.PCVA_RESULTS}
                            FILTER r.assigned_va == vaId
                               AND r.created_by == @coder
                               AND r.is_deleted == false
                            SORT r.datetime DESC
                            LIMIT 1
                            RETURN r
                    )
                    FILTER latest != null

                    LET ucod_id = latest.frameA.d != null ? latest.frameA.d :
                                  latest.frameA.c != null ? latest.frameA.c :
                                  latest.frameA.b != null ? latest.frameA.b :
                                  latest.frameA.a
                    LET ucod = FIRST(
                        FOR icd IN {db_collections.ICD10}
                            FILTER icd.uuid == ucod_id
                            RETURN CONCAT(icd.code, " - ", icd.name)
                    )

                    {paginator}
                    RETURN MERGE(va_record, {{
                        underlyingCause: ucod,
                        mlCause: latest.ml_analysis.cause,
                        mlProbability: latest.ml_analysis.probability
                    }})
            """

            bind_vars.update({
                "coder": coder,
            }) 
        else:
            query = f"""
                FOR va_record IN {db_collections.VA_TABLE}
                    FILTER va_record.{config.field_mapping.instance_id} IN (
                        FOR va IN {db_collections.PCVA_RESULTS}
                            FILTER va.is_deleted == false
                            SORT va.datetime DESC
                            COLLECT assigned_va = va.assigned_va
                            INTO latest_records = va.assigned_va
                            {paginator}
                            RETURN FIRST(latest_records)
                    )
                    RETURN va_record
            """
        coded_vas = await VManBaseModel.run_custom_query(query = query, bind_vars = bind_vars, db = db)

        coded_data = []
        for va in coded_vas:
            row = format_va_record(va, config)
            row.underlyingCause = va.get("underlyingCause")
            row.mlCause = va.get("mlCause")
            row.mlProbability = va.get("mlProbability")
            coded_data.append(row)

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
    

async def get_discordants_va_service(
        paging: bool = None,
        page_number: int = None,
        limit: int = None, 
        coder: str = None, 
        db: StandardDatabase = None):
    try:
        config = await fetch_odk_config(db)
        results = await get_categorised_pcva_results(coder_uuid = coder, paging = paging, page_number = page_number, limit = limit,  db=db)
        discordants = results.get("discordants", "")

        vas = [va[0].get("assigned_va", "") for va in discordants]

        query = f"""
            FOR va IN {db_collections.VA_TABLE}
            FILTER va.{config.field_mapping.instance_id} IN @discordants
            RETURN va
        """

        bind_vars = {
            "discordants": vas
        }
        va_data = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)

        total_discordants = results.get("total_discordants", "")

        return ResponseMainModel(data=[format_va_record(va, config) for va in va_data], message="Discordants VAs fetched successfully", total=total_discordants, pager=Pager(page=page_number, limit=limit) if paging else None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get discordants: {e}")
    

# @ttl_cache(ttl=300, key_prefix="form_questions")
async def get_form_questions_service(filters: Dict = None, db: StandardDatabase = None):
    questions = await VA_Question.get_many(paging=False, filters=filters, db=db)

    questions = [VAQuestionResponseClass(**question).model_dump() for question in questions]
    questions = { question['name']: question for question in questions} if len(questions) else {}

    # Only for the "give me everything" call (no specific names requested) -
    # merges in raw field names that actually exist on VA records but were
    # never, and can never be, part of the question dictionary (see
    # get_distinct_va_field_names). A caller asking for specific
    # question_id's wants exactly those, not the whole data-driven set.
    if not filters:
        raw_fields = await get_distinct_va_field_names(db)
        for name in raw_fields:
            if name not in questions:
                questions[name] = VAQuestionResponseClass(
                    path=name, name=name, type='string', label=name
                ).model_dump()

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


async def get_pcva_results(
        paging: int = None,
        page_number: int = None,
        limit: int = None,
        db: StandardDatabase = None):
    try:

        offset = (page_number - 1) * limit if paging else 0

        paginator = ""
        bind_vars = {}
        if paging:
            paginator = f"LIMIT @offset, @limit"
            bind_vars.update({
                "offset": offset,
                "limit": limit
            })

        query = f"""
            FOR doc IN {db_collections.PCVA_RESULTS}
            SORT doc.datetime DESC
            COLLECT created_by = doc.created_by, assigned_va = doc.assigned_va INTO grouped
            {paginator}
            RETURN FIRST(
                FOR group IN grouped
                    LIMIT 1
                    LET result = group.doc
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
                        FOR contrib_id IN (result.frameA.contributories || [])
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
        """

        results = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        if not results:
            return ResponseMainModel(data=[], message="No PCVA results found!")
        pcva_results = []
        for result in results:
            result = await populate_user_fields(data = result, specific_fields=["coder"], db=db)
            pcva_results.append(result) 
        return ResponseMainModel(data=pcva_results, message="PCVA results fetched successfully!", pager=Pager(page=page_number, limit=limit) if paging else None)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get PCVA results: {e}")

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
                        FOR contrib_id IN (result.frameA.contributories || [])
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
                        [CONCAT('coder', TO_STRING(i), '_underlying')]: coder.cause_d != null ? coder.cause_d :
                                                                        coder.cause_c != null ? coder.cause_c :
                                                                        coder.cause_b != null ? coder.cause_b :
                                                                        coder.cause_a,

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
    


async def save_discordant_message_service(va_id: str, user_id: str, message: str, db: StandardDatabase = None):
    try:
        discordant_message_object = await PCVAMessages(
            va=va_id,
            message=message,
            created_by=user_id,
            read_by=[user_id]
        ).save(db)
        message_object = {
            "uuid": discordant_message_object.get("uuid",""),
            "va": discordant_message_object.get("va",""),
            "message": discordant_message_object.get("message",""),
            "read_by": discordant_message_object.get("read_by",""),
            "created_by": discordant_message_object.get("created_by",""),
            "created_at": discordant_message_object.get("created_at","")
        }
        return json.dumps(message_object)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save discordant message: {e}")

async def get_discordant_messages_service(va_id: str, coder: str, db: StandardDatabase = None):
    try:
        if not db.has_collection(PCVAMessages.get_collection_name()):
            PCVAMessages.init_collection(db)

        if not va_id:
            raise HTTPException(status_code=400, detail="VA ID is required.")
        
        if not coder:
            raise HTTPException(status_code=400, detail="Coder ID is required.")
            
        query = f"""
            LET coded = (
                FOR doc in {db_collections.PCVA_RESULTS}
                FILTER doc.assigned_va == @va AND doc.created_by == @coder
                SORT doc.datetime DESC
                COLLECT created_by = doc.created_by, assigned_va = doc.assigned_va
                INTO latest_records = doc
                RETURN FIRST(latest_records)
            )
            
            LET discordants = (
                FOR doc in {db_collections.PCVA_RESULTS}
                FILTER doc.assigned_va == @va AND doc.created_by != @coder
                SORT doc.datetime DESC
                RETURN doc
            )

            LET messages = (
                FOR message IN {db_collections.PCVA_MESSAGES}
                FILTER message.va == @va AND message.is_deleted == false
                SORT message.created_at ASC
                RETURN {{
                    uuid: message.uuid,
                    va: message.va,
                    message: message.message,
                    read_by: message.read_by,
                    created_by: message.created_by,
                    created_at: message.created_at   
                }}
            )

            LET coders = (
                FOR doc IN pcva_results
                FILTER doc.assigned_va == @va
                COLLECT created_by = doc.created_by
                RETURN created_by
            )


            RETURN {{
                coded: coded,
                discordants: discordants,
                messages: messages,
                coders: coders
            }}
        """
        
        bind_vars = {
            "va": va_id,
            "coder": coder
        }

        discordant_messages_cursor = await PCVAMessages.run_custom_query(query=query, bind_vars=bind_vars, db = db)
        discordant_messages = discordant_messages_cursor.next()

        discordant_messages['coded'] = [await PCVAResultsResponseClass.get_structured_codedVA(pcva_result = discordant_messages['coded'][0], db = db)]
        discordant_messages['discordants'] = [
            await PCVAResultsResponseClass.get_structured_codedVA(pcva_result=discordant, db=db)
            for discordant in discordant_messages["discordants"]
        ]
        return ResponseMainModel(data=discordant_messages, message="Discordant message fetched successfully!")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save discordant message: {e}")
    

async def read_discordants_message(va_id: str, user_uuid: str = None, db: StandardDatabase = None):
    try:
        query = f"""
            FOR message IN {db_collections.PCVA_MESSAGES}
                FILTER message.va == @va_id AND POSITION(message.read_by, @user) == -1

                UPDATE {{ read_by: APPEND(message.read_by, [@user], true) }} IN {db_collections.PCVA_MESSAGES}
        """
        bind_vars = {
            "va_id": va_id,
            "user": user_uuid
        }

        await PCVAMessages.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        return ResponseMainModel(message="Message read successfully!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read message: {e}")


async def get_configurations_service(db: StandardDatabase = None):
    try:
        
        configuration = await PCVAConfigurations.get_many(db=db)

        if len(configuration) > 0:
            return ResponseMainModel(data = PCVAConfigurationsRequest(**configuration[0]), message="PCVA Configuration fetched successfully!")

        return ResponseMainModel(data = configuration, message="PCVA Configuration set successfully!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read message: {e}")

async def save_configurations_service(configs: PCVAConfigurationsRequest = None, current_user: User = None, db: StandardDatabase = None):
    try:
        if configs.concordanceLevel > configs.vaAssignmentLimit:
            raise HTTPException(status_code=400, detail=f"Concordance level has to be less than assignment limit")
        
        configuration = await PCVAConfigurations.get_many(db=db)

        if len(configuration) > 0:
            config_object = replace_object_values(configs.model_dump(), configuration[0])
            saved = await PCVAConfigurations(**config_object).update(updated_by=current_user.uuid, db=db)

        else:
            config_object = configs.model_dump()
            config_object["created_by"] = current_user.uuid
            saved = await PCVAConfigurations(**config_object).save(db=db)
        return ResponseMainModel(data = PCVAConfigurationsRequest(**saved), message="PCVA Configuration set successfully!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read message: {e}")




async def get_concordant_va_service(
    paging: bool = True,
    page_number: int = 1,
    limit: int = 10,
    db: StandardDatabase = None,
):
    """VA records where the physicians agree on the underlying cause.

    Agreement is measured exactly as PCVA Configuration defines it: a VA is
    concordant when at least `concordanceLevel` coders arrived at the same
    underlying cause - the last completed line of Frame A (d, else c, else b,
    else a), the same rule the Coded VA list and get_categorised_pcva_results
    use.

    This scales with the configuration rather than assuming two coders. With
    the assignment limit at 1 and concordance at 1, a single coder's cause
    trivially reaches the threshold, so this list matches Coded VA - which is
    the correct answer, not a special case. Raise the limit to 3 and set
    concordance to 3, and a VA appears here only once three physicians have
    independently landed on the same cause.

    Only each coder's *latest* coding counts, so re-coding corrects an earlier
    disagreement instead of being counted twice.
    """
    try:
        config = await fetch_odk_config(db)
        pcva_config = await fetch_pcva_settings(db)

        offset = (page_number - 1) * limit if paging else 0
        bind_vars = {"concordance_level": pcva_config.concordanceLevel}
        if paging:
            bind_vars.update({"offset": offset, "limit": limit})

        query = f"""
            LET agreements = (
                FOR result IN {db_collections.PCVA_RESULTS}
                    FILTER result.is_deleted == false
                    SORT result.datetime DESC
                    // one entry per (VA, coder): the coder's most recent coding
                    COLLECT va = result.assigned_va, coder = result.created_by
                    INTO codings = result
                    LET latest = FIRST(codings)
                    LET cause = latest.frameA.d != null ? latest.frameA.d :
                                latest.frameA.c != null ? latest.frameA.c :
                                latest.frameA.b != null ? latest.frameA.b :
                                latest.frameA.a
                    FILTER cause != null

                    COLLECT vaId = va INTO perCoder = {{ cause: cause, coder: coder }}

                    LET tallies = (
                        FOR entry IN perCoder
                            COLLECT c = entry.cause WITH COUNT INTO n
                            SORT n DESC
                            RETURN {{ cause: c, agreeing: n }}
                    )
                    LET top = FIRST(tallies)
                    FILTER top.agreeing >= @concordance_level

                    RETURN {{
                        vaId: vaId,
                        cause: top.cause,
                        agreeing: top.agreeing,
                        coders: LENGTH(perCoder)
                    }}
            )

            LET page = {'SLICE(agreements, @offset, @limit)' if paging else 'agreements'}

            LET rows = (
                FOR a IN page
                    LET va_record = FIRST(
                        FOR doc IN {db_collections.VA_TABLE}
                            FILTER doc.{config.field_mapping.instance_id} == a.vaId
                            LIMIT 1
                            RETURN doc
                    )
                    FILTER va_record != null
                    LET cause_name = FIRST(
                        FOR icd IN {db_collections.ICD10}
                            FILTER icd.uuid == a.cause
                            RETURN CONCAT(icd.code, " - ", icd.name)
                    )
                    RETURN MERGE(va_record, {{
                        underlyingCause: cause_name,
                        agreeingCoders: a.agreeing,
                        totalCoders: a.coders
                    }})
            )

            RETURN {{ total: LENGTH(agreements), data: rows }}
        """

        cursor = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        result = cursor.next()

        rows = []
        for va in result["data"]:
            row = format_va_record(va, config)
            row.underlyingCause = va.get("underlyingCause")
            row.agreeingCoders = va.get("agreeingCoders")
            row.totalCoders = va.get("totalCoders")
            rows.append(row)

        return ResponseMainModel(
            data=rows,
            total=result["total"],
            message=(
                f"{result['total']} concordant VA(s) at a concordance level of "
                f"{pcva_config.concordanceLevel}"
            ),
            pager=Pager(page=page_number, limit=limit),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get concordant vas: {e}")
