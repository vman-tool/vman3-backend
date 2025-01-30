from typing import Any, Dict, List, Union
from arango import Optional
from arango.database import StandardDatabase
from pydantic import BaseModel

from app.settings.models.settings import SettingsConfigData
from app.shared.configs.models import VManBaseModel



class DataResponse(BaseModel):
    id: str
    vaId: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    interviewDay: Optional[str] = None
    interviewerName: Optional[str] = None
    instanceid: Optional[str] = None
    assignments: Union[Any, None] = None
    coders: Union[List[Any], None] = None



def format_va_record(raw_data: dict, config: SettingsConfigData = None) -> DataResponse:
    assignments = None
    datacoders = None
    if 'assignments' in raw_data:
        assignments = raw_data['assignments']

    if 'coders' in raw_data:
        datacoders = raw_data['coders']

    region_field = config.field_mapping.location_level1
    vaid_field = config.field_mapping.va_id
    instance_field = config.field_mapping.instance_id
    district_field = config.field_mapping.location_level2
    interview_name_field = config.field_mapping.interviewer_name
    today_field = config.field_mapping.date

    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get(f"{instance_field}", vaid_field),
        region=raw_data.get(f"{region_field}", "id10005r"),
        district=raw_data.get(f"{district_field}", "id10005d"),
        interviewDay=raw_data.get(f"{today_field}", "today"),
        interviewerName=raw_data.get(f"{interview_name_field}", "id10007"),
        instanceid=raw_data.get(f"{instance_field}", ""),
        assignments=assignments,
        coders = datacoders
    )


async def get_categorised_pcva_results(coder_uuid: str = None, paging: bool = None, page_number: int = None, limit: int = None, db: StandardDatabase = None):
    if not coder_uuid:
        raise ValueError("Coder UUID is required")
    
    if not db:
        raise ValueError("Database connection is required")
    
    offset = (page_number - 1) * limit if paging else 0

    bind_vars = {}
    if paging:
        bind_vars.update({
            "offset": offset,
            "limit": limit
        })
    
    bind_vars.update({
        "coder": coder_uuid
    })

    query = f"""
        LET coders_coded = (
            FOR coded IN pcva_results
            FILTER coded.created_by == @coder AND coded.is_deleted == false
            SORT coded.datetime DESC
            COLLECT assigned_va = coded.assigned_va INTO latest
            RETURN FIRST(latest[*].coded).assigned_va
        )

        LET vas_with_multiple_coders = (
            FOR va IN coders_coded
            LET coder_count = LENGTH(
                UNIQUE(
                    FOR r IN pcva_results
                    FILTER r.assigned_va == va
                    RETURN r.created_by
                )
            )
            FILTER coder_count > 1
            RETURN va
        )

        LET results = (
        FOR result in pcva_results
            FILTER result.assigned_va IN vas_with_multiple_coders
            SORT result.datetime DESC
            COLLECT va_group = result.assigned_va, user = result.created_by 
            INTO userResults = {{
                result: result,
                datetime: result.datetime
            }}
            
            LET latestResult = (
            FOR r IN userResults
            SORT r.datetime DESC
            LIMIT 1
            RETURN r.result
            )[0]
            
            COLLECT va = va_group INTO vaResults = {{
                latest: latestResult
            }}
            
        LET priorityValues = (
            FOR r IN vaResults[*].latest
            RETURN (
                r.frameA.d != null ? r.frameA.d :
                r.frameA.c != null ? r.frameA.c :
                r.frameA.b != null ? r.frameA.b :
                r.frameA.a
            )
        )
        LET matchCount = LENGTH(
            FOR pv IN priorityValues
                COLLECT val = pv WITH COUNT INTO count
                FILTER count >= 2
                RETURN count
            )
            RETURN {{
                is_concordant: matchCount > 0,
                data: vaResults[*].latest
            }}
        )

        LET concordants = results[* FILTER CURRENT.is_concordant == true].data
        LET discordants = results[* FILTER CURRENT.is_concordant == false].data

        RETURN {{
            concordants: {'SLICE(concordants, @offset, @limit)' if paging else 'concordants'},
            discordants: {'SLICE(discordants, @offset, @limit)' if paging else 'discordants'},
            total_concordants: LENGTH(concordants),
            total_discordants: LENGTH(discordants)
        }}
    """

    categorised_results = await VManBaseModel.run_custom_query(query = query, bind_vars = bind_vars, db=db)
    return categorised_results.next() if categorised_results else None