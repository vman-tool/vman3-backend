from typing import Any, Dict, List, Union
from arango import Optional
from pydantic import BaseModel

from app.settings.models.settings import SettingsConfigData



class DataResponse(BaseModel):
    id: str
    vaId: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    interviewDay: Optional[str] = None
    interviewerName: Optional[str] = None
    instanceid: Optional[str] = None
    assignments: Union[List[Dict], None] = None



def format_va_record(raw_data: dict, config: SettingsConfigData = None) -> DataResponse:
    assignments = None
    if 'assignments' in raw_data:
        assignments = raw_data['assignments']
    # return DataResponse(
    #     id=raw_data.get("_key", ""),
    #     vaId=raw_data.get("__id", ""),
    #     region=raw_data.get("id10005r", ""),
    #     district=raw_data.get("id10005d", ""),
    #     interviewDay=raw_data.get("today", ""),
    #     interviewerName=raw_data.get("id10007", ""),
    #     instanceid=raw_data.get("instanceid", ""),
    #     instanceid=raw_data.get("instanceid", ""),
    # )

    region_field = config.field_mapping.location_level1
    vaid_field = config.field_mapping.va_id
    district_field = config.field_mapping.location_level2
    interview_name_field = config.field_mapping.interviewer_name
    today_field = config.field_mapping.date
    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get(f"{vaid_field}", "vaid"),
        region=raw_data.get(f"{region_field}", "id10005r"),
        district=raw_data.get(f"{district_field}", "id10005d"),
        interviewDay=raw_data.get(f"{today_field}", "today"),
        interviewerName=raw_data.get(f"{interview_name_field}", "id10007"),
        instanceid=raw_data.get("instanceid", ""),
        assignments=assignments
    )