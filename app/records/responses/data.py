from typing import Optional

from pydantic import BaseModel

from app.settings.models.settings import SettingsConfigData


class DataResponse(BaseModel):
    id: str
    vaId: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    interviewDay: Optional[str] = None
    interviewerName: Optional[str] = None

def map_to_data_response(config:SettingsConfigData,raw_data: dict) -> DataResponse:
    region_field = config.field_mapping.location_level1
    vaid_field = config.field_mapping.va_id
    district_field = config.field_mapping.location_level2
    interview_name_field = config.field_mapping.interviewer_name
    today_field = config.field_mapping.date
    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get(f"instanceid", "vaid"),
        region=raw_data.get(f"{region_field}", "id10005r"),
        district=raw_data.get(f"{district_field}", "id10005d"),
        interviewDay=raw_data.get(f"{today_field}", "today"),
        interviewerName=raw_data.get(f"{interview_name_field}", "id10007")
    )
