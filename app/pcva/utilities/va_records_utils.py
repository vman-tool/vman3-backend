from typing import Any, Dict, List, Union
from arango import Optional
from pydantic import BaseModel


class DataResponse(BaseModel):
    id: str
    vaId: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    interviewDay: Optional[str] = None
    interviewerName: Optional[str] = None
    instanceid: Optional[str] = None
    assignments: Union[List[Dict], None] = None



def format_va_record(raw_data: dict) -> DataResponse:
    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get("__id", ""),
        region=raw_data.get("id10005r", ""),
        district=raw_data.get("id10005d", ""),
        interviewDay=raw_data.get("today", ""),
        interviewerName=raw_data.get("id10007", ""),
        instanceid=raw_data.get("instanceid", ""),
        assignments=raw_data.get("assignments", "")
    )