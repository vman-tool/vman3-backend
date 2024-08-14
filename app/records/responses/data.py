from typing import Optional

from pydantic import BaseModel


class DataResponse(BaseModel):
    id: str
    vaId: Optional[str] = None
    region: Optional[str] = None
    district: Optional[str] = None
    interviewDay: Optional[str] = None
    interviewerName: Optional[str] = None



def map_to_data_response(raw_data: dict) -> DataResponse:
    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get("vaid", ""),
        region=raw_data.get("id10005r", ""),
        district=raw_data.get("id10005d", ""),
        interviewDay=raw_data.get("today", ""),
        interviewerName=raw_data.get("id10007", "")
    )