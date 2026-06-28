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
    questionnaireType: Optional[str] = None
    gender: Optional[str] = None

def map_to_data_response(config: SettingsConfigData, raw_data: dict) -> DataResponse:
    fm = config.field_mapping
    is_neonate_raw = raw_data.get(fm.is_neonate, "")
    is_child_raw   = raw_data.get(fm.is_child, "")
    is_adult_raw   = raw_data.get(fm.is_adult, "")
    if str(is_neonate_raw) == "1":
        questionnaire_type = "Neonate"
    elif str(is_child_raw) == "1":
        questionnaire_type = "Child"
    elif str(is_adult_raw) == "1":
        questionnaire_type = "Adult"
    else:
        questionnaire_type = None

    gender_raw = raw_data.get(fm.deceased_gender, "")
    gender = gender_raw.capitalize() if gender_raw else None

    return DataResponse(
        id=raw_data.get("_key", ""),
        vaId=raw_data.get("instanceid", ""),
        region=raw_data.get(fm.location_level1, ""),
        district=raw_data.get(fm.location_level2, ""),
        interviewDay=raw_data.get(fm.date, ""),
        interviewerName=raw_data.get(fm.interviewer_name, ""),
        questionnaireType=questionnaire_type,
        gender=gender,
    )
