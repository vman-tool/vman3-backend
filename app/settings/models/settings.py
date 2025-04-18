from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class OdkConfigModel(BaseModel):
    url: str #TODOS change to HttpUrl
    username: str
    password: str
    form_id: str
    project_id: str
    api_version: Optional[str] = 'v1'  # Optional field with a default value
    is_sort_allowed: Optional[bool] = False  # Optional boolean field with a default value



class SystemConfig(BaseModel):
    app_name: str
    page_title: str
    page_subtitle: Optional[str] = None  # Optional field
    admin_level1: str
    admin_level2: Optional[str] = None  # Optional field
    admin_level3: Optional[str] = None  # Optional field
    admin_level4: Optional[str] = None  # Optional field
    map_center: str
    additional_fields: Optional[Dict[str, Any]] = Field(default_factory=dict)  # For additional fields


class FieldMapping(BaseModel):
    table_name: Optional[str] = None
    table_details: Optional[str] = None  # Optional field
    instance_id: str
    va_id: str
    consent_id: str  # Optional field
    date: str
    location_level1: str
    location_level2: str  # Optional field
    deceased_gender: str  # Optional field
    is_adult:str  # Optional field
    is_child: str  # Optional field
    is_neonate: str  # Optional field
    interviewer_name: str
    interviewer_phone: str  # Optional field
    interviewer_sex: str  # Optional field
    submitted_date: Optional[str] = None
    birth_date: Optional[str] = None
    death_date: Optional[str] = None
    interview_date: Optional[str] = None
    additional_fields: Optional[Dict[str, Any]] = Field(default_factory=dict)  # For additional fields

class FieldLabels(BaseModel):
    field_id: str
    label: Union[str, None] = None
    options: Union[Dict, None] = None
# Define the models for cron and backup settings
class CronSettings(BaseModel):
    days: List[str] = []
    time: str = "00:00"

class BackupSettings(BaseModel):
    frequency: str = "daily"  # 'daily', 'weekly', 'monthly'
    time: str = "00:00"
    location: str = "local"  # 'local', 'cloud'

# class SettingsConfigData(BaseModel):
#     type: Union[str, None] = 'odk_api_configs'  # Optional field with a default value
#     odk_api_configs: Union[OdkConfigModel, None] = None  # Optional field
#     system_configs:     Union[SystemConfig, None] = None  # Optional field
#     field_mapping: Union[FieldMapping, None] = None  # Optional field
#     va_summary: Union[List[str], None] = None
#     field_labels: Union[List[FieldLabels], None] = None
class SettingsConfigData(BaseModel):
    type: Union[str, None] = 'odk_api_configs'  # Optional field with a default value
    odk_api_configs: Union[OdkConfigModel, None] = None  # Optional field
    system_configs: Union[SystemConfig, None] = None  # Optional field
    field_mapping: Union[FieldMapping, None] = None  # Optional field
    va_summary: Union[List[str], None] = None
    field_labels: Union[List[FieldLabels], None] = None
    cron_settings: Union[CronSettings, None] = None  # New field for cron settings
    backup_settings: Union[BackupSettings, None] = None  # New field for backup settings
class ImagesConfigData(BaseModel):
    logo: Union[str, None] = None
    favicon: Union[str, None] = None
    home_image: Union[str, None] = None
    
