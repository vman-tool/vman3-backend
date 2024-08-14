from typing import Optional, Union

from pydantic import BaseModel


class OdkConfigModel(BaseModel):
    form_id: str
    project_id: str
    url: str #TODOS change to HttpUrl
    username: str
    password: str
    api_version: Optional[str] = 'v1'
    is_sort_allowed: Union[bool, None] = None

    # @validator('form_id', 'project_id', 'username', 'password')
    # def not_empty(cls, v):
    #     if not v or v.strip() == "":
    #         raise ValueError('This field cannot be empty')
        # return v