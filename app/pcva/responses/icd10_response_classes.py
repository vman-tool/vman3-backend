from pydantic import BaseModel

from app.users.models.user import User


class ICD10CategoryResponseClass(BaseModel):
    name: str
    created_at: str
    created_by: str
    uuid: str
