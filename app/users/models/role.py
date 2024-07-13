from typing import List


from pydantic import BaseModel


class Role(BaseModel):
    name: str
    privileges: List[str]
