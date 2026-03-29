from pydantic import BaseModel
from typing import Union


class PCVAConfigurationsRequest(BaseModel):
    useICD11: bool = False
    vaAssignmentLimit: int
    concordanceLevel: int
    showOtherCodersWork: Union[bool, None] = None