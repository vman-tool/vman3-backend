from pydantic import BaseModel


class PCVAConfigurationsRequest(BaseModel):
    useICD11: bool
    vaAssignmentLimit: int
    concordanceLevel: int