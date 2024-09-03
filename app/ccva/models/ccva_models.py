from pydantic import BaseModel


class InterVA5Progress(BaseModel):
    progress: int
    message: str
    status: str
    elapsed_time: str
    task_id: str
    error: bool