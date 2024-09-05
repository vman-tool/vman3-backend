from typing import Optional

from pydantic import BaseModel


class InterVA5Progress(BaseModel):
    total_records: Optional[int] = 0
    progress: Optional[int] = 0
    message: str
    status: str
    elapsed_time: Optional[str] = "0:00:00"
    task_id: str
    error: bool