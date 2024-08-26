
import asyncio
import json
from typing import AsyncGenerator, Dict, Generator
import uuid
from arango.database import StandardDatabase
from fastapi.responses import StreamingResponse
from app.users.decorators.user import get_current_user, oauth2_scheme
from fastapi import APIRouter, Depends, status, BackgroundTasks
from app.ccva.services.ccva_services import run_ccva
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel


ccva_router = APIRouter(
    prefix="/ccva",
    tags=["CCVA"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

# In-memory store for task results
task_results = {}

@ccva_router.post("", status_code=status.HTTP_200_OK,)
async def run_internal_ccva(
    background_tasks: BackgroundTasks,
    oauth = Depends(oauth2_scheme), 
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        task_id = str(uuid.uuid4())
        background_tasks.add_task(run_ccva, db, task_id, task_results)
        return ResponseMainModel(data={"task_id": task_id}, message="CCVA Is running ...")
    except Exception as e:
        raise e
    
async def event_generator(task_id: str) -> AsyncGenerator[str, None]:
    while True:
        if task_id in task_results:
            result = task_results.pop(task_id)
            yield f"data: {json.dumps(result)}\n\n"
            break
        await asyncio.sleep(1)

@ccva_router.get("/events/{task_id}")
async def sse(task_id: str):
    return StreamingResponse(event_generator(task_id), media_type="text/event-stream")