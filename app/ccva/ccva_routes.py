
import uuid

from arango.database import StandardDatabase
from fastapi import APIRouter, BackgroundTasks, Depends, status

from app.ccva.services.ccva_data_services import fetch_processed_ccva_graphs
from app.ccva.services.ccva_services import run_ccva
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import get_current_user, oauth2_scheme

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
    
    
@ccva_router.get("", status_code=status.HTTP_200_OK,)
async def get_processed_ccva_graphs(
    background_tasks: BackgroundTasks,
    oauth = Depends(oauth2_scheme), 
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
   return await fetch_processed_ccva_graphs(db=db)
    
