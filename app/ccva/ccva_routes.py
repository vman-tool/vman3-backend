import uuid
from datetime import date
from typing import Optional

from arango.database import StandardDatabase
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from app.ccva.services.ccva_data_services import (
    delete_ccva_entry, fetch_all_processed_ccva_graphs,
    fetch_processed_ccva_graphs, set_ccva_as_default)
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
    
    
@ccva_router.get("", status_code=status.HTTP_200_OK)
async def get_processed_ccva_graphs(
    background_tasks: BackgroundTasks,
    ccva_id: Optional[str] = None,
    is_default: Optional[bool] = None,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 30,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    oauth = Depends(oauth2_scheme), 
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    """
    Route to fetch processed CCVA graphs, either by ccva_id, is_default, or with regular pagination and filtering.
    """
    try:
        # Fetch processed CCVA data
        response = await fetch_processed_ccva_graphs(
            ccva_id=ccva_id,
            is_default=is_default,
            paging=paging,
            page_number=page_number,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            db=db
        )
        return response
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch processed CCVA graphs.")
    
    
@ccva_router.get("/list", status_code=status.HTTP_200_OK,)
async def get_all_processed_ccva_graphs(
    background_tasks: BackgroundTasks,
    # oauth = Depends(oauth2_scheme), 
    # current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
   return await fetch_all_processed_ccva_graphs(db=db)


@ccva_router.post("/{ccva_id}/set-default", status_code=status.HTTP_200_OK)
async def set_default_ccva(
    ccva_id: str,
    db: StandardDatabase = Depends(get_arangodb_session),
    current_user = Depends(get_current_user)  # Add authentication
):
    # Implement your logic here to mark a specific CCVA entry as default
    result = await set_ccva_as_default(ccva_id, db)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="CCVA not found or could not be set as default")
    return {"message": "CCVA set as default successfully"}

# Service to delete a CCVA entry
@ccva_router.delete("/{ccva_id}", status_code=status.HTTP_200_OK)
async def delete_ccva(
    ccva_id: str,
    db: StandardDatabase = Depends(get_arangodb_session),
    current_user = Depends(get_current_user)  # Add authentication
):
    # Implement your logic here to delete the CCVA entry
    result = await delete_ccva_entry(ccva_id, db)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="CCVA not found or could not be deleted")
    return {"message": "CCVA entry deleted successfully"}

