import io
import uuid
import zipfile
from datetime import date, datetime
from typing import Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi import (APIRouter, BackgroundTasks, Body, Depends, File,
                     HTTPException, Query, Response, UploadFile, status)

from app.ccva.services.ccva_data_services import (
    delete_ccva_entry, fetch_all_processed_ccva_graphs,
    fetch_processed_ccva_graphs, set_ccva_as_default)
from app.ccva.services.ccva_graph_services import \
    fetch_db_processed_ccva_graphs
from app.ccva.services.ccva_services import (fetch_ccva_results_and_errors,
                                             get_record_to_run_ccva, run_ccva, process_upload_and_run_ccva)
from app.ccva.services.ccva_upload import insert_all_csv_data
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.shared.services.task_progress_service import TaskProgressService
from app.users.decorators.user import get_current_user, oauth2_scheme
from app.utilits.db_logger import  log_to_db
from app.shared.utils.cache import cache

ccva_router = APIRouter(
    prefix="/ccva",
    tags=["CCVA"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

# In-memory store for task results
task_results = {}

#@log_to_db(context="run_ccva_with_csv", log_args=True)
@ccva_router.post("/upload", status_code=status.HTTP_200_OK)
async def run_ccva_with_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    unique_id: Optional[str] = Body ('KEY', alias="unique_id"),
    oauth = Depends(oauth2_scheme),
    malaria_status = Body('h', alias="malaria_status"),
    ccva_algorithm: Optional[str] = Body(None, alias="ccva_algorithm"),
    hiv_status: Optional[str] = Body('h', alias="hiv_status"),
    current_user: Optional[str] = Depends(get_current_user),
    start_date: Optional[date] = Body(None, alias="start_date"),
    top: Optional[int] = Body(None, alias="top"),
    end_date: Optional[date] = Body(None, alias="end_date"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    start_time = datetime.now()

    try:
        # Validate ccva_algorithm
        if ccva_algorithm is not None and ccva_algorithm not in ["InterVA5"]:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid CCVA algorithm, or algorithm not yet supported.")

        # Generate task ID
        task_id = str(uuid.uuid4())
        task_results = {}  # Initialize task results storage

        # Read CSV file immediately
        contents = await file.read()
        
        # Prepare extra info for progress tracking (initial immediate response)
        user_id = current_user.get('uid') or current_user.get('id') or "unknown" if isinstance(current_user, dict) else "unknown"

        # Add the entire process (Upload -> Insert -> CCVA) to background tasks
        background_tasks.add_task(
            process_upload_and_run_ccva,
            file_contents=contents,
            unique_id=unique_id,
            current_user=current_user,
            start_date=start_date,
            end_date=end_date,
            top=top,
            date_type=date_type,
            malaria_status=malaria_status,
            hiv_status=hiv_status,
            ccva_algorithm=ccva_algorithm,
            task_id=task_id,
            task_results=task_results,
            db=db
        )

        # Constructing initial response
        datas = {
            "progress": 1,
            "total_records": 0, # Unknown yet
            "message": "Initializing upload...",
            "status": 'init',
            "elapsed_time": "0:00:00",
            "task_id": task_id,
            "error": False,
            "user_id": user_id
        }

        # Return immediately so frontend can connect to socket
        return ResponseMainModel(data=datas, message="CCVA upload started. Check progress...")

    except Exception as e:
        # Raising the error so FastAPI can handle it
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
#@log_to_db(context="run_internal_ccva", log_args=True)
@ccva_router.post("", status_code=status.HTTP_200_OK,)
async def run_internal_ccva(
    background_tasks: BackgroundTasks,
    oauth = Depends(oauth2_scheme), 
    malaria_status = Body('h', alias="malaria_status"),
    ccva_algorithm: Optional[str] = Body(None, alias="ccva_algorithm"),
    hiv_status: Optional[str] = Body('h', alias="hiv_status"),
    current_user: Optional[str] = Depends(get_current_user),
    start_date: Optional[date] = Body(None, alias="start_date"),
    end_date: Optional[date] = Body(None, alias="end_date"),
    top: Optional[int] = Body(None, alias="top"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    start_time = datetime.now()

    try:
        # Validate ccva_algorithm
        if ccva_algorithm is not None and ccva_algorithm not in ["InterVA5"]:
            print(ccva_algorithm)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid CCVA algorithm, or algorithm not yet supported.")

        # Generate task ID and fetch records
        task_id = str(uuid.uuid4())
        ## show the progress of getting data from db in web socket
        task_results = {}  # Initialize task results storage
        records = await get_record_to_run_ccva(current_user,db,None, task_id, task_results, start_date, end_date,date_type=date_type,top=top)

        # Handle no records scenario
        if not records:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found to run CCVA")
        print('before background task')
        
        # Prepare extra info for progress tracking
        user_id = current_user.get('uid') or current_user.get('id') or "unknown"

        # Add the CCVA task to background
        background_tasks.add_task(run_ccva, db, records, task_id, task_results, start_date, end_date, malaria_status, hiv_status, ccva_algorithm, user_id)
        print('after background task')
        # Constructing response
        datas = {
            "progress": 1,
            "total_records": len(records.data),
            "message": "Collecting data.",
            "status": 'init',
            "elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
            "task_id": task_id,
            "error": False,
            "user_id": user_id
        }


        return ResponseMainModel(data={"task_id": task_id, "total_records": len(records.data), **datas}, message="CCVA is running...")
    
    except Exception as e:
        print(e)
        # Raising the error so FastAPI can handle it
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@ccva_router.get("/progress/{task_id}", status_code=status.HTTP_200_OK)
async def get_ccva_progress(
    task_id: str,
    db: StandardDatabase = Depends(get_arangodb_session)
):
    """
    Fetch the current progress of a running task from the database.
    Useful for resuming progress tracking on page reload.
    """
    progress = await TaskProgressService.get_progress(db, task_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task progress not found.")
    
    return ResponseMainModel(data=progress, message="Progress fetched", error=False)

#@log_to_db(context="get_processed_ccva_graphs", log_args=True)    
@ccva_router.get("", status_code=status.HTTP_200_OK)
@cache(namespace='ccva_graphs_get',expire=6000)
async def get_processed_ccva_graphs(
    background_tasks: BackgroundTasks,
    ccva_id: Optional[str] = None,
    selected_success_type=None,
    is_default: Optional[bool] = None,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 30,
    ccva_graph_db_source: Optional[bool] = True,
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    locations: Optional[str] = Query(None, alias="locations"),
    oauth = Depends(oauth2_scheme), 
    current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    """
    Route to fetch processed CCVA graphs, either by ccva_id, is_default, or with regular pagination and filtering.
    """

    try:
        # Fetch processed CCVA data
        print(ccva_graph_db_source)
        if ccva_graph_db_source:

            response =  await fetch_db_processed_ccva_graphs (
                current_user=current_user,
                ccva_id=ccva_id,
                selected_success_type=selected_success_type,
                is_default=True,
                paging=paging,
                page_number=page_number,
                limit=limit,
                start_date=start_date,
                end_date=end_date,
                locations=locations.split(",") if locations else None,
                date_type=date_type,
                db=db
            )
        else:
            response = await fetch_processed_ccva_graphs(
                ccva_id=ccva_id,
                is_default=is_default,
                paging=paging,
                page_number=page_number,
                limit=limit,
                start_date=start_date,
                end_date=end_date,
                locations=locations.split(",") if locations else None,
                date_type=date_type,
                db=db
            )
 
        return response
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch processed CCVA graphs.")
    
#@log_to_db(context="get_all_processed_ccva_graphs", log_args=True)        
@ccva_router.get("/list", status_code=status.HTTP_200_OK,)
@cache(namespace='ccva_graphs_list_get',expire=6000)
async def get_all_processed_ccva_graphs(
    background_tasks: BackgroundTasks,
    # oauth = Depends(oauth2_scheme), 
    # current_user = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
):
   return await fetch_all_processed_ccva_graphs(db=db)

#@log_to_db(context="set_default_ccva", log_args=True)       
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
#@log_to_db(context="delete_ccva", log_args=True)       
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




# Endpoint to download the results as JSON or CSV
#@log_to_db(context="download_ccva_results", log_args=True)  
@ccva_router.get("/download_ccva_results/{task_id}", status_code=status.HTTP_200_OK)
async def download_ccva_results(
    task_id: str,
    db: StandardDatabase = Depends(get_arangodb_session),
    file_format: str = "json"
):
    # print(task_id)
    try:
        if task_id is None:
           raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task ID is required.")
        # Fetch the data
        ccva_data = await fetch_ccva_results_and_errors(db, str(task_id))

        if file_format == "json":
            # Return JSON response formatted with ResponseMainModel
            return ResponseMainModel(
                data=ccva_data, 
                message="CCVA results fetched successfully", 
                error=False
            )

        elif file_format == "csv":
            # Convert results to DataFrame for CSV export
            results_df = pd.DataFrame(ccva_data["results"])
            error_logs_df = pd.DataFrame(ccva_data["error_logs"])
            
            # Create an in-memory buffer to store the ZIP file
            buffer = io.BytesIO()

            # Create a new ZIP file in memory
            with zipfile.ZipFile(buffer, "w") as zip_file:
                # Write the results CSV to the ZIP file
                results_csv = results_df.to_csv(index=False)
                zip_file.writestr(f"ccva_results_{task_id}.csv", results_csv)

                # Write the error logs CSV to the ZIP file
                if not error_logs_df.empty:
                    error_logs_csv = error_logs_df.to_csv(index=False)
                    zip_file.writestr(f"ccva_error_logs_{task_id}.csv", error_logs_csv)

            # Seek to the start of the buffer
            buffer.seek(0)

            # Return the ZIP file as a response
            return Response(
                content=buffer.getvalue(),
                media_type="application/zip",
                headers={
                    "Content-Disposition": f"attachment; filename=ccva_results_{task_id}.zip"
                }
            )

        else:
            return ResponseMainModel(
                data=None, 
                message="Invalid format. Please choose either 'json' or 'csv'.", 
                error=True
            )

    except Exception as e:
        # Handle exceptions and return error response
        return ResponseMainModel(
            data=None,
            message=f"An error occurred: {str(e)}",
            error=True
        )