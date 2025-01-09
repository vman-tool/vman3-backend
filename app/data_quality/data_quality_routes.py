from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.data_quality.services.data_quality import (
    fetch_error_details,
    fetch_error_list,
    fetch_form_data,
    save_corrections_data,
)
from app.shared.configs.arangodb import get_arangodb_session
from app.users.decorators.user import get_current_user

data_quality_router = APIRouter(
    prefix="/data-quality",
    tags=["data-quality"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

@data_quality_router.get("/errors")
async def get_error_list(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    error_type: Optional[str] = None,
    group: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[List[str]] = Query(None),
    db: StandardDatabase = Depends(get_arangodb_session),
    # current_user = Depends(get_current_user)
):
    try:
        errors = await fetch_error_list(
            db,
            page,
            limit,
            error_type,
            group,
            start_date,
            end_date,
            locations
        )
        # print(errors)
        return errors
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@data_quality_router.get("/errors/{error_id}", status_code=status.HTTP_200_OK)
async def get_error_details(
    error_id: str,
    db: StandardDatabase = Depends(get_arangodb_session),
    # current_user = Depends(get_current_user)
):
    try:
        error_details = await fetch_error_details(db, error_id)
        if not error_details:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error not found")
        return error_details
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@data_quality_router.get("/form-data/{form_id}", status_code=status.HTTP_200_OK)
async def get_form_data(
    form_id: str,
    db: StandardDatabase = Depends(get_arangodb_session),
    current_user = Depends(get_current_user)
):
    try:
        form_data = await fetch_form_data(db, form_id)
        if not form_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Form data not found")
        return form_data
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@data_quality_router.put("/form-data/{form_id}", status_code=status.HTTP_200_OK)
async def update_form_data_route(
    form_id: str,
    updated_data: dict,
    db: StandardDatabase = Depends(get_arangodb_session),
    current_user = Depends(get_current_user)
):
    try:
        updated_form = await save_corrections_data(db, form_id, updated_data)
        if not updated_form:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Form data not found or could not be updated")
        return {"message": "Form data updated successfully", "updated_form": updated_form}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
