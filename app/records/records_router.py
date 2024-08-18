
from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, Query, status

from app.records.services.list_data import fetch_va_records
from app.records.services.map_data import fetch_va_map_records
from app.records.services.regions_data import get_unique_regions
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import get_current_user, oauth2_scheme

# from sqlalchemy.orm import Session


data_router = APIRouter(
    prefix="/records",
    tags=["Records"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)



# @data_router.get("/s",status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
# async def get_va_recordss(
#     paging: Optional[str] = Query(None, alias="paging"),
#     page_number: Optional[int] = Query(1, alias="page_number"),
#     limit: Optional[int] = Query(10, alias="limit"),
#     db: StandardDatabase = Depends(get_arangodb_session)):

#     try:
#         allow_paging = False if paging is not None and paging.lower() == 'false' else True
#         return await fetch_va_records(allow_paging, page_number, limit, db)

#     except Exception as e:
#         return ResponseMainModel(
#             data=None,
#             message="Failed to fetch records",
#             error=str(e),
#             total=None
#         )
        
        
@data_router.get("/", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_va_records(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    locations: Optional[List[str]] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_va_records(paging=allow_paging, page_number=page_number, limit=limit, start_date=start_date, end_date=end_date, locations=locations, db=db)
    return response

@data_router.get("/maps", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_fetch_va_map_records(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    locations: Optional[List[str]] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_va_map_records(paging=allow_paging, page_number=page_number, limit=limit, start_date=start_date, end_date=end_date, locations=locations, db=db)
    return response



@data_router.get("/unique-regions", response_model=ResponseMainModel)
async def fetch_unique_regions(db: StandardDatabase = Depends(get_arangodb_session)):
    return get_unique_regions(db)