
from datetime import date
from typing import Optional

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, Query, status

from app.records.services.list_data import fetch_va_records
from app.records.services.map_data import fetch_va_map_records
from app.records.services.regions_data import get_unique_regions
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import get_current_user
from app.utilits.db_logger import db_logger, log_to_db
from app.shared.utils.cache import cache

# from sqlalchemy.orm import Session


data_router = APIRouter(
    prefix="/records",
    tags=["Records"],
    responses={404: {"description": "Not found"}},
    # dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)



#@log_to_db(context="get_va_records", log_args=True)        
@data_router.get("", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
@cache(namespace='records',expire=6000)
async def get_va_records(
      current_user = Depends(get_current_user),
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    locations: Optional[str] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_va_records(
        current_user=current_user,
        paging=allow_paging,
        page_number=page_number,
        limit=limit,   
        start_date=start_date,
        end_date=end_date,
       locations=locations.split(",") if locations else None,
        date_type=date_type,
        db=db)
    return response
#@log_to_db(context="get_fetch_va_map_records", log_args=True)      
@data_router.get("/maps", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
@cache(namespace='maps',expire=6000)
async def get_fetch_va_map_records(
      current_user = Depends(get_current_user),
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10000000, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    date_type: Optional[str]=Query(None, alias="date_type"),
    locations: Optional[str] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_va_map_records(
        current_user=current_user,
        paging=allow_paging,
        page_number=page_number,
        limit=limit,   
        start_date=start_date,
        end_date=end_date,
        locations=locations.split(",") if locations else None,
        date_type=date_type,
        db=db)

    return response


#@log_to_db(context="fetch_unique_regions", log_args=True)      
@data_router.get("/unique-regions", response_model=ResponseMainModel)
@cache( namespace='unique_regions',expire=6000)
async def fetch_unique_regions(db: StandardDatabase = Depends(get_arangodb_session), current_user = Depends(get_current_user),):
    return await get_unique_regions(db,current_user)