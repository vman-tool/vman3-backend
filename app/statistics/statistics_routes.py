
from datetime import date
from typing import List, Optional

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, Query, status

from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.statistics.services.charts import fetch_charts_statistics
from app.statistics.services.submissions import fetch_submissions_statistics

# from sqlalchemy.orm import Session


statistics_router = APIRouter(
    prefix="/statistics",
    tags=["Statistics"],
    responses={404: {"description": "Not found"}},
)



     
@statistics_router.get("/submissions", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_submissions_statistics(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    locations: Optional[List[str]] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_submissions_statistics(paging=allow_paging, page_number=page_number, limit=limit, start_date=start_date, end_date=end_date, locations=locations, db=db)
    return response


@statistics_router.get("/charts", status_code=status.HTTP_200_OK, response_model=ResponseMainModel)
async def get_charts_statistics(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    start_date: Optional[date] = Query(None, alias="start_date"),
    end_date: Optional[date] = Query(None, alias="end_date"),
    locations: Optional[List[str]] = Query(None, alias="locations"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    allow_paging = False if paging is not None and paging.lower() == 'false' else True
    response = await fetch_charts_statistics(paging=allow_paging, page_number=page_number, limit=limit, start_date=start_date, end_date=end_date, locations=locations, db=db)
    return response

