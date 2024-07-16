from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, status

from app.pcva.services.va_records import fetch_va_records
from app.shared.configs.arangodb_db import get_arangodb_session


pcva_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
)


@pcva_router.get("/", status_code=status.HTTP_200_OK)
async def get_va_records(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    page_size: Optional[int] = Query(10, alias="page_size"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        records = await fetch_va_records(allowPaging, page_number, page_size, db)
        return records
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to get va records")