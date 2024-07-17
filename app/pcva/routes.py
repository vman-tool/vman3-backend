from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, status

from app.pcva.requests.icd10 import ICD10CategoryRequestClass, ICD10RequestClass
from app.pcva.services.icd10_services import create_icd10_categories, create_icd10_codes
from app.pcva.services.va_records import fetch_va_records
from app.shared.configs.arangodb_db import get_arangodb_session
from app.users.decorators.user import get_current_user, oauth2_scheme
from app.users.models.user import User


pcva_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)


@pcva_router.get("/", status_code=status.HTTP_200_OK)
async def get_va_records(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    page_size: Optional[int] = Query(10, alias="page_size"),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        return await fetch_va_records(allowPaging, page_number, page_size, db)
        
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to get va records")

@pcva_router.post("/create-icd10-categories", status_code=status.HTTP_201_CREATED)
async def create_icd10_categories(
    categories: List[ICD10CategoryRequestClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        return await create_icd10_categories(categories, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed create icd10 categories")


@pcva_router.post("/create-icd10", status_code=status.HTTP_201_CREATED)
async def create_icd10(
    codes: List[ICD10RequestClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        return await create_icd10_codes(codes, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed create icd10 codes")