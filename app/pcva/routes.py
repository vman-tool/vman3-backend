from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, status

from app.pcva.requests.icd10_request_classes import ICD10CategoryRequestClass, ICD10CategoryUpdateClass, ICD10CreateRequestClass, ICD10UpdateRequestClass
from app.pcva.requests.va_request_classes import AssignVARequestClass, CodeAssignedVARequestClass
from app.pcva.responses.icd10_response_classes import ICD10CategoryResponseClass, ICD10ResponseClass
from app.pcva.responses.va_response_classes import AssignVAResponseClass, CodedVAResponseClass
from app.pcva.services.icd10_services import (
    create_icd10_categories_service,
    get_icd10_categories_service, 
    get_icd10_codes,
    create_icd10_codes,
    update_icd10_categories_service, 
    update_icd10_codes
)
from app.pcva.services.va_records_services import assign_va_service, code_assigned_va_service, fetch_va_records, get_coded_va_service, get_va_assignment_service
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


@pcva_router.get(
        path="/get-icd10-categories", 
        status_code=status.HTTP_200_OK,
)
async def get_icd10_categories(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    page_size: Optional[int] = Query(10, alias="page_size"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10CategoryResponseClass] | Any:

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = False if include_deleted is not None and include_deleted.lower() == 'false' else True
        return await get_icd10_categories_service(
            paging = allowPaging, 
            page_number = page_number, 
            page_size = page_size, 
            include_deleted = include_deleted, 
            db = db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed get icd10 categories")


@pcva_router.post("/create-icd10-categories", status_code=status.HTTP_201_CREATED)
async def create_icd10_categories(
    categories: List[ICD10CategoryRequestClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10CategoryResponseClass]:

    try:
        return await create_icd10_categories_service(categories, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed create icd10 categories")


@pcva_router.post(
        path="/update-icd10-categories", 
        status_code=status.HTTP_200_OK,
        description="Submit array of icd10 categories in a json format"
)
async def update_icd10_categories(
    categories: List[ICD10CategoryUpdateClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10CategoryResponseClass]:
    try:
        return await update_icd10_categories_service(categories, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed update icd10 codes")



@pcva_router.get(
        path="/get-icd10", 
        status_code=status.HTTP_200_OK,
        description="Submit array of icd10 codes in a json format"
)
async def get_icd10(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    page_size: Optional[int] = Query(10, alias="page_size"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10ResponseClass] | Any:

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = False if include_deleted is not None and include_deleted.lower() == 'false' else True
        return await get_icd10_codes(
            paging = allowPaging, 
            page_number = page_number, 
            page_size = page_size, 
            include_deleted = include_deleted, 
            db = db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed get icd10 codes")

@pcva_router.post(
        path="/create-icd10", 
        status_code=status.HTTP_201_CREATED,
        response_description="Submit array of icd10 codes in a json format"
)
async def create_icd10(
    codes: List[ICD10CreateRequestClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10ResponseClass]:

    try:
        return await create_icd10_codes(codes, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed create icd10 codes")

@pcva_router.post(
        path="/update-icd10", 
        status_code=status.HTTP_200_OK,
        response_description="Submit array of icd10 codes in a json format"
)
async def update_icd10(
    codes: List[ICD10UpdateRequestClass],
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[ICD10ResponseClass]:
    try:
        return await update_icd10_codes(codes, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed update icd10 codes")


@pcva_router.post("/assign-va", status_code=status.HTTP_201_CREATED)
async def assign_va(
    vaAssignment: AssignVARequestClass,
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        return await assign_va_service(vaAssignment, user, db)    
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to assign va")

@pcva_router.get("/va-assignments", status_code=status.HTTP_200_OK)
async def get_assigned_va(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    page_size: Optional[int] = Query(10, alias="page_size"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    va_id: Optional[str] = Query(None, alias="va_id"),
    coder1: Optional[str] = Query(None, alias="coder1"),
    coder2: Optional[str] = Query(None, alias="coder2"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[AssignVAResponseClass] | Dict:

    try:
        filters = {}
        if va_id:
            filters['vaId'] = va_id
        if coder1: 
            filters['coder1'] = coder1
        if coder2:
            filters['coder2'] = coder2
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = False if include_deleted is not None and include_deleted.lower() == 'false' else True

        if coder1 and coder2:
            filters["or_conditions"]= [
                {"coder1": coder1},
                {"coder2": coder2}
            ]
            filters.pop('coder1', None)
            filters.pop('coder2', None)
        
        return await get_va_assignment_service(allowPaging, page_number, page_size, include_deleted, filters, current_user, db)    
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to get assigned va")

@pcva_router.get("/get-coded-va", status_code=status.HTTP_200_OK)
async def get_coded_va(
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> List[CodedVAResponseClass] | Dict:

    try:
        return  await get_coded_va_service(
            paging=True, 
            page_number=1,
            page_size=10,
            user = User(**current_user), 
            db = db)
    except Exception as e:
        raise e

@pcva_router.post("/code-assigned-va", status_code=status.HTTP_200_OK)
async def code_assigned_va(
    coded_va: CodeAssignedVARequestClass,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> CodedVAResponseClass | Dict:

    print(coded_va)

    return  await code_assigned_va_service(coded_va, current_user = User(**current_user), db = db)
    # try:
    # except Exception as e:
    #     raise e