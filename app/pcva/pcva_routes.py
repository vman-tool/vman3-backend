from io import BytesIO
import json
from typing import Any, Dict, List, Optional, Union

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
import pandas as pd

from app.pcva.requests.icd10_request_classes import (
    ICD10CategoryRequestClass,
    ICD10CategoryUpdateClass,
    ICD10CreateRequestClass,
    ICD10UpdateRequestClass,
)
from app.pcva.requests.va_request_classes import (
    AssignVARequestClass,
    CodeAssignedVARequestClass,
)
from app.pcva.responses.icd10_response_classes import (
    ICD10CategoryResponseClass,
    ICD10ResponseClass,
)
from app.pcva.responses.va_response_classes import CodedVAResponseClass
from app.pcva.services.icd10_services import (
    create_icd10_categories_service,
    create_icd10_codes,
    create_or_icd10_codes_from_file,
    get_icd10_categories_service,
    get_icd10_codes,
    update_icd10_categories_service,
    update_icd10_codes,
)
from app.pcva.services.va_records_services import (
    assign_va_service,
    code_assigned_va_service,
    get_coded_va_service,
    get_concordants_va_service,
    get_form_questions_service,
    get_va_assignment_service,
)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import get_current_user, oauth2_scheme
from app.users.models.user import User
from app.shared.services.va_records import shared_fetch_va_records

pcva_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)


@pcva_router.get("", status_code=status.HTTP_200_OK)
async def get_va_records(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_assignment: Optional[str] = Query(None, alias="include_assignment"),
    format_records: Optional[bool] = Query(True, alias="format_records"),
    va_id: Optional[str] = Query(None, alias="va_id"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_assignment = False if include_assignment is not None and include_assignment.lower() == 'false' else True
        filters = {}
        if va_id is not None:
            filters = {
                "instanceid": va_id
            }
        return await shared_fetch_va_records(paging = allowPaging, page_number = page_number, limit = limit, include_assignment = include_assignment, filters=filters, format_records=format_records, db=db)
        
    except Exception as e:
        raise e


@pcva_router.get(
        path="/icd10-categories", 
        status_code=status.HTTP_200_OK,
)
async def get_icd10_categories(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> Union[List[ICD10CategoryResponseClass], Any]:

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = False if include_deleted is not None and include_deleted.lower() == 'false' else True
        return await get_icd10_categories_service(
            paging = allowPaging, 
            page_number = page_number, 
            limit = limit, 
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

@pcva_router.post(
        path="/upload-icd10-data", 
        status_code=status.HTTP_200_OK,
        description="Update categories in excel or csv or json format"
)
async def upload_file(
    file: UploadFile = File(),
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
    ):
    try:
        file_extension = file.filename.split(".")[-1].lower()

        content = await file.read()
        file_content = BytesIO(content)

        if file_extension == "csv":
            df = pd.read_csv(file_content)
        elif file_extension == "xlsx" or file_extension == "xls":
            df = pd.read_excel(file_content)
        elif file_extension == "json":
            data = json.loads(content)
            df = pd.json_normalize(data)
        else:
            raise HTTPException(status_code=400, detail="Invalid file type")
        
        data_dictionary = df.head().to_dict(orient="records")
        return await create_or_icd10_codes_from_file(data_dictionary, user, db)
    except Exception as e:
        raise e

@pcva_router.get(
        path="/get-icd10", 
        status_code=status.HTTP_200_OK,
        description="Submit array of icd10 codes in a json format"
)
async def get_icd10(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> Union[List[ICD10ResponseClass], Any]:

    try:
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = False if include_deleted is not None and include_deleted.lower() == 'false' else True
        return await get_icd10_codes(
            paging = allowPaging, 
            page_number = page_number, 
            limit = limit, 
            include_deleted = include_deleted, 
            db = db)
    except Exception as e:
        raise e

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

@pcva_router.get("/va-assignments", status_code=status.HTTP_200_OK)
async def get_assigned_va(
    paging: Optional[str] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    va_id: Optional[str] = Query(None, alias="va_id"),
    coder: Optional[str] = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> Union[List[ResponseMainModel], Any]:

    try:
        filters = {}
        if va_id:
            filters['vaId'] = va_id
        if coder: 
            filters['coder'] = coder
        allowPaging = False if paging is not None and paging.lower() == 'false' else True
        include_deleted = True if include_deleted is not None and include_deleted.lower() == 'true' else False
        
        return await get_va_assignment_service(allowPaging, page_number, limit, include_deleted, filters, current_user, db)    
    except Exception as e:
        raise e


@pcva_router.post(
        "/assign-va", 
        status_code=status.HTTP_201_CREATED, 
        description="vaIds should be array of vaId, new_coder (user uuid) must be there and if you're not updating, then leave it empty. coder must have a user uuid")
async def assign_va(
    vaAssignment: AssignVARequestClass,
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        return await assign_va_service(vaAssignment, User(**user), db)    
    except Exception as e:
        raise e

@pcva_router.get("/get-coded-va", status_code=status.HTTP_200_OK)
async def get_coded_va(
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> Union[List[CodedVAResponseClass],Dict]:

    try:
        return  await get_coded_va_service(
            paging=True, 
            page_number=1,
            limit=10,
            user = User(**current_user), 
            db = db)
    except Exception as e:
        raise e

@pcva_router.post("/code-assigned-va", status_code=status.HTTP_200_OK)
async def code_assigned_va(
    coded_va: CodeAssignedVARequestClass,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> Union[CodedVAResponseClass,Dict]:
    try:
        return  await code_assigned_va_service(coded_va, current_user = User(**current_user), db = db)
    except Exception as e:
        raise e

@pcva_router.get("/get_concordant_vas", status_code=status.HTTP_200_OK)
async def code_assigned_va(
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await get_concordants_va_service(current_user, db = db)
    except Exception as e:
        raise e
    

@pcva_router.get("/form_questions", status_code=status.HTTP_200_OK)
async def get_form_questions(
    questions_keys: Optional[str] = Query(None, alias="question_id", description="If you need many, separate questons keys by comma, DOnt specify to get all the questions"),
    current_user: User = Depends(get_arangodb_session),
    db: StandardDatabase = Depends(get_arangodb_session)
) -> ResponseMainModel:
    try:
        fields_to_be_queried = {"name": questions_keys.split(',')} if questions_keys else []
        
        
        filters = {
            "in_conditions": fields_to_be_queried
        } if questions_keys else {}

        return await get_form_questions_service(filters=filters, db=db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))