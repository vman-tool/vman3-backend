from io import BytesIO
import json
from typing import Any, Dict, List, Optional, Union

from arango.database import StandardDatabase
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect, status, Request
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
    PCVAResultsRequestClass,
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
    export_pcva_results,
    get_coded_va_details,
    get_coded_va_service,
    get_coders,
    get_concordants_va_service,
    get_configurations_service,
    get_discordant_messages_service,
    get_discordants_va_service,
    get_form_questions_service,
    get_pcva_results,
    get_unassigned_va_service,
    get_va_assignment_service,
    get_uncoded_assignment_service,
    read_discordants_message,
    save_configurations_service,
    save_discordant_message_service,
    unassign_va_service,
)
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel
from app.users.decorators.user import get_current_user, get_current_user_ws, oauth2_scheme
from app.users.models.user import User
from app.shared.services.va_records import shared_fetch_va_records
from app.pcva.requests.configurations_request_classes import PCVAConfigurationsRequest

pcva_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

pcva_socket_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
)


@pcva_router.get("", status_code=status.HTTP_200_OK)
async def get_va_records(
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_assignment: Optional[str] = Query(None, alias="include_assignment"),
    format_records: Optional[bool] = Query(True, alias="format_records"),
    va_id: Optional[str] = Query(None, alias="va_id"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = paging if paging is not None else True
        include_assignment = False if include_assignment is not None and include_assignment.lower() == 'false' else True
        filters = {}
        if va_id is not None:
            filters = {
                "instanceid": va_id
            }
        return await shared_fetch_va_records(paging = allowPaging, page_number = page_number, limit = limit, include_assignment = include_assignment, filters=filters, format_records=format_records, db=db)
        
    except Exception as e:
        raise e

@pcva_router.get("/get-unassigned-va", status_code=status.HTTP_200_OK)
async def get_unassigned_va_records(
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    coder: Optional[str] = Query(None, alias="coder"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = paging if paging is not None else True
        
        return await get_unassigned_va_service(paging = allowPaging, page_number = page_number, limit = limit, coder = coder, db=db)
        
    except Exception as e:
        raise e

@pcva_router.get("/get-uncoded-assigned-va", status_code=status.HTTP_200_OK)
async def get_va_for_unnassignment(
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    coder: Optional[str] = Query(None, alias="coder"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = paging if paging is not None else True
        
        return await get_uncoded_assignment_service(paging = allowPaging, page_number = page_number, limit = limit, coder = coder, db=db)
        
    except Exception as e:
        raise e


@pcva_router.get(
        path="/icd10-categories", 
        status_code=status.HTTP_200_OK,
)
async def get_icd10_categories(
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = paging if paging is not None else True
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
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

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
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:
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
    ) -> ResponseMainModel:
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
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        allowPaging = paging if paging is not None else True
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
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

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
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:
    try:
        return await update_icd10_codes(codes, user, db)
    except:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed update icd10 codes")

@pcva_router.get("/coders", status_code=status.HTTP_200_OK)
async def get_assigned_va(
    paging: Optional[bool] = Query(False, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[bool] = Query(False, alias="include_deleted"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:
    try:
        return await get_coders(
            paging = paging,
            page_number = page_number,
            limit = limit,
            include_deleted = include_deleted,
            current_user = current_user,
            db = db
        )
    except Exception as e:
         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get coders: {e}")

@pcva_router.get("/va-assignments", status_code=status.HTTP_200_OK)
async def get_assigned_va(
    paging: Optional[bool] = Query(None, alias="paging"),
    page_number: Optional[int] = Query(1, alias="page_number"),
    limit: Optional[int] = Query(10, alias="limit"),
    include_deleted: Optional[str] = Query(None, alias="include_deleted"),
    va_id: Optional[str] = Query(None, alias="va_id"),
    coder: Optional[str] = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        filters = {}
        if coder: 
            filters['coder'] = coder
        allowPaging = paging if paging is not None else True
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

@pcva_router.post(
        "/unassign-va", 
        status_code=status.HTTP_201_CREATED, 
        description="vaIds should be array of vaId. coder must have a user uuid")
async def unassign_va(
    vaAssignment: AssignVARequestClass,
    user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):

    try:
        return await unassign_va_service(vaAssignment, User(**user), db)    
    except Exception as e:
        raise e

@pcva_router.get("/get-coded-va", status_code=status.HTTP_200_OK)
async def get_coded_va(
    coder: Optional[str] = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        return  await get_coded_va_service(
            paging=True, 
            page_number=1,
            limit=10,
            coder = coder,
            current_user = User(**current_user), 
            db = db)
    except Exception as e:
        raise e

@pcva_router.get("/get-coded-va-details/{va_id}", status_code=status.HTTP_200_OK)
async def get_coded_va(
    va_id: str,
    coder: Optional[str] = Query(None, alias="coder"),
    include_history: Optional[bool] = Query(False, alias="include_history"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:

    try:
        return  await get_coded_va_details(
            paging=True, 
            page_number=1,
            limit=10,
            va=va_id,
            coder = coder,
            include_history = include_history,
            current_user = User(**current_user), 
            db = db)
    except Exception as e:
        raise e

@pcva_router.post("/code-assigned-va", status_code=status.HTTP_200_OK)
async def code_assigned_va(
    coded_va: PCVAResultsRequestClass,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)) -> ResponseMainModel:
    try:
        return  await code_assigned_va_service(coded_va, current_user = User(**current_user), db = db)
    except Exception as e:
        raise e

@pcva_router.get("/get-concordants", status_code=status.HTTP_200_OK)
async def get_concordants(
    paging: bool = Query(None, alias='paging'),
    page_number: int = Query(1, alias='page_number'),
    limit: int = Query(10, alias='limit'),
    coder: str = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await get_concordants_va_service(paging = paging, page_number = page_number, limit = limit, coder = coder, db = db)
    except Exception as e:
        raise e

@pcva_router.get("/get-discordants", status_code=status.HTTP_200_OK)
async def get_discordants(
    paging: bool = Query(None, alias='paging'),
    page_number: int = Query(1, alias='page_number'),
    limit: int = Query(10, alias='limit'),
    coder: str = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await get_discordants_va_service(paging = paging, page_number = page_number, limit = limit, coder = coder, db = db)
    except Exception as e:
        raise e
    

@pcva_router.get("/get-discordant-messages/{va_id}", status_code=status.HTTP_200_OK)
async def get_discordant_messages(
    va_id: str,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await get_discordant_messages_service(va_id=va_id, coder=current_user.get("uuid", ""), db = db)
    except Exception as e:
        raise e
    
@pcva_router.post("/messages/{va_id}/read")
async def mark_message_as_read(
    va_id: str,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
    ):
    try:
        return await read_discordants_message(va_id=va_id, user_uuid=current_user.get("uuid", ""), db=db)
    except Exception as e:
        raise e                           

@pcva_router.get("/get-pcva-results", status_code=status.HTTP_200_OK)
async def get_coded_vas(
    paging: bool = Query(None, alias='paging'),
    page_number: int = Query(1, alias='page_number'),
    limit: int = Query(10, alias='limit'),
    coder: str = Query(None, alias="coder"),
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await get_pcva_results(paging = paging, page_number = page_number, limit = limit, db = db)
    except Exception as e:
        raise e

@pcva_router.get("/export-pcva-results", status_code=status.HTTP_200_OK)
async def export_coded_vas(
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)):
    try:
        return  await export_pcva_results(db = db)
    except Exception as e:
        raise e
    

@pcva_router.get("/form-questions", status_code=status.HTTP_200_OK)
async def get_form_questions(
    questions_keys: Optional[str] = Query(None, alias="question_id", description="If you need many, separate questons keys by comma, Do not specify to get all the questions"),
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
    

@pcva_router.post("/save-configurations", status_code=status.HTTP_200_OK)
async def save_configurations(
    configs: PCVAConfigurationsRequest,
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
) -> ResponseMainModel:
    try:
        return await save_configurations_service(configs=configs, current_user=User(**current_user), db=db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@pcva_router.get("/get-configurations", status_code=status.HTTP_200_OK)
async def get_configurations(
    current_user: User = Depends(get_current_user),
    db: StandardDatabase = Depends(get_arangodb_session)
) -> ResponseMainModel:
    try:
        return await get_configurations_service(db=db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))