from fastapi import APIRouter
from arango.database import StandardDatabase
from fastapi import APIRouter, BackgroundTasks, Depends, Header, status

from app.pcva.services.va_records import check_va_data
from app.shared.configs.arangodb_db import get_arangodb_session


pcva_router = APIRouter(
    prefix="/pcva",
    tags=["PCVA"],
    responses={404: {"description": "Not found"}},
)


@pcva_router.get("/", status_code=status.HTTP_201_CREATED)

async def get_va_records(data, background_tasks: BackgroundTasks, db: StandardDatabase = Depends(get_arangodb_session)):
    return check_va_data()
