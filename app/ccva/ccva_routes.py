
from app.users.decorators.user import get_current_user, oauth2_scheme
from fastapi import APIRouter, Depends, status


ccva_router = APIRouter(
    prefix="/ccva",
    tags=["CCVA"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

@ccva_router.get("/", status_code=status.HTTP_200_OK)
async def get_va_records():
    return {"message":"ccva works"}
    