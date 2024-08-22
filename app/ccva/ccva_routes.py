
from app.users.decorators.user import get_current_user, oauth2_scheme
from fastapi import APIRouter, Depends, status
from fastapi import UploadFile, File, HTTPException

import csv


ccva_router = APIRouter(
    prefix="/ccva",
    tags=["CCVA"],
    responses={404: {"description": "Not found"}},
    dependencies=[Depends(oauth2_scheme), Depends(get_current_user)]
)

@ccva_router.get("/", status_code=status.HTTP_200_OK)
async def get_va_records():
    return {"message":"ccva works"}

@ccva_router.post("/upload_csv", status_code=status.HTTP_200_OK)
async def upload_csv(file: UploadFile = File(...)):
    # Check if file exist
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file format")
    
    content = await file.read()
    csv_data = content.decode("utf-8").splitlines()
    csv_reader = csv.reader(csv_data)

    return {"message":"file uploaded"}
    