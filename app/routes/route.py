
from fastapi import APIRouter, HTTPException, status
from loguru import logger

from app.configs.database import db
from app.utilits.odk_client import ODKClient

odk_router = APIRouter(
    prefix="/odk",
    tags=["odk"],
    responses={404: {"description": "Not found"}},
)




@odk_router.post("/fetch-and-store", status_code=status.HTTP_201_CREATED)
async def fetch_and_store_data(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 1000,
):
    if start_date is None:
        start_date = '2021-01-01'  # Replace with your desired default start date
    if end_date is None:
        end_date = '2024-06-06' 
            
    logger.info(f"Fetching data from ODK between {start_date} and {end_date}")
    with ODKClient() as odk_client:
        data = odk_client.get_form_submissions(start_date, end_date, top=top, skip=skip)

    if isinstance(data, str):
        logger.error(f"Error fetching data: {data}")
        raise HTTPException(status_code=500, detail=data)
    collection = db["form_submissions"]
    await collection.insert_many(data['value'])
    logger.info(f"Data inserted successfully: {len(data['value'])} records")
    return {"status": "Data inserted successfully"}
