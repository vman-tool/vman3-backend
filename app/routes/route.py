
from fastapi import APIRouter, HTTPException, status
from loguru import logger

from app.configs.database import db
from app.utilits.utils import ODKClient
import json
from pandas import json_normalize

odk_router = APIRouter(
    prefix="/odk",
    tags=["odk"],
    responses={404: {"description": "Not found"}},
)




@odk_router.post("/fetch-and-store", status_code=status.HTTP_201_CREATED)
async def fetch_and_store_data(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 10,
    top: int = 10,
):
    if start_date is None:
        start_date = '2021-01-01'  # Replace with your desired default start date
    if end_date is None:
        end_date = '2024-06-06' 
            
    logger.info(f"Fetching data from ODK between {start_date} and {end_date}")
    with ODKClient() as odk_client:
        if skip is None:
            pass
        data = odk_client.getFormSubmissions(start_date, end_date, top=top, skip=skip)

        total_data_count = data["@odata.count"]


        while (skip < total_data_count):
            percent = total_data_count / 100
            print("Get more data")

            skip += top

        json_flattened = json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))
        with open("flattened.json", 'w') as file:
            print(total_data_count)
            json.dump(json_flattened, file)
        

    if isinstance(data, str):
        logger.error(f"Error fetching data: {data}")
        raise HTTPException(status_code=500, detail=data)
    # collection = db["form_submissions"]
    # await collection.insert_many(json_flattened)
    logger.info(f"Data inserted successfully: {len(data['value'])} records")
    return {"status": "Data inserted successfully"}
