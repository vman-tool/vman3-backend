
from fastapi import APIRouter, HTTPException, status
from loguru import logger

from app.configs.database import db
from app.utilits.utils import ODKClient
import json
from pandas import json_normalize # type: ignore

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
    top: int = 10,
):
    
    if start_date or end_date:
        logger.info(f"\nFetching data from ODK between {start_date} and {end_date} \n\n")
    else:
        logger.info(f"\nFetching data from ODK Central \n\n")

    with ODKClient() as odk_client:
        data_for_count = odk_client.getFormSubmissions(top=1)

        total_data_count = data_for_count["@odata.count"]

        num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)

        records_saved = 0
        last_progress = 0

        for i in range(num_iterations):

            data = odk_client.getFormSubmissions(start_date, end_date, top=top, skip=skip)

            json_flattened = json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))

            if isinstance(data, str):
                logger.error(f"Error fetching data: {data}")
                raise HTTPException(status_code=500, detail=data)
            collection = db["form_submissions"]
            await collection.insert_many(json_flattened)

            skip += top

            progress = ((i + 1) / num_iterations) * 100
            records_saved += len(json_flattened)

            if int(progress) != last_progress:
                last_progress = int(progress)
                print(f"\rDownloading: [{'*' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}%", end='')


    if records_saved == total_data_count:
        logger.info(f"\n Data inserted successfully: {records_saved} records")
        return {"status": "Data inserted successfully"}
    else:
        logger.info(f"\n Successfully inserted {records_saved} records while records were {total_data_count}")
        return {"status": "Data inserted with issues"}
