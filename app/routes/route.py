
from fastapi import APIRouter, BackgroundTasks, status

from app.services import data_download

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
    top: int = 100,
):
    
    res= await data_download.fetch_odk_datas(
          start_date= start_date,
    end_date=end_date,
    skip=skip,
    top=top
    )
    return res
    
    
    
    
@odk_router.post("/fetch_endpoint", status_code=status.HTTP_201_CREATED)
async def fetch_data_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(data_download.fetch_odk_data_with_memory)
    return {"message": "Data fetch initiated in the background"}

  