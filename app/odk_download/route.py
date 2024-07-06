

from fastapi import APIRouter, BackgroundTasks, HTTPException, status

from app.odk_download.services import data_download

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
    
    


@odk_router.post("/fetch_endpoint_with_async", status_code=status.HTTP_201_CREATED)
async def fetch_odk_data_with_async(background_tasks: BackgroundTasks,  start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 10000):
    try:
        await data_download.fetch_odk_data_with_async(       start_date= start_date,
    end_date=end_date,
    skip=skip,
    top=top)
        return {"status": "Data fetched sucessfuly"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@odk_router.get("/retry-failed-chunks",status_code=status.HTTP_200_OK)
async def retry_failed_chunks_endpoint():
    try:
        await data_download.retry_failed_chunks()
        return {"status": "Retries initiated for failed chunks"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))     