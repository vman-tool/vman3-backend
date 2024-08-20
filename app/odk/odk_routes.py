


from arango.database import StandardDatabase
from fastapi import (APIRouter, BackgroundTasks, Depends, HTTPException, Query,
                     status)

from app.odk.services import data_download
from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.models import ResponseMainModel

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




@odk_router.post("/fetch_endpoint_with_async", status_code=status.HTTP_200_OK)
async def fetch_odk_data_with_async_endpoint(
    background_tasks: BackgroundTasks,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    force_update: bool = Query(default=False),
    db: StandardDatabase = Depends(get_arangodb_session)
):
    try:
        # Wrap the async function call inside an async function to use create_task
        async def start_fetch():
            await data_download.fetch_odk_data_with_async(
                db=db,
                start_date=start_date,
                end_date=end_date,
                skip=skip,
                top=top,
                force_update=force_update
            )

        # Add this wrapped task to background tasks
        background_tasks.add_task(start_fetch)
        return {"status": "Data fetch initiated"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@odk_router.get("/get_form_questions", status_code=status.HTTP_200_OK)
async def get_form_questions(
    db: StandardDatabase = Depends(get_arangodb_session)
) -> ResponseMainModel:
    try:
        return await data_download.fetch_form_questions(db=db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





#TODOS: EXPERIMENTAL CODES: Do not delete
# @odk_router.post("/fetch_endpoint_with_async_old", status_code=status.HTTP_201_CREATED)
# async def fetch_odk_data(background_tasks: BackgroundTasks,  start_date: str = None,
#     end_date: str = None,
#     skip: int = 0,
#     top: int = 3000,      db: StandardDatabase = Depends(get_arangodb_session)):
#     try:
#         # await data_download_old.fetch_odk_data_with_async_old(
#         #         db=db,
#         #         start_date=start_date,
#         #         end_date=end_date,
#         #         skip=skip,
#         #         top=top)
#         background_tasks.add_task(
#                 data_download_old.fetch_odk_data_with_async_old,
#                 db=db,
#                 start_date=start_date,
#                 end_date=end_date,
#                 skip=skip,
#                 top=top
#             )
#         return {"status": "Data fetched sucessfuly"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @odk_router.get("/retry-failed-chunks",status_code=status.HTTP_200_OK)
# async def retry_failed_chunks_endpoint():
#     try:
#         await data_download.retry_failed_chunks()
#         return {"status": "Retries initiated for failed chunks"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

##TODOS: EXPERIMENTAL CODES: Do not delete