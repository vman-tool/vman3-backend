from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from loguru import logger

from app import routes
from app.odk_download.services import data_download, schedulers
from app.shared.configs.arangodb_db import get_arangodb_client
from app.shared.configs.database import (close_mongo_connection,
                                         connect_to_mongo)

logger.add("./../app.log", rotation="500 MB")
scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup logic
    logger.info("Application startup")
    print("Visit http://localhost:8080/api/docs for the API documentation (Swagger UI)")
    print("Visit http://localhost:8080/api for the main API")
 
    scheduler.add_job(schedulers. scheduled_failed_chucks_retry, IntervalTrigger(minutes=60*3))
    scheduler.add_job(data_download. fetch_odk_data_with_async, CronTrigger(hour=18, minute=0))
    scheduler.start()
        # Initialize ArangoDB connection

    # Initialize MongoDB connection
    await connect_to_mongo()
    arango_client= await get_arangodb_client()
    app.state.arango_client = arango_client
    
    try:
        yield
    finally:
        # Application shutdown logic
        logger.info("Application shutdown")
        
        # Close MongoDB connection
        await close_mongo_connection()

def create_application():
    application = FastAPI(
        title="vman3",
        version="1.0.0",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan
    )
    routes.main_route(application)
    return application

app = create_application()



@app.get("/")
async def root():
    return {"message": "Hello World"}

# To run the app, use the following command:
# uvicorn main:app --reload# To run the app, use the following command:
# uvicorn main:app --reload