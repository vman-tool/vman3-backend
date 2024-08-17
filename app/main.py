from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from decouple import config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from loguru import logger

from app import routes
from app.odk_download.services import data_download, schedulers
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_client
from app.shared.configs.database import (close_mongo_connection,
                                         connect_to_mongo)
from app.users.utils.default import default_account_creation

logger.add("./../app.log", rotation="500 MB")
scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup logic
    logger.info("Application startup")
    scheduler.add_job(schedulers. scheduled_failed_chucks_retry, IntervalTrigger(minutes=60*3))
    scheduler.add_job(data_download. fetch_odk_data_with_async, CronTrigger(hour=18, minute=0))
    scheduler.start()

    # Initialize MongoDB connection
    await connect_to_mongo()


    
    # Initialize ArangoDB connection
    arango_client= await get_arangodb_client()
    await ArangoDBClient.create_collections(arango_client)
    app.state.arango_client = arango_client
    await default_account_creation()
    
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
        docs_url="/api/v1/docs",
        openapi_url="/api/v1/openapi.json",
        lifespan=lifespan
    )
    routes.main_route(application)
    return application

app = create_application()

origins = config('CORS_ALLOWED_ORIGINS', default="*").split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# force_https_redirect = config("FORCE_HTTPS_REDIRECT", default=False, cast=bool)

# if force_https_redirect:
#     app.add_middleware(HTTPSRedirectMiddleware)



@app.get("/")
async def root():
    return {"message": "Hello World"}

# To run the app, use the following command:
# uvicorn main:app --reload# To run the app, use the following command:
# uvicorn main:app --reload