from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from decouple import config
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from loguru import logger

from app import routes
from app.odk_download.services import data_download, schedulers
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_client
from app.shared.configs.database import (close_mongo_connection,
                                         connect_to_mongo)
from app.users.utils.default import default_account_creation
from app.utilits import websocket_manager

logger.add("./../app.log", rotation="500 MB")
scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup logic
    logger.info("Application startup")
    logger.info("Visit http://localhost:8080/vman/api/v1/docs for the API documentation (Swagger UI)")
    logger.info("Visit http://localhost:8080/vman/api/v1 for the main API")
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
        docs_url="/vman/api/v1/docs",
        openapi_url="/api/v1/openapi.json",
        lifespan=lifespan
    )
    routes.main_route(application)
    return application

app = create_application()
websocket__manager = websocket_manager.WebSocketManager()

origins = config('CORS_ALLOWED_ORIGINS', default="*").split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

        
@app.get("/vman/api/v1", response_class=HTMLResponse)
@app.get("/vman", response_class=HTMLResponse)
@app.get("/", response_class=HTMLResponse)
@app.get("/vman/api", response_class=HTMLResponse)
async def get():
    """
    Welcome to the vman3 API
    """
    return HTMLResponse("""
        <!DOCTYPE html>
        <html>
            <head>
                <meta charset="utf-8">
                <meta http-equiv="X-UA-Compatible" content="IE=edge">
                <title></title>
                <meta name="description" content="">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <link rel="stylesheet" href="">
                <style>
                    .text-center {
                        text-align: center;
                    }        
                </style>
            </head>
            <body>
                <h1 class="text-center">Welcome to the VMan API</h1>
                <p class="text-center">To view documentation click to <a href="/vman/api/v1/docs">here</a> and enjoy</p>
            </body>
        </html>
    """)



@app.websocket("/vman/api/v1/ws/download_progress")
async def websocket_download_progress(websocket: WebSocket):
    await websocket__manager.connect(websocket)
    try:
        while True:
             data=await websocket.receive_text()  # Keep the connection open
             await websocket.send_text(f"Message text was: {data}")
    except WebSocketDisconnect:
        websocket__manager.disconnect(websocket)