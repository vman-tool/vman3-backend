from contextlib import asynccontextmanager
import json

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from arango.database import StandardDatabase
from decouple import config
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from loguru import logger
from app.utilits.db_logger import DBLogger, db_logger, get_db_logger
from app import routes
from app.shared.middlewares.error_handlers import register_error_handlers
from app.users.utils.default import default_account_creation
from app.utilits import websocket_manager
from app.shared.configs.arangodb import get_arangodb_session
from app.users.decorators.user import get_current_user_ws
from app.users.models.user import User
from app.pcva.services.va_records_services import save_discordant_message_service
from app.utilits.schedeular import shutdown_scheduler, start_scheduler


# logger.add("./logs/app.log", rotation="500 MB")
# scheduler = AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup logic
    logger.info("Application startup")
    logger.info("Visit http://localhost:8080/vman/api/v1/docs for the API documentation (Swagger UI)")
    logger.info("Visit http://localhost:8080/vman/api/v1 for the main API")

    # scheduler.add_job(schedulers. scheduled_failed_chucks_retry, IntervalTrigger(minutes=60*3))
    # scheduler.add_job(data_download. fetch_odk_data_with_async, CronTrigger(hour=18, minute=0))
    # scheduler.start()
<<<<<<< Updated upstream
    # await db_logger.ensure_collection()
    await get_db_logger()
    await start_scheduler()
=======
    await db_logger.ensure_collection()
    
    # await start_scheduler()
>>>>>>> Stashed changes
    await default_account_creation()
    
    try:
        yield
    finally:
        # Application shutdown logic
        logger.info("Application shutdown")
            # Shutdown the scheduler
    # shutdown_scheduler()
    
    # Flush any remaining logs
    # await db_logger.flush_buffer()
<<<<<<< Updated upstream
    scheduler.shutdown()
=======
    # scheduler.shutdown()
>>>>>>> Stashed changes
        

def create_application():
    application = FastAPI(
        title="vman3",
        version="1.0.0",
        docs_url="/vman/api/v1/docs",
        openapi_url="/vman/api/v1/openapi.json",
        lifespan=lifespan
    )
    routes.main_route(application)
  
    return application

app = create_application()
register_error_handlers(app)
websocket__manager = websocket_manager.WebSocketManager()
app.state.websocket__manager = websocket__manager

origins = config('CORS_ALLOWED_ORIGINS', default="*").split(',')
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#  Start the scheduler when the application starts
# @app.on_event("startup")
# async def startup_event():

#     # Log application startup
#     app_logger.info("Application starting up")
#     await db_logger.log(
#         message="Application starting up",
#         level="INFO",
#         context="startup_event",
#         module="app.main"
#     )
    
#     # Start the scheduler

    
#     app_logger.info("Application startup complete")
# db_logger = DBLogger()
# # Add the middleware
# app.add_middleware(GlobalErrorMiddleware)

    
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
                <h1 class="text-center">Welcome to the VMan API v3</h1>
                <p class="text-center">To view documentation click to <a href="/vman/api/v1/docs">here</a> and enjoy</p>
            </body>
        </html>
    """)



# @app.websocket("/ws")
# async def websocket_download_progress(websocket: WebSocket):
#     await websocket__manager.connect(websocket)
#     try:
#         while True:
#              data=await websocket.receive_text()  # Keep the connection open
#              await websocket.send_text(f"Message text was: {data}")
#     except WebSocketDisconnect:
#         await websocket__manager.disconnect(websocket)



# @app.websocket("/vman/api/v1/ws/ccva_progress/{task_id}")
# async def websocket_va_1(websocket: WebSocket, task_id: str):
#     await websocket__manager.connect(task_id, websocket)
#     try:
#         while True:
#             data = await websocket.receive_text()
#             # Handle incoming data here if needed
#             await websocket__manager.send_personal_message(f"Message received for task {task_id}: {data}", websocket)
#     except WebSocketDisconnect:
#         await websocket__manager.disconnect(task_id, websocket)

# @app.websocket("/vman/api/v1/ws/odk_progress/{task_id}")
# async def odk_progress(websocket: WebSocket,task_id: str):
#     # task_id = "some_task_id"  # Define how you want to assign the task_id or pass it dynamically
#     await websocket__manager.connect(task_id, websocket)
#     try:
#         while True:
#             data = await websocket.receive_text()
#             # Here you can send updates for the progress of a specific task_id
#             await websocket__manager.send_personal_message(f"Message received for task {task_id}: {data}", websocket)
#             # await asyncio.sleep(1)  # Simulate progress update every second
#     except WebSocketDisconnect:
#         await websocket__manager.disconnect(task_id, websocket)
        
 
@app.websocket("/vman/api/v1/ws/ccva_progress/{task_id}")
async def websocket_va_1(websocket: WebSocket, task_id: str):
    await websocket__manager.connect(task_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Send a personalized message based on task_id
            await websocket__manager.send_personal_message(f"Message received for task {task_id}: {data}", websocket)
    except WebSocketDisconnect:
        await websocket__manager.disconnect(task_id, websocket)

@app.websocket("/vman/api/v1/ws/odk_progress/{task_id}")
async def odk_progress(websocket: WebSocket, task_id: str):
    await websocket__manager.connect(task_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Send updates for the progress of the specific task_id
            await websocket__manager.send_personal_message(f"Message received for task {task_id}: {data}", websocket)
    except WebSocketDisconnect:
        await websocket__manager.disconnect(task_id, websocket)


@app.websocket("/vman/api/v1/ws/discordants/chat/{va_id}")
async def discordants_chat(
    websocket: WebSocket,
    va_id: str,
    current_user: User = Depends(get_current_user_ws),
    db: StandardDatabase = Depends(get_arangodb_session)
    ):

    await websocket__manager.connect(va_id, websocket)
    
    try:      
        while True:
            try:
                message = await websocket__manager.safe_receive_text(websocket)
                if message:
                    message_text = json.loads(message)
                    text_message = message_text.get("text", "")
                    saved_message = await save_discordant_message_service(va_id = va_id, user_id = current_user.get('uuid', ""), message = text_message, db = db)
                    await websocket__manager.broadcast(task_id=va_id, message=saved_message)
            except WebSocketDisconnect:
                await websocket__manager.disconnect(va_id, websocket)
                break
            except Exception as e:
                logger.error(f"Error in WebSocket communication: {e}")
                await websocket__manager.send_personal_message(message=json.dumps({"error": f"{e}"}), websocket=websocket)
                continue
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await websocket__manager.disconnect(va_id, websocket)