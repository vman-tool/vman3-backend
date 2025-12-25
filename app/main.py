from contextlib import asynccontextmanager
import json
import asyncio

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


logger.add("./logs/app.log", rotation="500 MB")
scheduler = AsyncIOScheduler()

# Track initialization status
app_status = {
    "server_ready": False,
    "db_logger_ready": False,
    "scheduler_ready": False,
    "default_account_ready": False,
    "initialization_complete": False
}

async def initialize_db_logger():
    """Initialize database logger in background"""
    try:
        await get_db_logger()
        app_status["db_logger_ready"] = True
        logger.info("✅ Database logger initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database logger: {e}")

async def initialize_scheduler():
    """Initialize scheduler in background"""
    try:
        await start_scheduler()
        app_status["scheduler_ready"] = True
        logger.info("✅ Scheduler initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize scheduler: {e}")

async def initialize_default_account():
    """Initialize default account in background"""
    try:
        await default_account_creation()
        app_status["default_account_ready"] = True
        logger.info("✅ Default account initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize default account: {e}")

async def run_background_initialization():
    """Run all initialization tasks concurrently"""
    # Small delay to ensure server is fully ready
    await asyncio.sleep(0.5)
    
    logger.info("🚀 Starting background initialization tasks...")
    
    # Run all initialization tasks concurrently
    await asyncio.gather(
        initialize_db_logger(),
        initialize_scheduler(),
        initialize_default_account(),
        return_exceptions=True  # Don't fail if one task fails
    )
    
    app_status["initialization_complete"] = True
    logger.info("✅ Background initialization completed")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup logic
    logger.info("🚀 Application startup")
    logger.info("📚 Visit http://localhost:8080/vman/api/v1/docs for the API documentation (Swagger UI)")
    logger.info("🌐 Visit http://localhost:8080/vman/api/v1 for the main API")
    
    # Mark server as ready immediately
    app_status["server_ready"] = True
    
    # Start background initialization tasks without blocking
    asyncio.create_task(run_background_initialization())
    
    try:
        yield
    finally:
        # Application shutdown logic
        logger.info("🛑 Application shutdown")
        await shutdown_scheduler()
        
        # Determine if we should also close the loop (usually not in lifespan)
        # scheduler.shutdown() # Removed because shutdown_scheduler() handles it.
        
        logger.info("✅ Shutdown completed")
        

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

    
@app.get("/health")
@app.get("/vman/api/v1/health")
async def health_check():
    """Health check endpoint to monitor initialization status"""
    return {
        "status": "healthy" if app_status["server_ready"] else "starting",
        "initialization_status": app_status,
        "message": "VMan API v3 is running" if app_status["server_ready"] else "VMan API v3 is starting up"
    }

@app.get("/vman/api/v1", response_class=HTMLResponse)
@app.get("/vman", response_class=HTMLResponse)
@app.get("/", response_class=HTMLResponse)
@app.get("/vman/api", response_class=HTMLResponse)
async def get():
    """
    Welcome to the vman3 API
    """
    # Show initialization status in the welcome page
    status_emoji = "✅" if app_status["initialization_complete"] else "⏳"
    status_text = "All systems ready" if app_status["initialization_complete"] else "Initializing..."
    
    return HTMLResponse(f"""
        <!DOCTYPE html>
        <html>
            <head>
                <meta charset="utf-8">
                <meta http-equiv="X-UA-Compatible" content="IE=edge">
                <title>VMan API v3</title>
                <meta name="description" content="VMan API v3 - Verbal Autopsy Management System">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    body {{
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        margin: 0;
                        padding: 40px 20px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        min-height: 100vh;
                    }}
                    .container {{
                        max-width: 800px;
                        margin: 0 auto;
                        text-align: center;
                    }}
                    .status {{
                        background: rgba(255, 255, 255, 0.1);
                        padding: 20px;
                        border-radius: 10px;
                        margin: 20px 0;
                        backdrop-filter: blur(10px);
                    }}
                    .status-item {{
                        display: flex;
                        justify-content: space-between;
                        margin: 10px 0;
                        padding: 10px;
                        background: rgba(255, 255, 255, 0.05);
                        border-radius: 5px;
                    }}
                    .status-emoji {{
                        font-size: 1.2em;
                    }}
                    .links {{
                        margin-top: 30px;
                    }}
                    .links a {{
                        display: inline-block;
                        margin: 10px;
                        padding: 15px 30px;
                        background: rgba(255, 255, 255, 0.2);
                        color: white;
                        text-decoration: none;
                        border-radius: 25px;
                        transition: all 0.3s ease;
                    }}
                    .links a:hover {{
                        background: rgba(255, 255, 255, 0.3);
                        transform: translateY(-2px);
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🚀 Welcome to VMan API v3</h1>
                    <p>Verbal Autopsy Management System</p>
                    
                    <div class="status">
                        <h3>{status_emoji} System Status: {status_text}</h3>
                        <div class="status-item">
                            <span>Server Ready</span>
                            <span class="status-emoji">{'✅' if app_status['server_ready'] else '⏳'}</span>
                        </div>
                        <div class="status-item">
                            <span>Database Logger</span>
                            <span class="status-emoji">{'✅' if app_status['db_logger_ready'] else '⏳'}</span>
                        </div>
                        <div class="status-item">
                            <span>Scheduler</span>
                            <span class="status-emoji">{'✅' if app_status['scheduler_ready'] else '⏳'}</span>
                        </div>
                        <div class="status-item">
                            <span>Default Account</span>
                            <span class="status-emoji">{'✅' if app_status['default_account_ready'] else '⏳'}</span>
                        </div>
                    </div>
                    
                    <div class="links">
                        <a href="/vman/api/v1/docs">📚 API Documentation</a>
                        <a href="/health">🔍 Health Check</a>
                    </div>
                </div>
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