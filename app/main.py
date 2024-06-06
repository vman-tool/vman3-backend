from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.routes import route

logger.add("./../app.log", rotation="500 MB")



@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")
    print("Visit http://localhost:8080/api/docs for the API documentation (Swagger UI)")
    print("Visit http://localhost:8080/api for the main API")
    yield
    logger.info("Application shutdown")


def create_application():
    application = FastAPI(
    title="vman3",
    version="1.0.0",
    docs_url="/api/docs",
    
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)
    application.include_router(route.odk_router)
  
    return application

app = create_application()


@app.get("/")
async def root():
    return {"message": "Hello World"}

