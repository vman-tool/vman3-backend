from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from app.utilits.odk_client import ODKClient

# Configure loguru logger
logger.add("./../app.log", rotation="500 MB")



@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")
    print("Visit http://localhost:8080/api/docs for the API documentation (Swagger UI)")
    print("Visit http://localhost:8080/api for the main API")
    yield
    logger.info("Application shutdown")

app = FastAPI(
    title="My FastAPI Application",
    version="1.0.0",
    docs_url="/api/docs",
    
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# Setup MongoDB client
client = AsyncIOMotorClient("mongodb://mongo:27017/")
db = client["odk_data"]

@app.post("/api/fetch-and-store")
async def fetch_and_store_data(start_date: str = None, end_date: str = None):
    if start_date is None:
        start_date = '2021-01-01'  # Replace with your desired default start date
    if end_date is None:
        end_date = '2024-06-06' 
            
    logger.info(f"Fetching data from ODK between {start_date} and {end_date}")
    odk_client = ODKClient()
    data = odk_client.get_form_submissions(start_date, end_date)
    print(data)
    if isinstance(data, str):
        logger.error(f"Error fetching data: {data}")
        raise HTTPException(status_code=500, detail=data)
    collection = db["form_submissions"]
    await collection.insert_many(data['value'])
    logger.info(f"Data inserted successfully: {len(data['value'])} records")
    return {"status": "Data inserted successfully"}

@app.get("/")
async def root():
    return {"message": "Hello World"}

