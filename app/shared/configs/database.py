
from decouple import config
from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient( config("MONGODB_URL", default="mongodb://localhost:27017/")  )
db = client[config("DB_NAME", default="vman3") ]
form_submissions_collection = db["form_submissions"]
tracker_collection = db["download_tracker"]
process_tracker_collection= db["download_process_tracker"]


async def connect_to_mongo():
    pass
    # global client, db, form_submissions_collection, tracker_collection


async def close_mongo_connection():
    global client
    client.close()
