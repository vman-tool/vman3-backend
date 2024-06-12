from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["vmam3"]

form_submissions_collection=db["form_submissions"]
tracker_collection = db['download_tracker']