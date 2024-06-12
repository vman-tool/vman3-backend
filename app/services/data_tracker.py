
import time

from app.configs.database import tracker_collection


# Function to get the last processed timestamp
async def get_last_processed_timestamp():
    tracker = await tracker_collection.find_one({'_id': 'last_processed'})
    return tracker['timestamp'] if tracker else None

# Function to update the last processed timestamp
async def update_last_processed_timestamp(timestamp):
    await tracker_collection.replace_one(
        {'_id': 'last_processed'},
        {'$set': {'timestamp': timestamp}},
        upsert=True
    )


async def log_error(error):
    await tracker_collection.insert_one({'error': error, 'timestamp': time.time()})