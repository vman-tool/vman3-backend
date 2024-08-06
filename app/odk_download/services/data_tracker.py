
import time
from datetime import datetime

from app.shared.configs.database import (process_tracker_collection,
                                         tracker_collection)


# Function to get the last successfully processed timestamp
async def get_last_processed_timestamp():
    tracker = await tracker_collection.find_one({'_id': 'last_processed'})
    return tracker['timestamp'] if tracker else None

# Function to update the last processed timestamp
async def update_last_processed_timestamp(timestamp):
    await tracker_collection.update_one(
        {'_id': 'last_processed'},
        {'$set': {'timestamp': timestamp}},
        upsert=True
    )

# Function to log errors
async def log_error(error):
    await tracker_collection.insert_one({'error': error, 'timestamp': time.time()})

# Function to log the start of a chunk download
async def log_chunk_start(chunk_id, total_data_count, start_date, end_date, skip, top, status='started'):
    await process_tracker_collection.update_one({  '_id': chunk_id,}, {'$set': {
      'total_data_count': total_data_count,
        'start_date': start_date,
        'end_date': end_date,
        'skip': skip,
        'top': top,
        'status': status,
        'timestamp': datetime.utcnow()
    }}  , upsert=True )

async def log_chunk_update( chunk_id, status='finished', error=None):
   
    await process_tracker_collection.update_one({  '_id': chunk_id,}, {'$set': {
      
        'finished_at': datetime.utcnow(),
        'status': status,
        'error': error
    }}  , upsert=True )

async def log_chunk_remove( chunk_id):
   
    await process_tracker_collection.delete_one({  '_id': chunk_id,})

# Function to update the status of a chunk download
async def update_chunk_status(start_date, end_date, skip, top, status, error=None):
    chunk_id = f"{start_date or 'null'}-{end_date or 'null'}-{skip}-{top}"
    update = {
        'status': status,
        'timestamp': datetime.utcnow()
    }
    if error:
        update['error'] = error
    await process_tracker_collection.update_one(
        {'_id': chunk_id},
        {'$set': update},
        upsert=True
    )

# Function to get all incomplete chunks
async def get_incomplete_chunks():
    return await process_tracker_collection.find({'status': {'$in': ['started', 'error']}}).to_list(length=None)