
# import time
# from datetime import datetime

# from app.shared.configs.database import (process_tracker_collection,
#                                          tracker_collection)


# # Function to get the last successfully processed timestamp
# async def get_last_processed_timestamp():
#     tracker = await tracker_collection.find_one({'_id': 'last_processed'})
#     return tracker['timestamp'] if tracker else None

# # Function to update the last processed timestamp
# async def update_last_processed_timestamp(timestamp):
#     await tracker_collection.update_one(
#         {'_id': 'last_processed'},
#         {'$set': {'timestamp': timestamp}},
#         upsert=True
#     )

# # Function to log errors
# async def log_error(error):
#     await tracker_collection.insert_one({'error': error, 'timestamp': time.time()})

# # Function to log the start of a chunk download
# async def log_chunk_start(chunk_id, total_data_count, start_date, end_date, skip, top, status='started'):
#     await process_tracker_collection.update_one({  '_id': chunk_id,}, {'$set': {
#       'total_data_count': total_data_count,
#         'start_date': start_date,
#         'end_date': end_date,
#         'skip': skip,
#         'top': top,
#         'status': status,
#         'timestamp': datetime.utcnow()
#     }}  , upsert=True )

# async def log_chunk_update( chunk_id, status='finished', error=None):
   
#     await process_tracker_collection.update_one({  '_id': chunk_id,}, {'$set': {
      
#         'finished_at': datetime.utcnow(),
#         'status': status,
#         'error': error
#     }}  , upsert=True )

# async def log_chunk_remove( chunk_id):
   
#     await process_tracker_collection.delete_one({  '_id': chunk_id,})

# # Function to update the status of a chunk download
# async def update_chunk_status(start_date, end_date, skip, top, status, error=None):
#     chunk_id = f"{start_date or 'null'}-{end_date or 'null'}-{skip}-{top}"
#     update = {
#         'status': status,
#         'timestamp': datetime.utcnow()
#     }
#     if error:
#         update['error'] = error
#     await process_tracker_collection.update_one(
#         {'_id': chunk_id},
#         {'$set': update},
#         upsert=True
#     )

# # Function to get all incomplete chunks
# async def get_incomplete_chunks():
#     return await process_tracker_collection.find({'status': {'$in': ['started', 'error']}}).to_list(length=None)






from datetime import datetime

from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections


# Function to get the last successfully processed timestamp
async def get_last_processed_timestamp(db: StandardDatabase):
    collection = db.collection(db_collections.DOWNLOAD_TRACKER)
    query = f"""
        LET doc = FIRST(FOR doc IN {collection.name} FILTER doc._key == @key RETURN doc)
        RETURN doc
    """
    bind_vars = {'key': 'last_processed'}
    
    try:
        cursor =  db.aql.execute(query, bind_vars=bind_vars)
        doc =  cursor.next()
        print(doc)
        if not doc:
            return None
        
        return doc['timestamp']
    except Exception as e:
        print(f"get_last_processed_timestamp :Error executing AQL: {str(e)}")
        raise e
        return None

# Function to update the last processed timestamp
async def update_last_processed_timestamp(db: StandardDatabase, timestamp):
    try:
        collection = db.collection(db_collections.DOWNLOAD_TRACKER)
        collection.insert({
            '_key': 'last_processed',
            'timestamp': timestamp
        }, overwrite=True)
    except Exception as e:
        print(f" update_last_processed_timestamp: Error executing AQL: {str(e)}")
    


# Function to log errors
async def log_error(db: StandardDatabase, error):
    try:
        collection = db.collection(db_collections.DOWNLOAD_TRACKER)
        collection.insert({
            '_key': f"error-{datetime.utcnow().timestamp()}",
            'error': error,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        print(f" log_error : Error executing AQL: {str(e)}")

# Function to log the start of a chunk download
async def log_chunk_start(db: StandardDatabase, chunk_id, total_data_count, start_date, end_date, skip, top, status='started'):
    try:
        collection = db.collection(db_collections.DOWNLOAD_PROCESS_TRACKER)
        collection.insert({
            '_key': chunk_id,
            'total_data_count': total_data_count,
            'start_date': start_date,
            'end_date': end_date,
            'skip': skip,
            'top': top,
            'status': status,
            'timestamp': datetime.utcnow().isoformat()
        }, overwrite=True)
    except Exception as e:
        print(f"Error logging chunk start: {str(e)}")
        raise e

# Function to log the update of a chunk download
async def log_chunk_update(db: StandardDatabase, chunk_id, status='finished', error=None):
        try:
            collection = db.collection(db_collections.DOWNLOAD_PROCESS_TRACKER)
            update_data = {
                'finished_at':datetime.utcnow().isoformat(),
                'status': status,
            }
            if error:
                update_data['error'] = error
            
            collection.update({
                '_key': chunk_id,
                **update_data
            })
        except Exception as e:
            print(f"Error logging chunk update: {str(e)}")
            raise e

# Function to remove a chunk record
async def log_chunk_remove(db: StandardDatabase, chunk_id):
    try:
        
        collection = db.collection(db_collections.DOWNLOAD_PROCESS_TRACKER)
        collection.delete({'_key': chunk_id})
    except Exception as e:
        print(f" log_chunk_remove: Error logging chunk update: {str(e)}")
        raise e

# Function to update the status of a chunk download
async def update_chunk_status(db: StandardDatabase, start_date, end_date, skip, top, status, error=None):
    try:
            
        chunk_id = f"{start_date or 'null'}-{end_date or 'null'}-{skip}-{top}"
        collection = db.collection(db_collections.DOWNLOAD_PROCESS_TRACKER)
        
        update_data = {
            'status': status,
            'timestamp': datetime.utcnow().isoformat()
        }
        if error:
            update_data['error'] = error
        
        collection.update({
            '_key': chunk_id,
            **update_data
        }, overwrite=True)
    except Exception as e:
        print(f" update_chunk_status: Error logging chunk update: {str(e)}")
        raise e

# Function to get all incomplete chunks
async def get_incomplete_chunks(db: StandardDatabase):
    try:
        
        collection = db.collection(db_collections.DOWNLOAD_PROCESS_TRACKER)
        query = '''
        FOR doc IN @@collection
            FILTER doc.status IN ['started', 'error']
            RETURN doc
        '''
        cursor = await db.aql.execute(query, bind_vars={'@collection': collection.name})
        return await cursor.to_list()
    except Exception as e:
        print(f" get_incomplete_chunks: Error logging chunk update: {str(e)}")
        raise e