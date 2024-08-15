import time
from datetime import datetime
from typing import List

import pandas as pd
from arango.database import StandardDatabase
from fastapi import Depends, HTTPException
from loguru import logger

from app.odk_download.services.data_tracker import (
    get_last_processed_timestamp, log_chunk_remove, log_chunk_start,
    log_chunk_update, log_error, update_last_processed_timestamp)
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_client
from app.shared.configs.constants import db_collections
from app.shared.configs.database import form_submissions_collection
from app.utilits.odk_client import ODKClientAsync


async def fetch_odk_data_with_async_old(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    db: StandardDatabase = None,
    resend: bool = False  # Resend flag to resend data
):   
    try:
        from app.main import websocket__manager
        log_message = "\nFetching data from ODK Central"
        if start_date or end_date:
            log_message += f" between {start_date} and {end_date}"
        logger.info(f"{log_message}\n")

        start_time = time.time()
        last_processed_timestamp = None if resend else await get_last_processed_timestamp(db)

        # Set the last processed date or use start_date if provided
        last_processed_date = start_date if start_date else last_processed_timestamp.date().isoformat() if last_processed_timestamp else None
        end_date = end_date if last_processed_timestamp is None else None
        print(last_processed_date)
        config = await fetch_odk_config(db)
        # logger.info(f"ODK Config: {config}")
        
        async with ODKClientAsync(config) as odk_client:
            try:
                data_for_count = await odk_client.getFormSubmissions(
                    top=1 if not resend else top, 
                    skip=None if not resend else skip, 
                    start_date=last_processed_date, 
                    end_date=end_date
                )
                # data_for_count = await odk_client.getFormSubmissions(top= 1 if resend is False else top,skip= None if resend is False else skip)
               
                total_data_count = data_for_count["@odata.count"]
                logger.info(f"{total_data_count} total records to be downloaded")
            except Exception as e:
                print(e,'hapa')
                print(e)
                await log_error(db, str(e))
                raise HTTPException(status_code=500, detail=str(e))

            num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
            logger.info(f"{num_iterations} iterations required")
            records_saved = 0
            last_progress = 0

            async def fetch_data_chunk(skip, top, chunk_id):
                try:
                    data = await odk_client.getFormSubmissions(
                        start_date=last_processed_date, 
                        end_date=end_date, 
                        top=top, 
                        skip=skip
                    )
                    if isinstance(data, str):
                        logger.error(f"Error fetching data: {data}")
                        await log_error(db, data)
                        await log_chunk_update(db, chunk_id, status='error', error=data)
                        raise HTTPException(status_code=500, detail=data)
                    
                    # Normalize and rename columns
                    df = pd.json_normalize(data['value'], sep='/')
                    df.columns = [col.split('/')[-1] for col in df.columns]
                    return df.to_dict(orient='records')
                except Exception as e:
                    print(e)
                    await log_error(db, str(e))
                    await log_chunk_update(db, chunk_id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            async def insert_all_data(data: List[dict], chunk_id):
                nonlocal records_saved, last_progress
                try:
                    for item in data:
                        await insert_to_arangodb(item)
                        records_saved += 1

                        progress = (records_saved / total_data_count) * 100
                        if int(progress) != last_progress:
                            last_progress = int(progress)
                            elapsed_time = time.time() - start_time
                            if websocket__manager:
                                await websocket__manager.broadcast(f"Progress: {progress:.0f}%")
                            
                            logger.info(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s")
                    
                    await log_chunk_update(db, chunk_id, status='finished')
                    await log_chunk_remove(db, chunk_id)
                except Exception as e:
                    await log_error(db, str(e))
                    await log_chunk_update(db, chunk_id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            for i in range(num_iterations):
                chunk_skip = skip + i * top
                chunk_top = top
                chunk_id = f"{start_date or 'null'}-{end_date or 'null'}-{chunk_skip}-{chunk_top}"

                await log_chunk_start(db, chunk_id, total_data_count, start_date, end_date, chunk_skip, chunk_top, status='started')
                print(f"Fetching chunk {i + 1} of {num_iterations}")
                try:
                    data_chunk = await fetch_data_chunk(chunk_skip, chunk_top, chunk_id)
                    if data_chunk:
                        await insert_all_data(data_chunk, chunk_id)
                except Exception as e:
                    await log_error(db, str(e))
                    await log_chunk_update(db, chunk_id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            end_time = time.time()
            total_elapsed_time = end_time - start_time
            logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

            await update_last_processed_timestamp(db, datetime.utcnow())

            if records_saved == total_data_count:
                logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
            else:
                logger.info(f"\nSuccessfully inserted {records_saved} records, but expected {total_data_count} records - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    except Exception as e:
        if websocket__manager:
            await websocket__manager.broadcast(f"Error: {str(e)}")
        await log_error(db, str(e))
        raise HTTPException(status_code=500, detail=str(e))



async def save_to_arangodb(db: ArangoDBClient = Depends(get_arangodb_client)):
    
    await db.collection('form_submissions').insert_many(form_submissions_collection.find())
    pass





# testing  arangodb
async def insert_to_arangodb(data: dict):
    pass
    try:
        db:ArangoDBClient = await get_arangodb_client()
        await db.replace_one(collection_name=db_collections.VA_TABLE  , document=data)
    except Exception as e:
        print(e)
        raise e




























