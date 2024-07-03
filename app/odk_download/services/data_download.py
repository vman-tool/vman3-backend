import time
from datetime import datetime
from json import loads
from typing import List

import pandas as pd
from fastapi import Depends, HTTPException
from loguru import logger

from app.odk_download.services.data_tracker import (
    get_last_processed_timestamp, log_chunk_remove, log_chunk_start,
    log_chunk_update, log_error, update_last_processed_timestamp)
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_client
from app.shared.configs.database import form_submissions_collection
from app.utilits.odk_client import ODKClientAsync


async def fetch_odk_data_with_async(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 1000,
    resend: bool = False # resend flag to resend data
):   
    try:
        log_message = "\nFetching data from ODK Central"
        if start_date or end_date:
            log_message += f" between {start_date} and {end_date}"
        logger.info(f"{log_message}\n")

        start_time = time.time()
        last_processed_timestamp =  None if resend is True else  await get_last_processed_timestamp()

        # if start_date is provided, set last_processed_date to start_date
        last_processed_date = start_date if start_date else last_processed_timestamp.date().isoformat() if last_processed_timestamp else None
        # if last_processed_timestamp is not none, then set end_date to None to fetch data up to the latest date
        end_date = end_date if last_processed_timestamp is None else None
        
        async with ODKClientAsync() as odk_client:
            try:
                
                data_for_count = await odk_client.getFormSubmissions(top= 1 if resend is False else top,skip= None if resend is False else skip, start_date=last_processed_date, end_date=end_date)
                total_data_count = data_for_count["@odata.count"]
                logger.info(f"{total_data_count} total to be downloaded")
            except Exception as e:
                await log_error(str(e))
                raise HTTPException(status_code=500, detail=str(e))

            num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
            logger.info(f"{num_iterations} number of loops to run")
            records_saved = 0
            last_progress = 0

            async def fetch_data_chunk(skip, top, chunk_id):
                try:
                    data = await odk_client.getFormSubmissions(start_date=last_processed_date, end_date=end_date, top=top, skip=skip)
                    if isinstance(data, str):
                        logger.error(f"Error fetching data: {data}")
                        await log_error(data)
                        await log_chunk_update(chunk_id, status='error', error=data)
                        raise HTTPException(status_code=500, detail=data)
                    # Normalize and rename columns
                    df = pd.json_normalize(data['value'], sep='/')
                    df.columns = [col.split('/')[-1] for col in df.columns]
                    return loads(df.to_json(orient='records'))
                except Exception as e:
                    await log_error(str(e))
                    await log_chunk_update(chunk_id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            async def insert_all_data(data: List[dict], chunk_id):
                nonlocal records_saved, last_progress
                try:
                    for item in data:
                        if '__id' in item:
                            await form_submissions_collection.replace_one({'__id': item['__id']}, item, upsert=True)
                            
                        else:
                            await form_submissions_collection.replace_one(item, item, upsert=True)
                        # trial for arangodb
                        await insert_to_arangodb(item)
                        records_saved += 1

                        progress = (records_saved / total_data_count) * 100
                        if int(progress) != last_progress:
                            last_progress = int(progress)
                            elapsed_time = time.time() - start_time
                            logger.info(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s")
                    await log_chunk_update(chunk_id, status='finished')
                    await log_chunk_remove(chunk_id)
                except Exception as e:
                    await log_error(str(e))
                    await log_chunk_update(chunk_id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            for i in range(num_iterations):
                chunk_skip = skip + i * top
                chunk_top = top
                chunk__id = f"{start_date or 'null'}-{end_date or 'null'}-{chunk_skip}-{chunk_top}"

                await log_chunk_start(chunk__id, total_data_count, start_date, end_date, chunk_skip, chunk_top, status='started')

                try:
                    data_chunk = await fetch_data_chunk(chunk_skip, chunk_top, chunk__id)
                    if data_chunk:
                        await insert_all_data(data_chunk, chunk__id)
                except Exception as e:
                    await log_error(str(e))
                    await log_chunk_update(chunk__id, status='error', error=str(e))
                    raise HTTPException(status_code=500, detail=str(e))

            end_time = time.time()
            total_elapsed_time = end_time - start_time
            logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

            await update_last_processed_timestamp(datetime.utcnow())

            if records_saved == total_data_count:
                logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
            else:
                logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    except Exception as e:
        await log_error(str(e))
        raise HTTPException(status_code=500, detail=str(e))


async def save_to_arangodb(db: ArangoDBClient = Depends(get_arangodb_client)):
    
    await db.collection('form_submissions').insert_many(form_submissions_collection.find())
    pass





# testing  arangodb
async def insert_to_arangodb(data: dict):
    try:
        db:ArangoDBClient = await get_arangodb_client()
        await db.replace_one(collection_name='form_submissions'   , document=data)
    except Exception as e:
        print(e)
        raise e














