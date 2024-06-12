import concurrent.futures
import json
import time
from datetime import datetime
from typing import List

from fastapi import HTTPException
from loguru import logger
from pandas import json_normalize
from pymongo import ReplaceOne

from app.configs.database import form_submissions_collection
from app.services.data_tracker import (get_last_processed_timestamp, log_error,
                                       update_last_processed_timestamp)
from app.utilits.odk_client import ODKClient


async def fetch_odk_data(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
):  
    if start_date or end_date:
        logger.info(f"\nFetching data from ODK between {start_date} and {end_date} \n\n")
    else:
        logger.info("\nFetching data from ODK Central \n\n")
    start_time = time.time()

    
    with ODKClient() as odk_client:
        data_for_count = odk_client.getFormSubmissions(top=1)



        total_data_count = 1000 or data_for_count["@odata.count"]
        print(f"${total_data_count} total to be downloded")

        num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
        print(f"${num_iterations}  number of loop to running")
        records_saved = 0
        last_progress = 0
     
        for i in range(num_iterations):

            data = odk_client.getFormSubmissions(start_date, end_date, top=top, skip=skip)
            print('\n odk data return')


            json_flattened = json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))

            if isinstance(data, str):
                logger.error(f"Error fetching data: {data}")
                raise HTTPException(status_code=500, detail=data)

            await form_submissions_collection.insert_many(json_flattened)

            skip += top

            progress = ((i + 1) / num_iterations) * 100
            records_saved += len(json_flattened)

            if int(progress) != last_progress:
                last_progress = int(progress)
                print(f"\rDownloading: [{'*' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}%", end='')
        end_time = time.time()
    total_elapsed_time = end_time - start_time
    print(total_elapsed_time)

    if records_saved == total_data_count:
        logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
    else:
        logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    
    
    
    
    
# async def store_data():
#     pass
    
    
    
    

# Assuming ODKClient and form_submissions_collection are defined elsewhere


async def fetch_odk_datas(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
):   
    if start_date or end_date:
        logger.info(f"\nFetching data from ODK between {start_date} and {end_date} \n\n")
    else:
        logger.info("\nFetching data from ODK Central \n\n")
    start_time = time.time()

    with ODKClient() as odk_client:
        data_for_count = odk_client.getFormSubmissions(top=1)

        total_data_count = data_for_count["@odata.count"]
        print.info(f"{total_data_count} total to be downloaded")

        num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
        print(f"{num_iterations} number of loops to run")
        records_saved = 0
        last_progress = 0

        def fetch_data_chunk(skip):
            data = odk_client.getFormSubmissions(start_date, end_date, top=top, skip=skip)
            if isinstance(data, str):
                logger.error(f"Error fetching data: {data}")
                raise HTTPException(status_code=500, detail=data)
            return json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_data_chunk, skip + i * top) for i in range(num_iterations)]
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    json_flattened = future.result()
                    await form_submissions_collection.insert_many(json_flattened)
                    records_saved += len(json_flattened)
                    
                    progress = ((i + 1) / num_iterations) * 100
                    if int(progress) != last_progress:
                        last_progress = int(progress)
                        logger.info(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}%", end='')
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    raise

    end_time = time.time()
    total_elapsed_time = end_time - start_time
    print(total_elapsed_time)

    if records_saved == total_data_count:
        clean_collection()
        logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
    else:
        clean_collection()
        logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    
    
async def clean_collection():
    await form_submissions_collection.drop()
    
    
    
async def fetch_odk_data_with_batch(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 100,
    batch_size: int = 50
):
           
    if start_date or end_date:
        logger.info(f"\nFetching data from ODK between {start_date} and {end_date} \n\n")
    else:
        logger.info("\nFetching data from ODK Central \n\n")
    
    start_time = time.time()

    with ODKClient() as odk_client:
        data_for_count = odk_client.getFormSubmissions(top=1)
        total_data_count = data_for_count["@odata.count"]
        logger.info(f"{total_data_count} total to be downloaded")

        num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
        logger.info(f"{num_iterations} number of loops to run")
        records_saved = 0
        last_progress = 0

        def fetch_data_chunk(skip, top):
            data = odk_client.getFormSubmissions(start_date, end_date, top=top, skip=skip)
            if isinstance(data, str):
                logger.error(f"Error fetching data: {data}")
                raise HTTPException(status_code=500, detail=data)
            return json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))

        async def insert_batches(batches: List[List[dict]]):
            nonlocal records_saved
            for batch in batches:
                await form_submissions_collection.insert_many(batch)
                records_saved += len(batch)

                progress = (records_saved / total_data_count) * 100
                if int(progress) != last_progress:
                    last_progress = int(progress)
                    elapsed_time = time.time() - start_time
                    logger.info(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s", end='')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_data_chunk, skip + i * top, top) for i in range(num_iterations)]
            
            batches = []
            batch = []
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    json_flattened = future.result()
                    batch.extend(json_flattened)

                    if len(batch) >= batch_size:
                        batches.append(batch[:batch_size])
                        batch = batch[batch_size:]

                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    raise

            if batch:
                batches.append(batch)

        await insert_batches(batches)

    end_time = time.time()
    total_elapsed_time = end_time - start_time
    logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

    if records_saved == total_data_count:
        await clean_collection()
        logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
    else:
        await clean_collection()
        logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    
    
    
    
    
async def fetch_odk_data_with_memory(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 1000
):   
    if start_date or end_date:
        logger.info(f"\nFetching data from ODK between {start_date} and {end_date} \n\n")
    else:
        logger.info("\nFetching data from ODK Central \n\n")
    
    start_time = time.time()
    last_processed_timestamp = await get_last_processed_timestamp()
        # Convert timestamp to date
    if last_processed_timestamp:
        last_processed_date = datetime.fromtimestamp(last_processed_timestamp).isoformat()
    else:
        last_processed_date = None
    print(type(last_processed_date))
    

    with ODKClient() as odk_client:
        data_for_count = odk_client.getFormSubmissions(top=1, start_date=last_processed_date)
        total_data_count =  data_for_count["@odata.count"]
        logger.info(f"{total_data_count} total to be downloaded")

        num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
        logger.info(f"{num_iterations} number of loops to run")
        records_saved = 0
        last_progress = 0

        def fetch_data_chunk(skip, top):
            data = odk_client.getFormSubmissions(start_date=last_processed_date, top=top, skip=skip)
            if isinstance(data, str):
                logger.error(f"Error fetching data: {data}")
                raise HTTPException(status_code=500, detail=data)
            return json.loads(json_normalize(data=data['value'], sep='/').to_json(orient='records'))

        async def insert_all_data(data: List[dict]):
            nonlocal records_saved, last_progress
            operations = []
            for item in data:
                if '__id' in item:
                    operations.append(ReplaceOne({'_id': item['__id']}, item, upsert=True))
                else:
                    operations.append(ReplaceOne(item, item, upsert=True))
            
            result = await form_submissions_collection.bulk_write(operations)
            records_saved += result.upserted_count + result.modified_count

            progress = (records_saved / total_data_count) * 100
            if int(progress) != last_progress:
                last_progress = int(progress)
                elapsed_time = time.time() - start_time
                logger.info(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s", end='')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_data_chunk, skip + i * top, top) for i in range(num_iterations)]
            
            all_data = []
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    json_flattened = future.result()
                    all_data.extend(json_flattened)
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    await log_error(str(e))
                    raise

        await insert_all_data(all_data)

    end_time = time.time()
    total_elapsed_time = end_time - start_time
    logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

    if records_saved == total_data_count:
        await update_last_processed_timestamp(time.time())
        logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
    else:
        await update_last_processed_timestamp(time.time())
        logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
        return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}