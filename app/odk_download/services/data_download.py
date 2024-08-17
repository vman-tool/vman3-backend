import time
from json import loads
from typing import Dict, List

import pandas as pd
from arango.database import StandardDatabase
from fastapi import Depends, HTTPException
from loguru import logger

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.arangodb_db import ArangoDBClient, get_arangodb_client
from app.shared.configs.constants import db_collections
from app.shared.configs.database import form_submissions_collection
from app.utilits.odk_client import ODKClientAsync
from app.utilits.websocket_manager import WebSocketManager


async def fetch_odk_data_with_async(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    force_update: bool = False, # resend flag to resend data
    resend: bool = False, # resend flag to resend data
    db: StandardDatabase =None,
   
):   
    try:
        websocket_manager: WebSocketManager = WebSocketManager()
        log_message = "\nFetching data from ODK Central"
        if start_date or end_date:
            log_message += f" between {start_date} and {end_date}"
        logger.info(f"{log_message}\n")

        start_time = time.time()

        available_data_count: int = 0
        records_margins: Dict = None
        # load setting from db
        config = await fetch_odk_config(db)
        
        # Load odk records and get the latest date to query if none then query all the data
        if not start_date and not end_date and not force_update:
            records_margins = await get_margin_dates_and_records_count(db)
        
        if records_margins:
            end_date = records_margins.get('earliest_date', None)
            start_date = records_margins.get('latest_date', None)
            available_data_count = records_margins.get('total_records', 0)
            
        async with ODKClientAsync(config) as odk_client:
            try:
                data_for_count = await odk_client.getFormSubmissions(top= 1 if resend is False else top,skip= None if resend is False else skip, order_by='__system/submissionDate', order_direction='asc')
                total_data_count = data_for_count["@odata.count"]
                server_latest_submisson_date = data_for_count['value'][0]['__system']['submissionDate']
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

            if total_data_count == available_data_count and start_date == server_latest_submisson_date:
                logger.info(f"\nVman is up to date.")
                return {"status": "Vman is up to date"}
            
            if available_data_count > 0 :
                if available_data_count < total_data_count and start_date == server_latest_submisson_date:
                    start_date = None
                elif available_data_count < total_data_count and start_date != server_latest_submisson_date:
                    end_date = None  
            
            if total_data_count > available_data_count:
                total_data_count = total_data_count - available_data_count

            
            logger.info(f"{total_data_count} total to be downloaded")
            
            num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
            records_saved = 0
            last_progress = 0

            async def fetch_data_chunk(skip, top, start_date, end_date):
                try:
                    data = await odk_client.getFormSubmissions(top=top, skip=skip, start_date=start_date, end_date=end_date, order_by='__system/submissionDate', order_direction='asc')
                    if isinstance(data, str):
                        logger.error(f"Error fetching data: {data}")
                        raise HTTPException(status_code=500, detail=data)
                    df = pd.json_normalize(data['value'], sep='/')
                    df.columns = [col.split('/')[-1] for col in df.columns]
                    
                    # df[df.columns].to_csv('columns_before.csv', index=False)
                    df.columns = df.columns.str.lower()
                    # print(df.columns[df.columns.duplicated()])
                    # df[df.columns].to_csv('columns_after.csv', index=False)
                    df = df.dropna(axis=1, how='all')
                    
                    return loads(df.to_json(orient='records'))
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            async def insert_all_data(data: List[dict]):
                nonlocal records_saved, last_progress
                try:
                    for item in data:
                        # trial for arangodb
                        await insert_to_arangodb(item)
                        records_saved += 1

                        progress = (records_saved / total_data_count) * 100
                        if int(progress) != last_progress:
                            last_progress = int(progress)
                            elapsed_time = time.time() - start_time
                            # send progress to websocket
                            if websocket_manager:
                                await websocket_manager.broadcast(f"Progress: {progress:.0f}%")
                            print(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s", end='', flush=True)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            for i in range(num_iterations):
                chunk_skip = skip + i * top
                chunk_top = top
                # chunk__id = f"{start_date or 'null'}-{end_date or 'null'}-{chunk_skip}-{chunk_top}"

                try:
                    data_chunk = await fetch_data_chunk(chunk_skip, chunk_top, start_date, end_date)
                    if data_chunk:
                        await insert_all_data(data_chunk)
                    pass
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            end_time = time.time()
            total_elapsed_time = end_time - start_time
            logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

            if records_saved == total_data_count:
                logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
            else:
                logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    except Exception as e:
        if websocket_manager:
            await websocket_manager.broadcast(f"Error: {str(e)}")
            
        raise HTTPException(status_code=500, detail=str(e))


async def save_to_arangodb(db: ArangoDBClient = Depends(get_arangodb_client)):
    
    await db.collection(db_collections.VA_TABLE).insert_many(form_submissions_collection.find())
    pass





# testing  arangodb
async def insert_to_arangodb(data: dict):
    try:
        db:ArangoDBClient = await get_arangodb_client()
        await db.replace_one(collection_name=db_collections.VA_TABLE, document=data)
    except Exception as e:
        print(e)
        raise e
    
async def get_margin_dates_and_records_count(db: StandardDatabase = None):
    query = f"""
                LET totalCount = LENGTH(FOR doc IN {db_collections.VA_TABLE} RETURN 1)

                
                LET latestRecord = (
                        FOR doc IN {db_collections.VA_TABLE}
                            SORT doc.submissiondate DESC
                            LIMIT 1
                            RETURN MERGE({{latest_date: doc.submissiondate}}, {{ total_records: totalCount }})
                    )[0]
                
                LET earliestRecord = (
                        FOR doc IN {db_collections.VA_TABLE}
                            SORT doc.submissiondate ASC
                            LIMIT 1
                            RETURN MERGE({{earliest_date: doc.submissiondate}}, latestRecord)
                    )[0]

                RETURN earliestRecord
            """

    return db.aql.execute(query=query).next()













