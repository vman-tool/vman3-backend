from datetime import datetime
import json
import time
from json import loads
from typing import Dict, List

import pandas as pd
from arango.database import StandardDatabase
from fastapi import HTTPException
from loguru import logger

from app.odk.models.questions_models import VA_Question
from app.odk.utils.data_transform import (assign_questions_options,
                                          filter_non_questions,
                                          odk_questions_formatter)
from app.odk.utils.odk_client import ODKClientAsync
from app.pcva.responses.va_response_classes import VAQuestionResponseClass
from app.settings.services.odk_configs import fetch_odk_config, add_configs_settings
from app.settings.models.settings import SettingsConfigData, SyncStatus
from app.shared.configs.arangodb import (ArangoDBClient, get_arangodb_client,
                                         remove_null_values, sanitize_document)
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def update_sync_status_internal(db: StandardDatabase, last_sync_data_count: int, total_synced_data: int):
    """Internal function to update sync status in the database"""
    try:
        print("Updating sync status internally")
        # Ensure the system_configs collection exists
        if not db.has_collection(db_collections.SYSTEM_CONFIGS):
            db.create_collection(db_collections.SYSTEM_CONFIGS)
            logger.info(f"Created {db_collections.SYSTEM_CONFIGS} collection")
        
        current_date = datetime.now().isoformat()
        
        sync_status = SyncStatus(
            last_sync_date=current_date,
            last_sync_data_count=last_sync_data_count,
            total_synced_data=total_synced_data
        )
        
        config_data = SettingsConfigData(
            type="sync_status",
            sync_status=sync_status
        )
        
        result = await add_configs_settings(config_data, db)
        logger.info(f"Sync status updated automatically: {current_date}, records: {last_sync_data_count}, total: {total_synced_data}")
        return result
        
    except Exception as e:
        logger.error(f"Error updating sync status automatically: {e}")
        return None


async def fetch_odk_data_initial(
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    force_update: bool = False, 
    resend: bool = False, 
    db: StandardDatabase = None,
):
    
    try:
  
        log_message = "\nFetching data from ODK Central"
        if start_date or end_date:
            log_message += f" between {start_date} and {end_date}"
        logger.info(f"{log_message}\n")



        available_data_count: int = 0
        records_margins: Dict = None
        
        config = await fetch_odk_config(db)
        
        if not start_date and not end_date and not force_update:
            records_margins = await get_margin_dates_and_records_count(db)
        
        if records_margins:
            end_date = records_margins.get('earliest_date', None)
            start_date = records_margins.get('latest_date', None)
            available_data_count = records_margins.get('total_records', 0)
            
        async with ODKClientAsync(config.odk_api_configs) as odk_client:
            try:
                data_for_count = await odk_client.getFormSubmissions(top= 1 if resend is False else top,skip= None if resend is False else skip, order_by='__system/submissionDate', order_direction='asc')
                total_data_count = data_for_count["@odata.count"]
                server_latest_submisson_date = data_for_count['value'][0]['__system']['submissionDate']
            except Exception as e:
                print(e)
                raise e

            if total_data_count == available_data_count and start_date == server_latest_submisson_date:
                logger.info("\nVman is up to date.")
                return {
                "download_status": False,
                "status": "Vman is up to date",
                
                "total_data_count": total_data_count,
                "start_date": start_date,
                "end_date": end_date,  
            }
            
            if available_data_count > 0 :
                if available_data_count < total_data_count and start_date == server_latest_submisson_date:
                    start_date = None
                elif available_data_count < total_data_count and start_date != server_latest_submisson_date:
                    end_date = None  
            
            if total_data_count > available_data_count:
                total_data_count = total_data_count - available_data_count

            logger.info(f"{total_data_count} total to be downloaded")
            
            
            return {
                "download_status": True,
                "status": "Data to be downloaded",
                "total_data_count": total_data_count,
                "start_date": start_date,
                "end_date": end_date,  
                
            }
            
    except Exception as e:
        logger.error(f"Error fetching ODK data: {e}")
        # print(e)
        raise HTTPException(status_code=500, detail=str(e))
        



            
            



async def fetch_odk_data_with_async(
    total_data_count: int,
    start_date: str = None, 
    end_date: str = None,
    skip: int = 0,
    top: int = 3000,
    db: StandardDatabase = None,
    start_time: float = 0
):   
    """
    Fetch ODK data asynchronously with WebSocket progress updates.
    This function handles the actual data downloading and processing.
    """
    try:
        from app.main import websocket__manager

        start_time = time.time()
        config = await fetch_odk_config(db)
        
        async with ODKClientAsync(config.odk_api_configs) as odk_client:
            logger.info(f"{total_data_count} total records to be downloaded")
            if websocket__manager:
                        # await websocket__manager.broadcast(f"Progress: {progress:.0f}%")
                        progress_data = {
                            "total_records": total_data_count,
                            "progress": 0,
                            "elapsed_time": 0
                        }
                        await websocket__manager.broadcast("123",json.dumps(progress_data))
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
                    
                    df.columns = df.columns.str.lower()
                    df = df.dropna(axis=1, how='all')

                    # TODO: Investigate on duplicate columns temporary solution is to rename duplicate columns
                    df = df.loc[:, ~df.columns.duplicated()] 
                    
                    return loads(df.to_json(orient='records'))
                except Exception as e:
                    raise e
            async def insert_all_data(data: List[dict]):
                nonlocal records_saved, last_progress, start_time
                try:
                    elapsed_time = 0  # Initialize elapsed_time to a default value
                    for item in data:
                        await insert_data_to_arangodb(item)
                        records_saved += 1

                        # Cap progress at 100% to prevent overflow
                        progress = min((records_saved / total_data_count) * 100, 100.0)
                        if int(progress) != last_progress:
                            last_progress = int(progress)
                            elapsed_time = time.time() - start_time  # Update elapsed_time based on current time
                            
                            logger.info(f"Progress: {progress:.1f}% ({records_saved}/{total_data_count} records)")
                            
                            if websocket__manager:
                                # await websocket__manager.broadcast(f"Progress: {progress:.0f}%")
                                progress_data = {
                                    "total_records": total_data_count,
                                    "progress": progress,
                                    "elapsed_time": elapsed_time,
                                    "records_processed": records_saved
                                }
                                await websocket__manager.broadcast("123",json.dumps(progress_data))
                            print(f"\rDownloading: [{'=' * int(progress // 2)}{' ' * (50 - int(progress // 2))}] {progress:.0f}% - Elapsed time: {elapsed_time:.2f}s", end='', flush=True)
                except Exception as e:
                    raise e

            # Process data in chunks
            for i in range(num_iterations):
                chunk_skip = skip + i * top
                chunk_top = top

                try:
                    data_chunk = await fetch_data_chunk(chunk_skip, chunk_top, start_date, end_date)
                    if data_chunk:
                        await insert_all_data(data_chunk)
                except Exception as e:
                    raise e

            end_time = time.time()
            total_elapsed_time = end_time - start_time
            logger.info(f"Total elapsed time: {total_elapsed_time:.2f}s")

            # Get current total data count from database after sync
            current_records_info = await get_margin_dates_and_records_count(db)
            current_total_data = current_records_info.get('total_records', 0) if current_records_info else 0
            print(f"Current total data: {current_total_data}")
            # Update sync status automatically after sync completion
            await update_sync_status_internal(db, records_saved, current_total_data)

            if records_saved == total_data_count:
                logger.info(f"\nData inserted successfully: {records_saved} records - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted successfully", "elapsed_time": total_elapsed_time}
            else:
                logger.info(f"\nSuccessfully inserted {records_saved} records while records were {total_data_count} - Total elapsed time: {total_elapsed_time:.2f}s")
                return {"status": "Data inserted with issues", "elapsed_time": total_elapsed_time}
    except Exception as e:
        if websocket__manager:
            await websocket__manager.broadcast("123",f"Error: {str(e)}")
            
        raise e


async def fetch_form_questions(db: StandardDatabase):
    try:
        
        config = await fetch_odk_config(db)
        async with ODKClientAsync(config.odk_api_configs) as odk_client:
            questions = await odk_client.getFormQuestions()
            fields = await odk_client.getFormFields()

            formated_questions = odk_questions_formatter(questions)
            all_questions_fields = filter_non_questions(fields)
            # questions_with_options = { 
            #     question['name']: question 
            #     for question in 
            #     [
            #         assign_questions_options(field, formated_questions)
            #         for field in all_questions_fields
            #     ]
            # }
            count = 0
            for question in [
                    assign_questions_options(field, formated_questions)
                    for field in all_questions_fields
                ]:
                await VA_Question(**question).save(db, "name")
                count += 1

            questions = await VA_Question.get_many(paging=False, db=db)

            questions = [VAQuestionResponseClass(**question).model_dump() for question in questions]
            questions = { question['name']: question for question in questions} if len(questions) else []
            
            # Questions fetching doesn't update sync status - only actual data sync operations do
            
            return ResponseMainModel(data=questions, message="Questions fetched successfully", total=count)
    except Exception as e:
        print(e)
        raise e

async def insert_data_to_arangodb(data: dict,data_source:str=None):

    try:
        data=remove_null_values(data)
        # Sanitize the document
        data = sanitize_document(data)
        
        

        db:ArangoDBClient = await get_arangodb_client()
        await db.replace_one(collection_name=db_collections.VA_TABLE, document=data)
    except Exception as e:
        print(e)
        raise e
    
    
async def insert_many_data_to_arangodb(data: List[dict], overwrite_mode: str = 'ignore'):

    try:
        data=remove_null_values(data)

        # Sanitize the document
        data = [sanitize_document(item) for item in data]
  


        db:ArangoDBClient = await get_arangodb_client()
        await db.insert_many(collection_name=db_collections.VA_TABLE, documents=data,overwrite_mode = overwrite_mode)

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

    return db.aql.execute(query=query,cache=True).next()


















