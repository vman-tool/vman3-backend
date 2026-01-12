"""
ODK Data Sync Background Tasks

Celery tasks for ODK data synchronization with:
- Progress updates via Redis Pub/Sub
- Chunked data processing
- Task retry on failure
"""

import json
import time
from datetime import datetime
from typing import Optional
from celery import shared_task
from celery.utils.log import get_task_logger

import redis
import pandas as pd
from json import loads

from decouple import config

logger = get_task_logger(__name__)


def get_redis_client():
    """Get Redis client for Pub/Sub"""
    redis_url = config('REDIS_URL', default='redis://localhost:6370')
    redis_password = config('REDIS_PASSWORD', default=None)
    
    # Parse URL
    if '://' in redis_url:
        _, rest = redis_url.split('://', 1)
        if ':' in rest:
            host, port = rest.split(':')
            port = int(port)
        else:
            host = rest
            port = 6379
    else:
        host = 'localhost'
        port = 6379
    
    return redis.Redis(host=host, port=port, password=redis_password, decode_responses=True)


def publish_progress(task_id: str, progress_data: dict):
    """Publish progress update to Redis Pub/Sub channel"""
    try:
        r = get_redis_client()
        channel = f"ws:broadcast:{task_id}"
        r.publish(channel, json.dumps(progress_data))
        logger.debug(f"Published ODK progress to {channel}")
    except Exception as e:
        logger.error(f"Failed to publish ODK progress: {e}")


@shared_task(
    bind=True,
    name='app.tasks.odk_tasks.sync_odk_data_task',
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    acks_late=True,
)
def sync_odk_data_task(
    self,
    total_data_count: int,
    task_id: str = "123",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip: int = 0,
    top: int = 3000,
):
    """
    Celery task to sync ODK data.
    
    Fetches data from ODK Central and stores in ArangoDB with progress updates.
    """
    logger.info(f"Starting ODK sync task {task_id}: {total_data_count} records")
    
    start_time = time.time()
    
    # Publish initial progress
    publish_progress(task_id, {
        "total_records": total_data_count,
        "progress": 0,
        "elapsed_time": 0,
        "records_processed": 0,
        "status": "running",
        "message": "Starting ODK data sync..."
    })
    
    try:
        import asyncio
        from app.shared.configs.arangodb import get_arangodb_client_sync
        from app.settings.services.odk_configs import fetch_odk_config
        from app.odk.utils.odk_client import ODKClientAsync
        from app.odk.services.data_download import insert_many_data_to_arangodb, update_sync_status_internal, get_margin_dates_and_records_count
        
        # Get database connection
        db = get_arangodb_client_sync()
        
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Get ODK config
            config_obj = loop.run_until_complete(fetch_odk_config(db))
            
            records_saved = 0
            last_progress = 0
            num_iterations = (total_data_count // top) + (1 if total_data_count % top != 0 else 0)
            
            async def fetch_and_process():
                nonlocal records_saved, last_progress
                
                async with ODKClientAsync(config_obj.odk_api_configs) as odk_client:
                    for i in range(num_iterations):
                        chunk_skip = skip + i * top
                        chunk_top = top
                        
                        # Fetch chunk
                        data = await odk_client.getFormSubmissions(
                            top=chunk_top,
                            skip=chunk_skip,
                            start_date=start_date,
                            end_date=end_date,
                            order_by='__system/submissionDate',
                            order_direction='asc'
                        )
                        
                        if isinstance(data, str):
                            raise Exception(f"Error fetching data: {data}")
                        
                        # Process data
                        df = pd.json_normalize(data['value'], sep='/')
                        df.columns = [col.split('/')[-1] for col in df.columns]
                        df.columns = df.columns.str.lower()
                        df = df.dropna(axis=1, how='all')
                        df = df.loc[:, ~df.columns.duplicated()]
                        
                        records = loads(df.to_json(orient='records'))
                        
                        # Batch insert all records from this chunk
                        if records:
                            await insert_many_data_to_arangodb(records, overwrite_mode='replace')
                            records_saved += len(records)
                            
                            # Cap progress at 100%
                            progress = min((records_saved / total_data_count) * 100, 100.0)
                            elapsed_time = time.time() - start_time
                            
                            publish_progress(task_id, {
                                "total_records": total_data_count,
                                "progress": progress,
                                "elapsed_time": elapsed_time,
                                "records_processed": records_saved,
                                "status": "running",
                                "message": f"Processing records... ({records_saved}/{total_data_count})"
                            })
                
                # Update sync status
                current_records = await get_margin_dates_and_records_count(db)
                current_total = current_records.get('total_records', 0) if current_records else 0
                await update_sync_status_internal(db, records_saved, current_total)
                
                return records_saved
            
            # Run the async fetch
            records_saved = loop.run_until_complete(fetch_and_process())
            
        finally:
            loop.close()
        
        elapsed_time = time.time() - start_time
        
        # Publish completion
        publish_progress(task_id, {
            "total_records": total_data_count,
            "progress": 100,
            "elapsed_time": elapsed_time,
            "records_processed": records_saved,
            "status": "completed",
            "message": f"ODK sync completed: {records_saved} records in {elapsed_time:.2f}s"
        })
        
        logger.info(f"ODK sync task {task_id} completed: {records_saved} records in {elapsed_time:.2f}s")
        
        return {
            "status": "completed",
            "task_id": task_id,
            "records_saved": records_saved,
            "elapsed_time": elapsed_time
        }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        
        logger.error(f"ODK sync task {task_id} failed: {e}")
        
        # Publish error
        publish_progress(task_id, {
            "total_records": total_data_count,
            "progress": 0,
            "elapsed_time": elapsed_time,
            "status": "error",
            "message": f"ODK sync failed: {str(e)}",
            "error": True
        })
        
        # Re-raise for Celery retry
        raise self.retry(exc=e)
