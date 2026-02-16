"""
CCVA Background Tasks

Celery tasks for running CCVA analysis in the background with:
- Progress updates via Redis Pub/Sub
- Task retry on failure
- Result storage in database
"""

import json
import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional
from celery import shared_task
from celery.utils.log import get_task_logger

import redis

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
        logger.debug(f"Published progress to {channel}: {progress_data.get('progress', 0)}%")
    except Exception as e:
        logger.error(f"Failed to publish progress: {e}")


@shared_task(
    bind=True,
    name='app.tasks.ccva_tasks.run_ccva_task',
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    acks_late=True,
)
def run_ccva_task(
    self,
    records_data: List[Dict],
    task_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    malaria_status: Optional[str] = None,
    hiv_status: Optional[str] = None,
    ccva_algorithm: Optional[str] = None,
    user_id: str = "unknown"
):
    """
    Celery task to run CCVA analysis.
    
    This is a wrapper that runs the async CCVA function in a sync context
    and publishes progress updates to Redis Pub/Sub for WebSocket broadcasting.
    """
    logger.info(f"Starting CCVA task {task_id} with {len(records_data)} records")
    
    start_time = datetime.now()
    
    # Publish initial progress
    publish_progress(task_id, {
        "progress": 1,
        "total_records": len(records_data),
        "message": "Starting CCVA analysis...",
        "status": "running",
        "task_id": task_id,
        "error": False,
        "elapsed_time": "0:0:0"
    })
    
    try:
        # Import here to avoid circular imports
        from app.shared.configs.arangodb import get_arangodb_client_sync
        from app.ccva.services.ccva_services import runCCVA
        from app.settings.services.odk_configs import fetch_odk_config
        from app.shared.configs.models import ResponseMainModel
        
        import pandas as pd
        from app.shared.configs.arangodb import remove_null_values
        
        # Get database connection (sync version for Celery)
        db = get_arangodb_client_sync()
        
        # Convert to DataFrame
        publish_progress(task_id, {
            "progress": 3,
            "message": "Preparing data...",
            "status": "running",
            "task_id": task_id,
            "error": False
        })
        
        database_dataframe = pd.DataFrame.from_records(remove_null_values(records_data))
        
        # Get ODK config synchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            config_obj = loop.run_until_complete(fetch_odk_config(db, True))
        finally:
            loop.close()
        
        id_col = config_obj.field_mapping.instance_id
        date_col = config_obj.field_mapping.date
        
        # Define callback for progress updates
        def celery_update_callback(progress):
            """Callback to publish progress from runCCVA"""
            if isinstance(progress, str):
                try:
                    progress = json.loads(progress)
                except:
                    progress = {"message": progress}
            
            publish_progress(task_id, progress)
        
        # Run CCVA (this is synchronous)
        publish_progress(task_id, {
            "progress": 5,
            "message": "Running InterVA5 analysis...",
            "status": "running",
            "task_id": task_id,
            "total_records": len(database_dataframe),
            "error": False
        })
        
        result = runCCVA(
            odk_raw=database_dataframe,
            file_id=task_id,
            update_callback=celery_update_callback,
            db=db,
            id_col=id_col,
            date_col=date_col,
            start_time=start_time,
            algorithm=ccva_algorithm,
            malaria=malaria_status,
            hiv=hiv_status,
            user_id=user_id
        )
        
        elapsed = datetime.now() - start_time
        elapsed_str = f"{elapsed.seconds // 3600}:{(elapsed.seconds // 60) % 60}:{elapsed.seconds % 60}"
        
        # Publish completion
        publish_progress(task_id, {
            "progress": 100,
            "message": "CCVA analysis completed successfully",
            "status": "completed",
            "task_id": task_id,
            "elapsed_time": elapsed_str,
            "error": False,
            "data": result
        })
        
        logger.info(f"CCVA task {task_id} completed in {elapsed_str}")
        return {"status": "completed", "task_id": task_id, "elapsed_time": elapsed_str}
        
    except Exception as e:
        elapsed = datetime.now() - start_time
        elapsed_str = f"{elapsed.seconds // 3600}:{(elapsed.seconds // 60) % 60}:{elapsed.seconds % 60}"
        
        logger.error(f"CCVA task {task_id} failed: {e}")
        
        # Publish error
        publish_progress(task_id, {
            "progress": 0,
            "message": f"Error during CCVA analysis: {str(e)}",
            "status": "error",
            "task_id": task_id,
            "elapsed_time": elapsed_str,
            "error": True
        })
        
        # Re-raise for Celery retry mechanism
        raise self.retry(exc=e)
