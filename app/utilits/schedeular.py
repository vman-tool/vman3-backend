import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import BackgroundTasks


from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import db_collections
from app.utilits.db_logger import log_to_db
from app.utilits.logger import app_logger
from app.ccva.services.ccva_public_services import cleanup_expired_ccva_public_results
from app.ccva_public_module.config import CCVA_PUBLIC_CLEANUP_ENABLED

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a global scheduler instance
scheduler = AsyncIOScheduler()

async def get_cron_settings(db) -> Dict[str, Any]:
    """Get cron settings from the database"""
    try:
        # Query to get the vman_config document
        aql_query = f"""
        FOR settings in {db_collections.SYSTEM_CONFIGS}
        FILTER settings._key == 'vman_config'
        RETURN settings.cron_settings
        """
        # Execute the query without using await on the cursor
        cursor = db.aql.execute(aql_query, bind_vars={}, cache=True)
        
        # Collect results without using await
        cron_settings = [doc for doc in cursor]
        
        # If no cron settings found, return default settings
        if not cron_settings or not cron_settings[0]:
            return {"days": [], "time": "00:00"}
        
        return cron_settings[0]
    except Exception as e:
        logger.error(f"Error fetching cron settings: {str(e)}")
        return {"days": [], "time": "00:00"}

def day_of_week_to_cron(days: List[str]) -> str:
    """Convert day names to cron day numbers (0-6, where 0 is Monday in APScheduler)"""
    day_mapping = {
        'monday': 0,
        'tuesday': 1,
        'wednesday': 2,
        'thursday': 3,
        'friday': 4,
        'saturday': 5,
        'sunday': 6
    }
    
    if not days:
        return '*'  # If no days specified, run every day
    
    cron_days = [str(day_mapping[day.lower()]) for day in days if day.lower() in day_mapping]
    return ','.join(cron_days) if cron_days else '*'

# Create a wrapper function that creates a BackgroundTasks object
@log_to_db(context="odk_fetch_job_wrapper")
async def odk_fetch_job_wrapper(db=None):
    """Wrapper function to create BackgroundTasks and call the fetch function"""
    from fastapi import BackgroundTasks
    # from app.ccva.services.odk_fetch import fetch_odk_data_with_async_endpoint
    from app.odk.odk_routes import fetch_odk_data_with_async_endpoint
    try:
        # Create a new BackgroundTasks object
        background_tasks = BackgroundTasks()
        
        # If db is not provided, get a new connection
        if db is None:
            db = await get_arangodb_session()
        logger.info(f"Scheduled ODK fetch job started at {datetime.now().isoformat()}")
        # Call the fetch function with the required parameters
        await fetch_odk_data_with_async_endpoint(background_tasks=background_tasks, db=db)
        
        logger.info(f"Scheduled ODK fetch job executed successfully at {datetime.now().isoformat()}")
    except Exception as e:
        logger.error(f"Error executing scheduled ODK fetch job: {str(e)}")
@log_to_db(context="schedule_odk_fetch_job")
async def schedule_odk_fetch_job(db=None):
    """Schedule the ODK fetch job based on cron settings"""

    try:
        # Get cron settings
        cron_settings = await get_cron_settings(db)
        days = cron_settings.get('days', [])
        time_str = cron_settings.get('time', '00:00')
        
        # Parse time
        hour, minute = time_str.split(':')
        
        # Convert days to cron format
        cron_days = day_of_week_to_cron(days)
        
        # Remove existing job if it exists
        if scheduler.get_job('odk_fetch_job'):
            scheduler.remove_job('odk_fetch_job')
        
        # Schedule new job using the wrapper function
        scheduler.add_job(
            odk_fetch_job_wrapper,
            CronTrigger(day_of_week=cron_days, hour=int(hour), minute=int(minute)),
            id='odk_fetch_job',
            replace_existing=True,
            kwargs={'db': db}
        )
        
        logger.info(f"Scheduled ODK fetch job to run at {time_str} on days: {', '.join(days) if days else 'every day'}")
        
        # Also schedule a job to check for updated cron settings every hour
        if not scheduler.get_job('update_cron_settings_job'):
            scheduler.add_job(
                schedule_odk_fetch_job,
                'interval',
                hours=1,
                id='update_cron_settings_job',
                kwargs={'db': db}
            )
    except Exception as e:
        logger.error(f"Error scheduling ODK fetch job: {str(e)}")

@log_to_db(context="ccva_cleanup_job")
async def ccva_cleanup_job(db=None):
    """
    Cleanup job for expired CCVA public results based on TTL.
    Privacy-first backup: Frontend deletes immediately on completion,
    but this ensures cleanup if user closes browser or deletion fails.
    """
    try:
        # If db is not provided, get a new connection
        if db is None:
            db = await get_arangodb_session()
        
        logger.info(f"CCVA cleanup job started at {datetime.now().isoformat()}")
        result = await cleanup_expired_ccva_public_results(db)
        
        if result:
            logger.info(f"CCVA cleanup job completed: Deleted {result.get('deleted_count', 0)} expired record(s)")
        else:
            logger.warning("CCVA cleanup job completed but returned no result")
            
    except Exception as e:
        logger.error(f"Error executing CCVA cleanup job: {str(e)}")

async def start_scheduler():
    db = None
    async for session in get_arangodb_session():
        
        db = session
        break  # Exit after the first yielded value

    if db is None:
        logger.error("Failed to get database session")
        return

    """Start the scheduler"""
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    
    # Schedule the initial job
    import asyncio
    loop = asyncio.get_event_loop()
    loop.create_task(schedule_odk_fetch_job(db))
    
    # Schedule CCVA cleanup job to run every 6 hours for privacy
    # This will clean up expired TTL records from CCVA_PUBLIC_RESULTS
    # Privacy-first: Frontend deletes immediately on completion, but this is a backup
    # in case user closes browser or deletion fails
    # Only schedule if CCVA Public module cleanup is enabled
    if CCVA_PUBLIC_CLEANUP_ENABLED and not scheduler.get_job('ccva_cleanup_job'):
        scheduler.add_job(
            ccva_cleanup_job,
            'interval',
            hours=6,  # Run every 6 hours for faster cleanup
            id='ccva_cleanup_job',
            replace_existing=True,
            kwargs={'db': db}
        )
        logger.info("Scheduled CCVA cleanup job to run every 6 hours (privacy-first backup)")

async def shutdown_scheduler():
    """Shutdown the scheduler"""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down")
        # 1. First, flush any remaining logs in the database logger
    try:
        from app.utilits.db_logger import db_logger, background_processor
        
        # Flush the buffer
        await db_logger.flush_buffer()
        
        # Stop the background processor
        background_processor.stop()
        
        # Wait for the queue to be processed (with timeout)
        background_processor.queue.join(timeout=5.0)
        
        app_logger.info("Database logger shutdown complete")
    except Exception as e:
        app_logger.error(f"Error shutting down database logger: {str(e)}")
    
    # 2. Then, close all handlers in the standard logger
    try:
        for handler in app_logger.handlers:
            if hasattr(handler, 'close'):
                handler.close()
        
        app_logger.info("Standard logger shutdown complete")
    except Exception as e:
        # Can't log this error through the logger since we're shutting it down
        print(f"Error shutting down standard logger: {str(e)}")
    
    # 3. Allow a small delay for final log processing
    await asyncio.sleep(0.5)
        
