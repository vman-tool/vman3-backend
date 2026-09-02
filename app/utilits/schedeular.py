import asyncio
import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler


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

# ODK sync scheduling (day/time from cron_settings) used to also be driven
# from here via an in-process APScheduler job, alongside Celery beat's
# check_odk_sync_schedule (app/tasks/odk_tasks.py, registered in
# app/celery_app.py's beat_schedule). Both independently read the same
# cron_settings and dispatch the same run_scheduled_odk_sync task, but only
# the Celery beat path deduplicates fires with a Redis lock - the
# APScheduler path here had none, so whenever a schedule was configured,
# the scheduled sync could fire twice (once from each), which is likely
# what made syncing look like it was ignoring the configured schedule. This
# in-process copy was removed; Celery beat (running independently of this
# API process, and correctly deduplicated) is now the only trigger.

#@log_to_db(context="ccva_cleanup_job")
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

async def ccva_stale_task_check_job(db=None):
    """
    Check for CCVA tasks that are stuck in 'running' or 'init' state
    and haven't been updated in 10 minutes.
    """
    try:
        if db is None:
            # Re-get the session if it's lost
            async for session in get_arangodb_session():
                db = session
                break
        
        if db is None:
            return

        # Query for stale tasks
        ten_minutes_ago = (datetime.utcnow() - timedelta(minutes=10)).isoformat()
        
        aql = f"""
        FOR task IN {db_collections.TASK_PROGRESS}
        FILTER task.status IN ['running', 'init']
        FILTER task.timestamp < @ten_minutes_ago
        UPDATE task WITH {{ 
            status: 'failed', 
            message: 'Task stalled or worker crashed (detected by background monitor).',
            error: true,
            timestamp: @now
        }} IN {db_collections.TASK_PROGRESS}
        RETURN NEW
        """
        
        bind_vars = {
            "ten_minutes_ago": ten_minutes_ago,
            "now": datetime.utcnow().isoformat()
        }
        
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        updated = [doc for doc in cursor]
        
        if updated:
            logger.info(f"Stale task monitor: Flagged {len(updated)} orphaned task(s) as failed.")
            
    except Exception as e:
        logger.error(f"Error in stale task monitor: {e}")

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

    # Schedule stale task monitoring job to run every minute
    if not scheduler.get_job('ccva_stale_task_check_job'):
        scheduler.add_job(
            ccva_stale_task_check_job,
            'interval',
            minutes=1,
            id='ccva_stale_task_check_job',
            replace_existing=True,
            kwargs={'db': db}
        )
        logger.info("Scheduled stale task monitor job (running every 1 minute)")

async def shutdown_scheduler():
    """Shutdown the scheduler"""
    try:
        if scheduler.running:
            scheduler.shutdown()
            logger.info("Scheduler shut down")
    except Exception as e:
        logger.error(f"Error shutting down scheduler: {e}")

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
        
