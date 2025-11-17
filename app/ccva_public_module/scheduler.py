"""
Scheduler for CCVA Public Module
Handles cleanup of expired TTL records
"""
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.ccva_public_module.config import (
    CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS,
    CCVA_PUBLIC_CLEANUP_ENABLED
)
from app.ccva.services.ccva_public_services import cleanup_expired_ccva_public_results
from app.shared.configs.arangodb import get_arangodb_session
from app.utilits.db_logger import log_to_db
from app.utilits.logger import app_logger

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a scheduler instance for CCVA Public Module
ccva_public_scheduler = AsyncIOScheduler()


@log_to_db(context="ccva_public_cleanup_job")
async def ccva_public_cleanup_job(db=None):
    """
    Cleanup job for expired CCVA public results based on TTL.
    Privacy-first backup: Frontend deletes immediately on completion,
    but this ensures cleanup if user closes browser or deletion fails.
    """
    try:
        # If db is not provided, get a new connection
        if db is None:
            async for session in get_arangodb_session():
                db = session
                break
        
        logger.info(f"CCVA Public cleanup job started at {datetime.now().isoformat()}")
        result = await cleanup_expired_ccva_public_results(db)
        
        if result:
            logger.info(f"CCVA Public cleanup job completed: Deleted {result.get('deleted_count', 0)} expired record(s)")
            app_logger.info(f"CCVA Public cleanup: Deleted {result.get('deleted_count', 0)} expired record(s)")
        else:
            logger.warning("CCVA Public cleanup job completed but returned no result")
            
    except Exception as e:
        logger.error(f"Error executing CCVA Public cleanup job: {str(e)}")
        app_logger.error(f"Error executing CCVA Public cleanup job: {str(e)}")


async def initialize_ccva_public_scheduler():
    """Initialize scheduler for CCVA Public Module"""
    if not CCVA_PUBLIC_CLEANUP_ENABLED:
        logger.info("CCVA Public cleanup scheduler is disabled")
        return
    
    try:
        # Get database session
        db = None
        async for session in get_arangodb_session():
            db = session
            break
        
        if db is None:
            logger.error("Failed to get database session for CCVA Public scheduler")
            return
        
        # Start scheduler if not running
        if not ccva_public_scheduler.running:
            ccva_public_scheduler.start()
            logger.info("CCVA Public scheduler started")
        
        # Schedule cleanup job
        if not ccva_public_scheduler.get_job('ccva_public_cleanup_job'):
            ccva_public_scheduler.add_job(
                ccva_public_cleanup_job,
                IntervalTrigger(hours=CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS),
                id='ccva_public_cleanup_job',
                replace_existing=True,
                kwargs={'db': db}
            )
            logger.info(f"CCVA Public cleanup job scheduled to run every {CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS} hours")
        
    except Exception as e:
        logger.error(f"Error initializing CCVA Public scheduler: {str(e)}")


async def shutdown_ccva_public_scheduler():
    """Shutdown scheduler for CCVA Public Module"""
    try:
        if ccva_public_scheduler.running:
            ccva_public_scheduler.shutdown()
            logger.info("CCVA Public scheduler shut down")
    except Exception as e:
        logger.error(f"Error shutting down CCVA Public scheduler: {str(e)}")

