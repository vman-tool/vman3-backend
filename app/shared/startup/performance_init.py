"""
Performance initialization for database optimization
This module ensures critical indexes are created at startup
"""
import asyncio
from loguru import logger
from arango.database import StandardDatabase
from app.shared.configs.arangodb import get_arangodb_session


async def initialize_performance_optimizations():
    """Initialize all performance optimizations at startup"""
    logger.info("Initializing database performance optimizations...")
    
    try:
        # Get database session
        async for db in get_arangodb_session():
            await setup_database_indexes(db)
            await validate_performance_setup(db)
            break  # Only need one session for setup
            
        logger.info("✅ Database performance optimizations completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize performance optimizations: {e}")
        # Don't raise - app should still start even if optimizations fail
        

async def setup_database_indexes(db: StandardDatabase):
    """Ensure critical indexes exist for optimal query performance"""
    logger.info("Setting up database indexes for optimal performance...")
    
    try:
        from app.shared.utils.performance import DatabaseIndexManager
        await DatabaseIndexManager.ensure_indexes(db)
        logger.info("✅ Database indexes setup completed")
        
    except ImportError:
        logger.warning("Performance utils not available, skipping index optimization")
    except Exception as e:
        logger.error(f"❌ Failed to setup database indexes: {e}")


async def validate_performance_setup(db: StandardDatabase):
    """Validate that performance optimizations are working"""
    logger.info("Validating performance setup...")
    
    try:
        # Test the optimized statistics function
        from app.odk.services.data_download import get_margin_dates_and_records_count
        import time
        
        start_time = time.time()
        stats = await get_margin_dates_and_records_count(db)
        execution_time = time.time() - start_time
        
        if execution_time > 2.0:
            logger.warning(f"⚠️  Statistics query took {execution_time:.2f}s - consider adding indexes")
        else:
            logger.info(f"✅ Statistics query completed in {execution_time:.3f}s")
            
        logger.info(f"📊 Database stats: {stats.get('total_records', 0)} records")
        
    except Exception as e:
        logger.error(f"❌ Performance validation failed: {e}")


async def clear_performance_caches():
    """Clear all performance caches (useful for development/debugging)"""
    logger.info("Clearing performance caches...")
    
    try:
        from app.shared.utils.performance import StatisticsCache, CollectionCache
        
        # Get database session to clear caches
        async for db in get_arangodb_session():
            StatisticsCache.invalidate(db)
            CollectionCache.invalidate(db)
            break
            
        logger.info("✅ Performance caches cleared")
        
    except ImportError:
        logger.warning("Performance utils not available")
    except Exception as e:
        logger.error(f"❌ Failed to clear caches: {e}")


# Background task to refresh caches periodically
async def refresh_statistics_cache():
    """Background task to refresh statistics cache periodically"""
    while True:
        try:
            logger.debug("Refreshing statistics cache in background...")
            
            async for db in get_arangodb_session():
                from app.shared.utils.performance import StatisticsCache
                from app.shared.configs import db_collections
                
                # Refresh cache for main data table
                await StatisticsCache.get_record_stats(db, db_collections.VA_TABLE)
                break
                
            logger.debug("✅ Statistics cache refreshed")
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh statistics cache: {e}")
        
        # Wait 5 minutes before next refresh
        await asyncio.sleep(300)


if __name__ == "__main__":
    # For testing the performance initialization
    asyncio.run(initialize_performance_optimizations())

