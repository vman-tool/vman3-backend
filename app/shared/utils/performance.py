"""
Database Performance Utilities
Critical optimizations for avoiding blocking operations
"""
import asyncio
import time
from typing import Dict, Any, Optional
from functools import wraps
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool
from loguru import logger

class CollectionCache:
    """Cache collection existence to avoid repeated database checks"""
    _cache: Dict[str, bool] = {}
    
    @classmethod
    def exists(cls, db: StandardDatabase, collection_name: str) -> bool:
        """Check if collection exists with caching"""
        cache_key = f"{id(db)}_{collection_name}"
        
        if cache_key not in cls._cache:
            try:
                cls._cache[cache_key] = db.has_collection(collection_name)
                logger.debug(f"Cached collection existence: {collection_name} = {cls._cache[cache_key]}")
            except Exception as e:
                logger.error(f"Error checking collection existence: {e}")
                return False
                
        return cls._cache[cache_key]
    
    @classmethod
    def invalidate(cls, db: StandardDatabase, collection_name: str = None):
        """Invalidate cache for specific collection or all collections"""
        if collection_name:
            cache_key = f"{id(db)}_{collection_name}"
            cls._cache.pop(cache_key, None)
        else:
            # Clear all cache entries for this database instance
            db_id = id(db)
            keys_to_remove = [k for k in cls._cache.keys() if k.startswith(f"{db_id}_")]
            for key in keys_to_remove:
                cls._cache.pop(key, None)

class StatisticsCache:
    """Cache database statistics to avoid expensive queries"""
    _cache: Dict[str, Dict[str, Any]] = {}
    _cache_timestamps: Dict[str, float] = {}
    _cache_ttl: int = 300  # 5 minutes
    
    @classmethod
    async def get_record_stats(cls, db: StandardDatabase, collection_name: str) -> Dict[str, Any]:
        """Get cached record statistics or compute if expired"""
        cache_key = f"{id(db)}_{collection_name}"
        current_time = time.time()
        
        # Check if cache is valid
        if (cache_key in cls._cache and 
            cache_key in cls._cache_timestamps and
            current_time - cls._cache_timestamps[cache_key] < cls._cache_ttl):
            logger.debug(f"Using cached stats for {collection_name}")
            return cls._cache[cache_key]
        
        # Compute fresh statistics
        logger.info(f"Computing fresh statistics for {collection_name}")
        try:
            stats = await cls._compute_stats(db, collection_name)
            cls._cache[cache_key] = stats
            cls._cache_timestamps[cache_key] = current_time
            return stats
        except Exception as e:
            logger.error(f"Error computing statistics for {collection_name}: {e}")
            # Return cached data if available, even if expired
            return cls._cache.get(cache_key, {
                'total_records': 0,
                'earliest_date': None,
                'latest_date': None
            })
    
    @classmethod
    async def _compute_stats(cls, db: StandardDatabase, collection_name: str) -> Dict[str, Any]:
        """Compute statistics using optimized queries"""
        try:
            # Use collection statistics for count (much faster than LENGTH())
            collection = db.collection(collection_name)
            collection_stats = collection.statistics()
            total_records = collection_stats.get('count', 0)
            
            if total_records == 0:
                return {
                    'total_records': 0,
                    'earliest_date': None,
                    'latest_date': None
                }
            
            # Optimized query for date range using indexes
            query = f"""
                LET latest = (
                    FOR doc IN {collection_name}
                    SORT doc.submissiondate DESC
                    LIMIT 1
                    RETURN doc.submissiondate
                )[0]
                
                LET earliest = (
                    FOR doc IN {collection_name}
                    SORT doc.submissiondate ASC
                    LIMIT 1
                    RETURN doc.submissiondate
                )[0]
                
                RETURN {{
                    total_records: {total_records},
                    latest_date: latest,
                    earliest_date: earliest
                }}
            """
            
            def execute_query():
                cursor = db.aql.execute(query, cache=True)
                return cursor.next()

            result = await run_in_threadpool(execute_query)
            return result
            
        except Exception as e:
            logger.error(f"Error in _compute_stats: {e}")
            raise
    
    @classmethod
    def invalidate(cls, db: StandardDatabase, collection_name: str = None):
        """Invalidate statistics cache"""
        if collection_name:
            cache_key = f"{id(db)}_{collection_name}"
            cls._cache.pop(cache_key, None)
            cls._cache_timestamps.pop(cache_key, None)
        else:
            # Clear all cache entries for this database instance
            db_id = id(db)
            keys_to_remove = [k for k in cls._cache.keys() if k.startswith(f"{db_id}_")]
            for key in keys_to_remove:
                cls._cache.pop(key, None)
                cls._cache_timestamps.pop(key, None)

def async_timeout(seconds: int):
    """Decorator to add timeout to async functions"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except asyncio.TimeoutError:
                logger.error(f"Function {func.__name__} timed out after {seconds} seconds")
                raise TimeoutError(f"Operation timed out after {seconds} seconds")
        return wrapper
    return decorator

class BatchProcessor:
    """Process large datasets in batches to avoid blocking"""
    
    @staticmethod
    async def process_in_batches(
        items: list,
        batch_size: int = 1000,
        process_func: callable = None,
        progress_callback: callable = None
    ):
        """Process items in batches with optional progress tracking"""
        total_items = len(items)
        processed = 0
        
        for i in range(0, total_items, batch_size):
            batch = items[i:i + batch_size]
            
            try:
                if process_func:
                    await process_func(batch)
                
                processed += len(batch)
                
                if progress_callback:
                    progress = (processed / total_items) * 100
                    await progress_callback(progress, processed, total_items)
                
                # Yield control to prevent blocking
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                raise
        
        return processed

class DatabaseIndexManager:
    """Manage database indexes for optimal performance"""
    
    CRITICAL_INDEXES = {
        'va_table': [
            {'fields': ['submissiondate'], 'type': 'persistent'},
            {'fields': ['status'], 'type': 'hash'},
            {'fields': ['submissiondate', 'status'], 'type': 'persistent'},
            {'fields': ['_key'], 'type': 'primary'},
        ],
        'system_configs': [
            {'fields': ['type'], 'type': 'hash'},
            {'fields': ['last_sync_date'], 'type': 'persistent'},
        ],
        'ccva_results': [
            {'fields': ['task_id'], 'type': 'hash'},
            {'fields': ['created_at'], 'type': 'persistent'},
            {'fields': ['task_id', 'status'], 'type': 'persistent'},
        ],
        'logs': [
            {'fields': ['level', 'timestamp'], 'type': 'persistent'},
            {'fields': ['module', 'timestamp'], 'type': 'persistent'},
            {'fields': ['context', 'timestamp'], 'type': 'persistent'},
        ]
    }
    
    @classmethod
    async def ensure_indexes(cls, db: StandardDatabase):
        """Ensure critical indexes exist for optimal performance"""
        logger.info("Checking and creating database indexes for performance...")
        
        for collection_name, indexes in cls.CRITICAL_INDEXES.items():
            if not CollectionCache.exists(db, collection_name):
                logger.warning(f"Collection {collection_name} does not exist, skipping indexes")
                continue
            
            collection = db.collection(collection_name)
            existing_indexes = {idx['fields']: idx for idx in collection.indexes()}
            
            for index_config in indexes:
                fields = index_config['fields']
                index_type = index_config['type']
                
                # Skip if index already exists
                if tuple(fields) in existing_indexes:
                    logger.debug(f"Index on {fields} already exists for {collection_name}")
                    continue
                
                try:
                    if index_type == 'hash':
                        collection.add_hash_index(fields=fields)
                    elif index_type == 'persistent':
                        collection.add_persistent_index(fields=fields)
                    elif index_type == 'primary':
                        continue  # Primary index is automatic
                    
                    logger.info(f"Created {index_type} index on {fields} for {collection_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to create index on {fields} for {collection_name}: {e}")

# Performance monitoring utilities
class PerformanceMonitor:
    """Monitor database performance metrics"""
    
    @staticmethod
    def log_query_performance(func):
        """Decorator to log query performance"""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                if execution_time > 1.0:  # Log slow queries
                    logger.warning(f"Slow query detected: {func.__name__} took {execution_time:.2f}s")
                else:
                    logger.debug(f"Query {func.__name__} completed in {execution_time:.3f}s")
                
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Query {func.__name__} failed after {execution_time:.3f}s: {e}")
                raise
        return wrapper

