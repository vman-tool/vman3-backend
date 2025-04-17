import asyncio
from functools import wraps
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from arango.database import StandardDatabase
from fastapi import Depends

from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import db_collections
from app.utilits.logger import app_logger

# Define log levels
class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class DBLogger:
    """Database logger for storing important logs in ArangoDB"""
    
    def __init__(self, db: StandardDatabase = None):
        self.db = db
        self.collection_name = "system_logs"
        self.buffer = []
        self.buffer_size = 10  # Number of logs to buffer before bulk insert
        self.buffer_lock = asyncio.Lock()
        
    async def ensure_collection(self):
        """Ensure the logs collection exists"""


        async for session in get_arangodb_session():
            self.db = session
            break  # Exit after the first yielded value

            
        # Check if collection exists, create if not
        collections = self.db.collections()
        collection_names = [c['name'] for c in collections]
        
        if self.collection_name not in collection_names:
            self.db.create_collection(self.collection_name)
            
            # Create indexes for faster queries
            self.db.collection(self.collection_name).add_hash_index(["level", "timestamp"])
            self.db.collection(self.collection_name).add_hash_index(["module", "timestamp"])
            self.db.collection(self.collection_name).add_hash_index(["context", "timestamp"])
            
            app_logger.info(f"Created logs collection: {self.collection_name}")
    
    async def log(self, 
                 message: str, 
                 level: str = LogLevel.INFO, 
                 context: Optional[str] = None,
                 module: Optional[str] = None,
                 exception: Optional[Exception] = None,
                 data: Optional[Dict[str, Any]] = None,
                 user_id: Optional[str] = None,
                 immediate: bool = False):
        """
        Log a message to the database
        
        Args:
            message: The log message
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            context: Context of the log (e.g., function name, API endpoint)
            module: Module where the log originated
            exception: Exception object if this is an error log
            data: Additional data to store with the log
            user_id: ID of the user who triggered the action
            immediate: Whether to insert immediately or buffer
        """
        await self.ensure_collection()
        
        # Create log document
        log_doc = {
            "message": message,
            "level": level,
            "timestamp": datetime.utcnow().isoformat(),
            "context": context,
            "module": module,
            "user_id": user_id,
            "data": data or {}
        }
        
        # Add exception details if provided
        if exception:
            import traceback
            log_doc["exception"] = {
                "type": exception.__class__.__name__,
                "message": str(exception),
                "traceback": traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                )
            }
        
        # Log to standard logger as well
        log_method = getattr(app_logger, level.lower(), app_logger.info)
        log_method(message, extra={"db_log": True, "context": context})
        
        if immediate:
            # Insert immediately
            try:
                self.db.collection(self.collection_name).insert(log_doc)
            except Exception as e:
                app_logger.error(f"Failed to insert log to database: {str(e)}")
        else:
            # Add to buffer
            async with self.buffer_lock:
                self.buffer.append(log_doc)
                
                # Flush buffer if it reaches the threshold
                if len(self.buffer) >= self.buffer_size:
                    await self.flush_buffer()
    
    async def flush_buffer(self):
        """Flush the log buffer to the database"""
        async with self.buffer_lock:
            if not self.buffer:
                return
                
            try:
                # Insert all buffered logs
                self.db.collection(self.collection_name).insert_many(self.buffer)
                self.buffer = []
            except Exception as e:
                app_logger.error(f"Failed to flush log buffer: {str(e)}")
    
    async def get_logs(self, 
                      level: Optional[str] = None,
                      context: Optional[str] = None,
                      module: Optional[str] = None,
                      user_id: Optional[str] = None,
                      start_time: Optional[str] = None,
                      end_time: Optional[str] = None,
                      limit: int = 100,
                      offset: int = 0,
                      sort_direction: str = "DESC") -> List[Dict[str, Any]]:
        """
        Query logs from the database
        
        Args:
            level: Filter by log level
            context: Filter by context
            module: Filter by module
            user_id: Filter by user ID
            start_time: Filter by start time (ISO format)
            end_time: Filter by end time (ISO format)
            limit: Maximum number of logs to return
            offset: Offset for pagination
            sort_direction: Sort direction (ASC or DESC)
            
        Returns:
            List of log documents
        """
        await self.ensure_collection()
        
        # Build AQL query
        aql = "FOR log IN system_logs"
        filters = []
        bind_vars = {}
        
        if level:
            filters.append("log.level == @level")
            bind_vars["level"] = level
            
        if context:
            filters.append("log.context == @context")
            bind_vars["context"] = context
            
        if module:
            filters.append("log.module == @module")
            bind_vars["module"] = module
            
        if user_id:
            filters.append("log.user_id == @user_id")
            bind_vars["user_id"] = user_id
            
        if start_time:
            filters.append("log.timestamp >= @start_time")
            bind_vars["start_time"] = start_time
            
        if end_time:
            filters.append("log.timestamp <= @end_time")
            bind_vars["end_time"] = end_time
            
        if filters:
            aql += " FILTER " + " AND ".join(filters)
            
        # Add sorting
        sort_dir = "DESC" if sort_direction.upper() == "DESC" else "ASC"
        aql += f" SORT log.timestamp {sort_dir}"
        
        # Add pagination
        aql += " LIMIT @offset, @limit"
        bind_vars["offset"] = offset
        bind_vars["limit"] = limit
        
        aql += " RETURN log"
        
        # Execute query
        cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
        return [doc for doc in cursor]
    
    async def get_error_logs(self, 
                           start_time: Optional[str] = None,
                           end_time: Optional[str] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """Get error and critical logs"""
        return await self.get_logs(
            level=LogLevel.ERROR,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
    
    async def get_log_stats(self) -> Dict[str, Any]:
        """Get log statistics"""
        await self.ensure_collection()
        
        # Get counts by level
        aql = """
        RETURN {
            total: LENGTH(FOR log IN system_logs RETURN 1),
            by_level: (
                FOR log IN system_logs
                COLLECT level = log.level WITH COUNT INTO count
                RETURN { level, count }
            ),
            recent_errors: (
                FOR log IN system_logs
                FILTER log.level IN ["ERROR", "CRITICAL"]
                SORT log.timestamp DESC
                LIMIT 5
                RETURN log
            )
        }
        """
        
        cursor = self.db.aql.execute(aql)
        stats = [doc for doc in cursor]
        return stats[0] if stats else {}

# Create a global DB logger instance
db_logger = DBLogger()

# Decorator for logging function calls and errors to the database
def log_to_db(context: Optional[str] = None, log_args: bool = False):
    """Decorator to log function calls and errors to the database"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            function_name = context or func.__name__
            module_name = func.__module__
            
            # Log function call
            data = None
            if log_args:
                data = {
                    "args": str(args),
                    "kwargs": str({k: v for k, v in kwargs.items() if k != "db"})
                }
                
            await db_logger.log(
                message=f"Calling {function_name}",
                level=LogLevel.DEBUG,
                context=function_name,
                module=module_name,
                data=data
            )
            
            try:
                result = await func(*args, **kwargs)
                
                # Log success
                await db_logger.log(
                    message=f"{function_name} completed successfully",
                    level=LogLevel.DEBUG,
                    context=function_name,
                    module=module_name
                )
                
                return result
            except Exception as e:
                # Log error
                await db_logger.log(
                    message=f"Error in {function_name}: {str(e)}",
                    level=LogLevel.ERROR,
                    context=function_name,
                    module=module_name,
                    exception=e,
                    data=data,
                    immediate=True  # Errors are logged immediately
                )
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            function_name = context or func.__name__
            module_name = func.__module__
            
            # For synchronous functions, we need to run the async log function in a new event loop
            async def log_async():
                # Log function call
                data = None
                if log_args:
                    data = {
                        "args": str(args),
                        "kwargs": str({k: v for k, v in kwargs.items() if k != "db"})
                    }
                    
                await db_logger.log(
                    message=f"Calling {function_name}",
                    level=LogLevel.DEBUG,
                    context=function_name,
                    module=module_name,
                    data=data
                )
            
            asyncio.run(log_async())
            
            try:
                result = func(*args, **kwargs)
                
                # Log success
                async def log_success():
                    await db_logger.log(
                        message=f"{function_name} completed successfully",
                        level=LogLevel.DEBUG,
                        context=function_name,
                        module=module_name
                    )
                
                asyncio.run(log_success())
                
                return result
            except Exception as e:
                # Log error
                async def log_error(exception):
                    await db_logger.log(
                        message=f"Error in {function_name}: {str(exception)}",
                        level=LogLevel.ERROR,
                        context=function_name,
                        module=module_name,
                        exception=exception,
                        immediate=True  # Errors are logged immediately
                    )
                
                asyncio.run(log_error(e))
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator