import asyncio
from functools import wraps
import json
import threading
import queue
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from arango.database import StandardDatabase
from fastapi import Depends

from app.shared.configs.arangodb import get_arangodb_session
from app.shared.configs.constants import db_collections
from app.utilits import logger
from app.utilits.logger import app_logger

# Define log levels
class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class BackgroundProcessor:
    """Background processor for database operations"""
    
    def __init__(self, collection_name="system_logs"):
        self.collection_name = collection_name
        self.queue = queue.Queue()
        self.running = False
        self.db = None
        self.worker_thread = None
        
    def start(self):
        """Start the background processor"""
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=lambda: asyncio.run(self._worker_loop()), daemon=True)
            self.worker_thread.start()
            app_logger.info("Background log processor started")
            
    def stop(self):
        """Stop the background processor"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=2.0)
            app_logger.info("Background log processor stopped")
            
    def enqueue(self, operation, *args, **kwargs):
        """Add an operation to the queue"""
        self.queue.put((operation, args, kwargs))
        
    async def _worker_loop(self):
        """Worker loop that processes operations from the queue"""
        while self.running:
            try:
                # Get database connection if needed
                if self.db is None:
                    try:
                        # Use a synchronous approach to get the database

                        async for session in get_arangodb_session():
                            print("Session: ", session)
         
                            
                            self.db = session
                            break  # Exit after the first yielded value

                        if self.db  is None:
                            app_logger.error("Failed to get database session")
                            return
                        # from app.shared.configs.arangodb import get_arangodb_session
                        # self.db =  get_arangodb_session()
                        
                        # Ensure collection exists
                        collections =  self.db.collections()
                        collection_names = [c['name'] for c in collections]
                        
                        if self.collection_name not in collection_names:
                            self.db.create_collection(self.collection_name)
                            
                            # Create indexes for faster queries
                            self.db.collection(self.collection_name).add_hash_index(["level", "timestamp"])
                            self.db.collection(self.collection_name).add_hash_index(["module", "timestamp"])
                            self.db.collection(self.collection_name).add_hash_index(["context", "timestamp"])
                            
                            app_logger.info(f"Created logs collection: {self.collection_name}")
                    except Exception as e:
                        print(f"Failed to get database connection: {str(e)}")
                        app_logger.error(f"Failed to get database connection: {str(e)}")
                        time.sleep(5)  # Wait before retrying
                        continue
                
                # Process operations from the queue
                try:
                    # Get an operation with a timeout
                    operation, args, kwargs = self.queue.get(timeout=1.0)
                    
                    # Execute the operation
                    if operation == "insert":
                        log_doc = args[0]
                        try:
                            self.db.collection(self.collection_name).insert(log_doc)
                        except Exception as e:
                            app_logger.error(f"Failed to insert log to database: {str(e)}")
                    
                    elif operation == "insert_many":
                        logs = args[0]
                        try:
                            self.db.collection(self.collection_name).insert_many(logs)
                        except Exception as e:
                            app_logger.error(f"Failed to insert logs to database: {str(e)}")
                    
                    elif operation == "query":
                        aql = args[0]
                        bind_vars = args[1]
                        callback = kwargs.get("callback")
                        try:
                            cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
                            result = [doc for doc in cursor]
                            if callback:
                                callback(result)
                        except Exception as e:
                            app_logger.error(f"Failed to execute query: {str(e)}")
                            if callback:
                                callback([])
                    
                    # Mark the task as done
                    self.queue.task_done()
                    
                except queue.Empty:
                    # No operations to process
                    pass
                    
            except Exception as e:
                app_logger.error(f"Error in background processor: {str(e)}")
                time.sleep(1)  # Sleep on error

# Create a global background processor
background_processor = BackgroundProcessor()
background_processor.start()

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
        # This is now handled by the background processor
        # We keep this method for compatibility

        pass
    
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
        try:
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
                try:
                    log_doc["exception"] = {
                        "type": exception.__class__.__name__,
                        "message": str(exception),
                        "traceback": traceback.format_exception(
                            type(exception), exception, exception.__traceback__
                        )
                    }
                except Exception as e:
                    app_logger.error(f"Failed to format exception details: {str(e)}")
            
            # Log to standard logger as well
            log_method = getattr(app_logger, level.lower(), app_logger.info)
            log_method(message, extra={"db_log": True, "context": context})
            
            if immediate:
                # Insert immediately using background processor
                background_processor.enqueue("insert", log_doc)
            else:
                # Add to buffer
                async with self.buffer_lock:
                    self.buffer.append(log_doc)
                    
                    # Flush buffer if it reaches the threshold
                    if len(self.buffer) >= self.buffer_size:
                        await self.flush_buffer()
        except Exception as e:
            # Catch any errors in the logging process
            app_logger.error(f"Error in log method: {str(e)}")
    
    async def flush_buffer(self):
        """Flush the log buffer to the database"""
        try:
            async with self.buffer_lock:
                if not self.buffer:
                    return
                    
                # Copy the buffer and clear it
                logs_to_insert = self.buffer.copy()
                self.buffer = []
                
                # Insert logs using background processor
                background_processor.enqueue("insert_many", logs_to_insert)
        except Exception as e:
            app_logger.error(f"Error in flush_buffer: {str(e)}")
    
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
        
        # Capture the current loop
        loop = asyncio.get_running_loop()
        
        def set_result(result):
            loop.call_soon_threadsafe(future.set_result, result)
        
        # Execute query using background processor
        background_processor.enqueue("query", aql, bind_vars, callback=set_result)
        
        # Wait for the result
        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            app_logger.error("Timeout waiting for query result")
            return []
    
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
        
        # Create a future to get the result
        future = asyncio.Future()
        
        # Capture the current loop
        loop = asyncio.get_running_loop()
        
        def set_result(result):
            loop.call_soon_threadsafe(future.set_result, result)
        
        # Execute query using background processor
        background_processor.enqueue("query", aql, {}, callback=set_result)
        
        # Wait for the result
        try:
            result = await asyncio.wait_for(future, timeout=10.0)
            return result[0] if result else {}
        except asyncio.TimeoutError:
            app_logger.error("Timeout waiting for stats result")
            return {}

# Create a global DB logger instance
db_logger = None

async def get_db_logger():
    db = None
    async for session in get_arangodb_session():
        
        db = session
        break  # Exit after the first yielded value

    if db is None:
        logger.error("Failed to get database session")
        return
    global db_logger
    if db_logger is None:
        db_logger = DBLogger(db=db)
    return db_logger

# Decorator for logging function calls and errors to the database
def log_to_db(context: Optional[str] = None, log_args: bool = False):
    """Decorator to log function calls and errors to the database"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            function_name = context or func.__name__
            module_name = func.__module__
            
            # Log function call without blocking
            try:
                data = None
                if log_args:
                    try:
                        data = {
                            "args": str(args),
                            "kwargs": str({k: v for k, v in kwargs.items() if k != "db"})
                        }
                    except Exception as e:
                        app_logger.error(f"Failed to format log args: {str(e)}")
                
                # Create task for logging function call
                asyncio.create_task(
                    db_logger.log(
                        message=f"Calling {function_name}",
                        level=LogLevel.DEBUG,
                        context=function_name,
                        module=module_name,
                        data=data
                    )
                )
            except Exception as e:
                app_logger.error(f"Failed to log function call: {str(e)}")
            
            try:
                # Execute the function
                result = await func(*args, **kwargs)
                
                # Log success without blocking
                try:
                    asyncio.create_task(
                        db_logger.log(
                            message=f"{function_name} completed successfully",
                            level=LogLevel.DEBUG,
                            context=function_name,
                            module=module_name
                        )
                    )
                except Exception as e:
                    app_logger.error(f"Failed to log function success: {str(e)}")
                
                return result
            except Exception as e:
                # Log error to app_logger
                app_logger.error(f"Error in {function_name}: {str(e)}")
                
                # Log error to database without blocking
                try:
                    asyncio.create_task(
                        db_logger.log(
                            message=f"Error in {function_name}: {str(e)}",
                            level=LogLevel.ERROR,
                            context=function_name,
                            module=module_name,
                            exception=e,
                            data=data,
                            immediate=True
                        )
                    )
                except Exception as log_error:
                    app_logger.error(f"Failed to log function error: {str(log_error)}")
                
                # Re-raise the original exception
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            function_name = context or func.__name__
            module_name = func.__module__
            
            # Log function call without blocking
            try:
                data = None
                if log_args:
                    try:
                        data = {
                            "args": str(args),
                            "kwargs": str({k: v for k, v in kwargs.items() if k != "db"})
                        }
                    except Exception as e:
                        app_logger.error(f"Failed to format log args: {str(e)}")
                
                # Log to database in background
                background_processor.enqueue(
                    "insert", 
                    {
                        "message": f"Calling {function_name}",
                        "level": LogLevel.DEBUG,
                        "timestamp": datetime.utcnow().isoformat(),
                        "context": function_name,
                        "module": module_name,
                        "data": data or {}
                    }
                )
            except Exception as e:
                app_logger.error(f"Failed to log function call: {str(e)}")
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log success without blocking
                try:
                    background_processor.enqueue(
                        "insert", 
                        {
                            "message": f"{function_name} completed successfully",
                            "level": LogLevel.DEBUG,
                            "timestamp": datetime.utcnow().isoformat(),
                            "context": function_name,
                            "module": module_name,
                            "data": {}
                        }
                    )
                except Exception as e:
                    app_logger.error(f"Failed to log function success: {str(e)}")
                
                return result
            except Exception as e:
                # Log error to app_logger
                app_logger.error(f"Error in {function_name}: {str(e)}")
                
                # Log error to database without blocking
                try:
                    # Create error log document
                    log_doc = {
                        "message": f"Error in {function_name}: {str(e)}",
                        "level": LogLevel.ERROR,
                        "timestamp": datetime.utcnow().isoformat(),
                        "context": function_name,
                        "module": module_name,
                        "data": data or {}
                    }
                    
                    # Add exception details
                    try:
                        log_doc["exception"] = {
                            "type": e.__class__.__name__,
                            "message": str(e),
                            "traceback": traceback.format_exception(
                                type(e), e, e.__traceback__
                            )
                        }
                    except Exception as ex:
                        app_logger.error(f"Failed to format exception details: {str(ex)}")
                    
                    # Insert immediately
                    background_processor.enqueue("insert", log_doc)
                except Exception as log_error:
                    app_logger.error(f"Failed to log function error: {str(log_error)}")
                
                # Re-raise the original exception
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator