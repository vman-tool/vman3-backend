import asyncio
import json
import logging
import os
import sys
import threading
import queue
import time
import traceback
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Any, Callable, Dict, Optional, Type, Union

# Configure log directory
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Configure log levels
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# Custom JSON formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if available
        if record.exc_info:
            log_record["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
            
        # Add extra fields if available
        if hasattr(record, "extra"):
            log_record.update(record.extra)
            
        return json.dumps(log_record)

# Non-blocking handler
class NonBlockingHandler(logging.Handler):
    def __init__(self, target_handler):
        super().__init__(level=target_handler.level)
        self.target_handler = target_handler
        self.queue = queue.Queue(maxsize=10000)  # Limit queue size to prevent memory issues
        self.thread = threading.Thread(target=self._process_logs, daemon=True)
        self.thread.start()
        self.formatter = target_handler.formatter
        
    def emit(self, record):
        try:
            # Add to queue, with a timeout to prevent blocking if queue is full
            self.queue.put(record, block=True, timeout=0.1)
        except queue.Full:
            # If queue is full, log a warning and drop the message
            sys.stderr.write("WARNING: Log queue is full, dropping log message\n")
        except Exception:
            self.handleError(record)
            
    def _process_logs(self):
        while True:
            try:
                record = self.queue.get()
                self.target_handler.emit(record)
                self.queue.task_done()
            except Exception:
                # Just continue processing logs even if one fails
                pass
            
    def close(self):
        # Process remaining logs
        try:
            self.queue.join(timeout=5.0)  # Wait up to 5 seconds
        except:
            pass
        self.target_handler.close()
        super().close()

# Create logger with non-blocking handlers
def setup_logger(name: str = "app") -> logging.Logger:
    """Set up and configure a logger with non-blocking handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(NonBlockingHandler(console_handler))
    
    # File handler for all logs
    all_logs_file = os.path.join(LOG_DIR, "app.log")
    file_handler = RotatingFileHandler(
        all_logs_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(NonBlockingHandler(file_handler))
    
    # File handler for errors only
    error_logs_file = os.path.join(LOG_DIR, "errors.log")
    error_file_handler = RotatingFileHandler(
        error_logs_file, maxBytes=10*1024*1024, backupCount=5
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(JsonFormatter())
    logger.addHandler(NonBlockingHandler(error_file_handler))
    
    # Daily rotating file handler
    daily_logs_file = os.path.join(LOG_DIR, "daily.log")
    daily_file_handler = TimedRotatingFileHandler(
        daily_logs_file, when="midnight", backupCount=30
    )
    daily_file_handler.setFormatter(JsonFormatter())
    logger.addHandler(NonBlockingHandler(daily_file_handler))
    
    return logger

# Create a default application logger
app_logger = setup_logger("app")

# Decorator for logging function calls and errors - non-blocking version
def log_function(logger: Optional[logging.Logger] = None):
    """Decorator to log function calls and errors without blocking"""
    if logger is None:
        logger = app_logger
        
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            function_name = func.__name__
            
            # Log function call without blocking
            try:
                logger.debug(f"Calling {function_name}", extra={"function_args": str(args), "function_kwargs": str(kwargs)})
            except Exception as log_error:
                # Don't let logging errors affect the function
                pass
                
            try:
                # Execute the function
                result = await func(*args, **kwargs)
                
                # Log success without blocking
                try:
                    logger.debug(f"{function_name} completed successfully")
                except Exception as log_error:
                    # Don't let logging errors affect the function
                    pass
                    
                return result
            except Exception as e:
                # Log error without blocking
                try:
                    logger.error(
                        f"Error in {function_name}: {str(e)}",
                        exc_info=True,
                        extra={
                            "function_args": str(args),
                            "function_kwargs": str(kwargs)
                        }
                    )
                except Exception as log_error:
                    # Don't let logging errors affect the function
                    pass
                    
                # Re-raise the original exception
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            function_name = func.__name__
            
            # Log function call without blocking
            try:
                logger.debug(f"Calling {function_name}", extra={"function_args": str(args), "function_kwargs": str(kwargs)})
            except Exception as log_error:
                # Don't let logging errors affect the function
                pass
                
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log success without blocking
                try:
                    logger.debug(f"{function_name} completed successfully")
                except Exception as log_error:
                    # Don't let logging errors affect the function
                    pass
                    
                return result
            except Exception as e:
                # Log error without blocking
                try:
                    logger.error(
                        f"Error in {function_name}: {str(e)}",
                        exc_info=True,
                        extra={
                            "function_args": str(args),
                            "function_kwargs": str(kwargs)
                        }
                    )
                except Exception as log_error:
                    # Don't let logging errors affect the function
                    pass
                    
                # Re-raise the original exception
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator

# Context manager for logging blocks of code - non-blocking version
class LogContext:
    """Context manager for logging blocks of code without blocking"""
    def __init__(self, context_name: str, logger: Optional[logging.Logger] = None):
        self.context_name = context_name
        self.logger = logger or app_logger
        
    def __enter__(self):
        try:
            self.logger.debug(f"Entering context: {self.context_name}")
        except Exception:
            # Don't let logging errors affect the context
            pass
            
        self.start_time = datetime.now()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        
        try:
            if exc_type:
                self.logger.error(
                    f"Error in context {self.context_name}: {str(exc_val)}",
                    exc_info=(exc_type, exc_val, exc_tb),
                    extra={"duration_seconds": duration}
                )
            else:
                self.logger.debug(
                    f"Exiting context: {self.context_name}",
                    extra={"duration_seconds": duration}
                )
        except Exception:
            # Don't let logging errors affect the context
            pass
            
        return False  # Don't suppress exceptions