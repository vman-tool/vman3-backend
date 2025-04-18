import asyncio
import json
import logging
import os
import sys
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

# Create logger
def setup_logger(name: str = "app") -> logging.Logger:
    """Set up and configure a logger"""
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(console_handler)
    
    # File handler for all logs
    all_logs_file = os.path.join(LOG_DIR, "app.log")
    file_handler = RotatingFileHandler(
        all_logs_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    
    # File handler for errors only
    error_logs_file = os.path.join(LOG_DIR, "errors.log")
    error_file_handler = RotatingFileHandler(
        error_logs_file, maxBytes=10*1024*1024, backupCount=5
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(JsonFormatter())
    logger.addHandler(error_file_handler)
    
    # Daily rotating file handler
    daily_logs_file = os.path.join(LOG_DIR, "daily.log")
    daily_file_handler = TimedRotatingFileHandler(
        daily_logs_file, when="midnight", backupCount=30
    )
    daily_file_handler.setFormatter(JsonFormatter())
    logger.addHandler(daily_file_handler)
    
    return logger

# Create a default application logger
app_logger = setup_logger("app")

# Decorator for logging function calls and errors
def log_function(logger: Optional[logging.Logger] = None):
    """Decorator to log function calls and errors"""
    if logger is None:
        logger = app_logger
        
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            function_name = func.__name__
            logger.debug(f"Calling {function_name}", extra={"function_args": str(args), "function_kwargs": str(kwargs)})
            try:
                result = await func(*args, **kwargs)
                logger.debug(f"{function_name} completed successfully")
                return result
            except Exception as e:
                logger.error(
                    f"Error in {function_name}: {str(e)}",
                    exc_info=True,
                    extra={
                        "function_args": str(args),
                        "function_kwargs": str(kwargs)
                    }
                )
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            function_name = func.__name__
            logger.debug(f"Calling {function_name}", extra={"function_args": str(args), "function_kwargs": str(kwargs)})
            try:
                result = func(*args, **kwargs)
                logger.debug(f"{function_name} completed successfully")
                return result
            except Exception as e:
                logger.error(
                    f"Error in {function_name}: {str(e)}",
                    exc_info=True,
                    extra={
                        "function_args": str(args),
                        "function_kwargs": str(kwargs)
                    }
                )
                raise
                
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator

# Context manager for logging blocks of code
class LogContext:
    """Context manager for logging blocks of code"""
    def __init__(self, context_name: str, logger: Optional[logging.Logger] = None):
        self.context_name = context_name
        self.logger = logger or app_logger
        
    def __enter__(self):
        self.logger.debug(f"Entering context: {self.context_name}")
        self.start_time = datetime.now()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
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
        return False  # Don't suppress exceptions