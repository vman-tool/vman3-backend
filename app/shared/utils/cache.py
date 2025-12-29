from fastapi_cache.decorator import cache as _fastapi_cache
from fastapi_cache import FastAPICache
from typing import Optional
import hashlib
import json
import time
from functools import wraps

def cache(*args, **kwargs):
    """
    Wrapper around fastapi-cache2 @cache decorator with performance logging.
    """
    def decorator(func):
        cached_func = _fastapi_cache(*args, **kwargs)(func)

        @wraps(func)
        async def wrapper(*func_args, **func_kwargs):
            start_time = time.time()
            try:
                return await cached_func(*func_args, **func_kwargs)
            finally:
                end_time = time.time()
                elapsed = (end_time - start_time) * 1000
                print(f"[Performance] {func.__name__} took {elapsed:.2f}ms")
        return wrapper
    return decorator

async def invalidate_cache(key: str):
    """
    Invalidates a specific cache key using FastAPICache backend.
    Dynamically gets prefix and client from initialized FastAPICache.
    """
    backend = FastAPICache.get_backend()
    prefix = FastAPICache.get_prefix() or ""
    
    if hasattr(backend, "redis"):
        redis = backend.redis
        full_key = f"{prefix}:{key}" if prefix else key
        await redis.delete(full_key)

async def invalidate_cache_pattern(pattern: str):
    """
    Invalidates keys matching a pattern using FastAPICache backend.
    """
    backend = FastAPICache.get_backend()
    prefix = FastAPICache.get_prefix() or ""

    if hasattr(backend, "redis"):
        redis = backend.redis
        
        # Ensure pattern includes prefix
        full_pattern = f"{prefix}:{pattern}" if prefix else pattern
        
        # Use SCAN to delete keys matching pattern safely
        cursor = '0'
        while cursor != 0:
            cursor, keys = await redis.scan(cursor=cursor, match=full_pattern, count=100)
            if keys:
                await redis.delete(*keys)

def ttl_cache(ttl: int = 300, key_prefix: Optional[str] = None):
    """
    Wrapper around fastapi-cache2 @cache decorator.
    Allows explicit key_prefix for deterministic caching and invalidation.
    Includes custom key generation logic to handle 'db' and 'current_user'.
    """
    def specific_key_builder(func, namespace: str = "", request = None, response = None, *args, **kwargs):
        # 1. Static/Explicit Key
        if key_prefix:
            return key_prefix
        
        # 2. Dynamic Key Generation
        key_parts = [func.__name__]
        
        filtered_args = []
        for arg in args:
            # Skip complex objects like DB session or Arango objects
            if hasattr(arg, 'aql') or hasattr(arg, 'collection'): 
                continue
            filtered_args.append(str(arg))
        
        filtered_kwargs = []
        for k, v in sorted(kwargs.items()):
            if k == 'db':
                continue
            if k == 'current_user':
                # Handle user dict intelligently to prefer ID/UID
                if isinstance(v, dict):
                   if 'uid' in v:
                        filtered_kwargs.append(f"{k}={v['uid']}")
                   elif 'id' in v:
                         filtered_kwargs.append(f"{k}={v['id']}")
                   else:
                       # Fallback hash of empty/unknown user dict
                       filtered_kwargs.append(f"{k}=user_obj") 
                else:
                     filtered_kwargs.append(f"{k}={str(v)}")
                continue
            
            filtered_kwargs.append(f"{k}={str(v)}")
        
        # Construct the key payload
        # format: func_name:arg1:arg2:k=v
        payload = ":".join(key_parts + filtered_args + filtered_kwargs)
        return payload

    import time
    from functools import wraps

    def decorator(func):
        # Apply the cache decorator first
        cached_func = _fastapi_cache(expire=ttl, key_builder=specific_key_builder)(func)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await cached_func(*args, **kwargs)
                return result
            finally:
                end_time = time.time()
                elapsed = (end_time - start_time) * 1000
                print(f"[Performance] {func.__name__} took {elapsed:.2f}ms")
        
        return wrapper

    return decorator
