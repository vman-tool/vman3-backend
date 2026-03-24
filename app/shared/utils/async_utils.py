import asyncio
import inspect
from typing import Any, Callable, Union

def call_update_callback(update_callback: Callable, progress: Any):
    """
    Safely invoke update_callback whether it is sync or async.
    - If the result is awaitable, run it in the current loop (if any) or a new one.
    - If there IS a running loop (e.g. called from FastAPI/ensure_task), schedule it.
    - Otherwise, run it with asyncio.run().
    """
    if not update_callback:
        return
        
    result = update_callback(progress)
    if inspect.isawaitable(result):
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(result)
            else:
                asyncio.run(result)
        except RuntimeError:
            # No running loop
            asyncio.run(result)
