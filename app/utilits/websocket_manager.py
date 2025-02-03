from typing import Dict, List, Optional
import asyncio

from fastapi import WebSocket, WebSocketDisconnect


connections_lock = asyncio.Lock()

class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, task_id: str, websocket: WebSocket):
        await websocket.accept()

        async with connections_lock:
            if task_id not in self.active_connections:
                self.active_connections[task_id] = []
            self.active_connections[task_id].append(websocket)

    async def disconnect(self, task_id: str, websocket: WebSocket):
        async with connections_lock:
            if task_id in self.active_connections:
                self.active_connections[task_id].remove(websocket)
                if not self.active_connections[task_id]:
                    del self.active_connections[task_id]

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, task_id: str, message: str):
        if task_id in self.active_connections:
            for connection in self.active_connections[task_id]:
                await connection.send_text(message)

    async def safe_receive_text(self, websocket: WebSocket, timeout: float = 60.0) -> Optional[str]:
        """
        Safely receive WebSocket text with timeout and error handling.
        Returns None if the connection is closed or errored.
        """
        try:
            receive_task = asyncio.create_task(websocket.receive_text())
            
            message = await asyncio.wait_for(receive_task, timeout=timeout)
            return message
            
        except asyncio.TimeoutError:
            print("WebSocket receive operation timed out")
            return TimeoutError("Websocket connection timed out.")
        except WebSocketDisconnect as e:
            print(f"WebSocket disconnected during receive: {e}")
            return e
        except Exception as e:
            print(f"Error during WebSocket receive: {str(e)}")
            raise e

# from typing import Dict, List

# import redis.asyncio as redis
# from fastapi import WebSocket


# class WebSocketManager:
#     def __init__(self, redis_url: str):
#         self.redis_url = redis_url
#         self.redis = None
#         self.local_connections: Dict[str, List[WebSocket]] = {}

#     async def connect(self, task_id: str, websocket: WebSocket):
#         # Accept the WebSocket connection
#         await websocket.accept()
        
#         # Add to local in-memory connections for the current worker
#         if task_id not in self.local_connections:
#             self.local_connections[task_id] = []
#         self.local_connections[task_id].append(websocket)

#         # Add the task_id to Redis to keep track of active tasks
#         await self.redis.sadd(f"active_task:{task_id}", str(websocket))

#     async def disconnect(self, task_id: str, websocket: WebSocket):
#         # Remove from local in-memory connections for the current worker
#         if task_id in self.local_connections:
#             self.local_connections[task_id].remove(websocket)
#             if not self.local_connections[task_id]:
#                 del self.local_connections[task_id]

#         # Remove from Redis active task set
#         await self.redis.srem(f"active_task:{task_id}", str(websocket))

#         # Clean up the task in Redis if no connections remain
#         if await self.redis.scard(f"active_task:{task_id}") == 0:
#             await self.redis.delete(f"active_task:{task_id}")

#     async def send_personal_message(self, message: str, websocket: WebSocket):
#         # Send a message directly to a single WebSocket connection
#         await websocket.send_text(message)

#     async def broadcast(self, task_id: str, message: str):
#         # Broadcast the message to all active WebSocket connections
#         if task_id in self.local_connections:
#             for connection in self.local_connections[task_id]:
#                 await connection.send_text(message)
        
#         # Publish the message to Redis for other workers to pick up
#         await self.redis.publish(f"broadcast_task:{task_id}", message)

#     async def listen_for_broadcasts(self, task_id: str):
#         # Subscribe to the Redis broadcast channel
#         pubsub = self.redis.pubsub()
#         await pubsub.subscribe(f"broadcast_task:{task_id}")
        
#         # Listen for messages from Redis Pub/Sub
#         async for message in pubsub.listen():
#             if message['type'] == 'message':
#                 # Send the message to all active WebSocket connections in the current worker
#                 if task_id in self.local_connections:
#                     for connection in self.local_connections[task_id]:
#                         await connection.send_text(message['data'].decode('utf-8'))

#     async def start(self):
#         # Create the Redis connection pool
#         self.redis = redis.from_url(self.redis_url)

#     async def stop(self):
#         # Close the Redis connection pool
#         await self.redis.close()