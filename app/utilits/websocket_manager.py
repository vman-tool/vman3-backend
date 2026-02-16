"""
WebSocket Manager with Redis Pub/Sub

This module provides WebSocket management that works across multiple Gunicorn workers
by using Redis Pub/Sub for cross-process message broadcasting.

Architecture:
- Each worker maintains its own local WebSocket connections
- Messages are published to Redis Pub/Sub channels
- Each worker subscribes to relevant channels and forwards to local connections
"""

from typing import Dict, List, Optional
import asyncio
import json
from decouple import config

from fastapi import WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis


connections_lock = asyncio.Lock()


class WebSocketManager:
    """
    WebSocket Manager with Redis Pub/Sub for multi-worker support.
    
    This replaces the in-memory-only manager that couldn't broadcast
    across Gunicorn workers.
    """
    
    def __init__(self, redis_url: str = None):
        """
        Initialize WebSocket Manager.
        
        Args:
            redis_url: Redis URL for Pub/Sub. If None, uses REDIS_URL env var.
        """
        self.redis_url = redis_url or config('REDIS_URL', default='redis://localhost:6370')
        self.redis_password = config('REDIS_PASSWORD', default=None)
        
        # Local connections for this worker
        self.local_connections: Dict[str, List[WebSocket]] = {}
        
        # Redis client for publishing
        self._redis: Optional[aioredis.Redis] = None
        
        # Pubsub listener task
        self._pubsub_task: Optional[asyncio.Task] = None
        
        # Track subscribed channels
        self._subscribed_channels: set = set()
        
        # Running flag
        self._running = False
    
    async def _get_redis(self) -> aioredis.Redis:
        """Get or create Redis connection."""
        if self._redis is None:
            self._redis = aioredis.from_url(
                self.redis_url,
                password=self.redis_password,
                decode_responses=True
            )
        return self._redis
    
    async def start(self):
        """Start the WebSocket manager and Redis Pub/Sub listener."""
        if self._running:
            return
        
        self._running = True
        print("🔌 WebSocket Manager started with Redis Pub/Sub")
    
    async def stop(self):
        """Stop the WebSocket manager and cleanup resources."""
        self._running = False
        
        # Cancel pubsub task if running
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
        
        # Close Redis connection
        if self._redis:
            await self._redis.close()
            self._redis = None
        
        print("🔌 WebSocket Manager stopped")
    
    async def connect(self, task_id: str, websocket: WebSocket):
        """
        Accept a WebSocket connection and register it for a task.
        
        Args:
            task_id: Identifier for the task/channel
            websocket: WebSocket connection to register
        """
        await websocket.accept()
        
        async with connections_lock:
            if task_id not in self.local_connections:
                self.local_connections[task_id] = []
            self.local_connections[task_id].append(websocket)
        
        # Start listening for this channel if not already
        channel = f"ws:broadcast:{task_id}"
        if channel not in self._subscribed_channels:
            self._subscribed_channels.add(channel)
            # Start/update pubsub listener
            await self._ensure_pubsub_listener()
    
    async def disconnect(self, task_id: str, websocket: WebSocket):
        """
        Remove a WebSocket connection from a task.
        
        Args:
            task_id: Identifier for the task/channel
            websocket: WebSocket connection to remove
        """
        async with connections_lock:
            if task_id in self.local_connections:
                try:
                    self.local_connections[task_id].remove(websocket)
                except ValueError:
                    pass  # Already removed
                    
                if not self.local_connections[task_id]:
                    del self.local_connections[task_id]
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        """
        Send a message to a specific WebSocket connection.
        
        Args:
            message: Message to send
            websocket: Target WebSocket connection
        """
        try:
            await websocket.send_text(message)
        except Exception as e:
            print(f"Failed to send personal message: {e}")
    
    async def broadcast(self, task_id: str, message: str):
        """
        Broadcast a message to all connections for a task across all workers.
        
        This publishes to Redis Pub/Sub so all workers receive the message.
        
        Args:
            task_id: Identifier for the task/channel
            message: Message to broadcast (string or dict)
        """
        # Ensure message is a string
        if isinstance(message, dict):
            message = json.dumps(message)
        
        # Publish to Redis for all workers
        try:
            redis_client = await self._get_redis()
            channel = f"ws:broadcast:{task_id}"
            await redis_client.publish(channel, message)
        except Exception as e:
            print(f"Redis publish error: {e}")
            # Fallback to local broadcast only
            await self._local_broadcast(task_id, message)
    
    async def _local_broadcast(self, task_id: str, message: str):
        """
        Broadcast message to local connections only.
        
        Args:
            task_id: Identifier for the task/channel
            message: Message to broadcast
        """
        if task_id in self.local_connections:
            dead_connections = []
            
            for connection in self.local_connections[task_id]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    print(f"Failed to send to connection: {e}")
                    dead_connections.append(connection)
            
            # Clean up dead connections
            for conn in dead_connections:
                try:
                    self.local_connections[task_id].remove(conn)
                except ValueError:
                    pass
    
    async def _ensure_pubsub_listener(self):
        """Ensure Pub/Sub listener is running."""
        if self._pubsub_task is None or self._pubsub_task.done():
            self._pubsub_task = asyncio.create_task(self._pubsub_listener())
    
    async def _pubsub_listener(self):
        """
        Background task to listen for Redis Pub/Sub messages
        and forward them to local WebSocket connections.
        """
        while self._running:
            try:
                redis_client = await self._get_redis()
                pubsub = redis_client.pubsub()
                
                # Subscribe to pattern for all broadcast channels
                await pubsub.psubscribe("ws:broadcast:*")
                
                async for message in pubsub.listen():
                    if not self._running:
                        break
                        
                    if message['type'] == 'pmessage':
                        # Extract task_id from channel name
                        channel = message['channel']
                        if channel.startswith("ws:broadcast:"):
                            task_id = channel.replace("ws:broadcast:", "")
                            data = message['data']
                            
                            # Broadcast to local connections
                            await self._local_broadcast(task_id, data)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Pub/Sub listener error: {e}")
                await asyncio.sleep(1)  # Wait before reconnecting
    
    async def safe_receive_text(
        self, 
        websocket: WebSocket, 
        timeout: float = None
    ) -> Optional[str]:
        """
        Safely receive WebSocket text with timeout and error handling.
        
        Args:
            websocket: WebSocket connection to receive from
            timeout: Timeout in seconds (default from config)
            
        Returns:
            Received message or None/Exception on error
        """
        if timeout is None:
            timeout = config('REFRESH_TOKEN_EXPIRE_MINUTES', default=10, cast=float) * 60.0
        
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