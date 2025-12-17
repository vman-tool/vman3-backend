
from typing import Dict, Any, Optional
import time
from datetime import datetime

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections
from app.utilits.logger import app_logger

class TaskProgressService:
    @staticmethod
    def _ensure_collection_sync(db: StandardDatabase):
        """Ensure the collection and TTL index exist (Synchronous for run_in_threadpool)"""
        if not db.has_collection(db_collections.TASK_PROGRESS):
            db.create_collection(db_collections.TASK_PROGRESS)
            # Index creation is handled by the main app initialization or constants config, 
            # but we can ensure it here safely.
            db.collection(db_collections.TASK_PROGRESS).add_ttl_index(fields=["expires_at"], expire_after=0, name="idx_expires_at")

    @classmethod
    async def ensure_collection(cls, db: StandardDatabase):
        await run_in_threadpool(cls._ensure_collection_sync, db)

    @staticmethod
    def _save_progress_sync(db: StandardDatabase, task_id: str, data: Dict[str, Any], ttl_seconds: int = 86400):
        """Save progress to DB (Synchronous)"""
        collection = db.collection(db_collections.TASK_PROGRESS)
        
        # Prepare document
        doc = data.copy()
        doc['_key'] = task_id
        doc['task_id'] = task_id
        doc['timestamp'] = datetime.utcnow().isoformat()
        # Set exact expiration time (Unix timestamp)
        doc['expires_at'] = int(time.time()) + ttl_seconds
        
        collection.insert(doc, overwrite=True, overwrite_mode="update")
        return doc

    @classmethod
    async def save_progress(cls, db: StandardDatabase, task_id: str, data: Dict[str, Any], ttl_seconds: int = 86400):
        """Asynchronously save progress to DB"""
        return await run_in_threadpool(cls._save_progress_sync, db, task_id, data, ttl_seconds)

    @staticmethod
    def _get_progress_sync(db: StandardDatabase, task_id: str) -> Optional[Dict[str, Any]]:
        """Get progress from DB (Synchronous)"""
        if not db.has_collection(db_collections.TASK_PROGRESS):
            return None
        
        collection = db.collection(db_collections.TASK_PROGRESS)
        if collection.has(task_id):
            return collection.get(task_id)
        return None

    @classmethod
    async def get_progress(cls, db: StandardDatabase, task_id: str) -> Optional[Dict[str, Any]]:
        """Asynchronously get progress from DB"""
        return await run_in_threadpool(cls._get_progress_sync, db, task_id)
