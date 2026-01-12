"""
Celery Application Configuration

This module configures Celery with Redis as the message broker for:
- Background task processing (CCVA, ODK sync)
- Task result storage
- Task priority routing
"""

import os
from celery import Celery
from decouple import config

# Redis configuration - Read from environment
REDIS_URL = config('redis://redis:6370', default='redis://redis:6370')
REDIS_PASSWORD = config('REDIS_PASSWORD', default='vman@1029')

# Build broker URL with password
if REDIS_PASSWORD:
    if '://' in REDIS_URL:
        protocol, rest = REDIS_URL.split('://', 1)
        BROKER_URL = f"{protocol}://:{REDIS_PASSWORD}@{rest}"
    else:
        BROKER_URL = REDIS_URL
else:
    BROKER_URL = REDIS_URL

# Debug: Print the broker URL (password will be masked in logs)
print(f"🔧 Celery Broker URL: {BROKER_URL.replace(REDIS_PASSWORD or '', '***')}")
print(f"🔧 Creating Celery app with broker: {BROKER_URL[:30]}...")

# Create Celery app
celery_app = Celery(
    'vman3',
    broker=BROKER_URL,
    backend=BROKER_URL,  # Use Redis for result backend too
    include=[
        'app.tasks.ccva_tasks',
        'app.tasks.odk_tasks',
    ]
)

print(f"✅ Celery app created successfully!")

# Celery configuration
celery_app.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='UTC',
    enable_utc=True,
    
    # Task settings
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max per task
    task_soft_time_limit=3300,  # Soft limit at 55 minutes
    
    # Result settings
    result_expires=86400,  # Results expire after 24 hours
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Fair task distribution
    worker_concurrency=2,  # Default 2 concurrent tasks
    
    # Retry settings
    task_default_retry_delay=60,  # 1 minute delay between retries
    task_max_retries=3,  # Max 3 retries
    
    # Task routing - CCVA has higher priority
    task_routes={
        'app.tasks.ccva_tasks.*': {'queue': 'ccva', 'priority': 10},
        'app.tasks.odk_tasks.*': {'queue': 'odk', 'priority': 5},
    },
    
    # Queue definitions
    task_queues={
        'celery': {'exchange': 'celery', 'routing_key': 'celery'},
        'ccva': {'exchange': 'ccva', 'routing_key': 'ccva.#'},
        'odk': {'exchange': 'odk', 'routing_key': 'odk.#'},
    },
    
    # Default queue
    task_default_queue='celery',
)

# Optional: Beat schedule for periodic tasks (can be extended later)
celery_app.conf.beat_schedule = {
    # Example: 'odk-auto-sync': {
    #     'task': 'app.tasks.odk_tasks.auto_sync_odk_data',
    #     'schedule': 3600.0,  # Every hour
    # },
}
