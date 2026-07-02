# CCVA Public Module - Deployment Guide

## Overview

The CCVA Public Module is now a **standalone, deployable module** that can be:
1. **Deployed independently** as a separate service
2. **Integrated** into the main application
3. **Enabled/disabled** via configuration

## Quick Start

### Standalone Deployment

```bash
# Option 1: Using the runner script
python run_ccva_public.py

# Option 2: Using uvicorn directly
uvicorn app.ccva_public_module.app:app --host 0.0.0.0 --port 8001

# Option 3: Using environment variables
export CCVA_PUBLIC_ENABLED=true
export CCVA_PUBLIC_CLEANUP_ENABLED=true
uvicorn app.ccva_public_module.app:app --host 0.0.0.0 --port 8001
```

### Integrated Deployment

The module is automatically included in the main app if `CCVA_PUBLIC_ENABLED=true` (default).

To disable:
```bash
export CCVA_PUBLIC_ENABLED=false
```

## Configuration

All settings are configurable via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CCVA_PUBLIC_ENABLED` | `true` | Enable/disable the module |
| `CCVA_PUBLIC_PREFIX` | `/ccva_public` | API prefix |
| `CCVA_PUBLIC_API_PREFIX` | `/vman/api/v1/ccva_public` | Full API prefix |
| `CCVA_PUBLIC_TTL_HOURS` | `24` | TTL in hours |
| `CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS` | `6` | Cleanup job interval |
| `CCVA_PUBLIC_CLEANUP_ENABLED` | `true` | Enable/disable cleanup job |
| `CCVA_PUBLIC_COLLECTION` | `ccva_public_results` | Database collection name |

## Docker Deployment

### Standalone Service

Create `docker-compose.ccva-public.yml`:

```yaml
version: '3.8'

services:
  ccva-public:
    build: .
    ports:
      - "8001:8000"
    environment:
      - CCVA_PUBLIC_ENABLED=true
      - CCVA_PUBLIC_CLEANUP_ENABLED=true
      - CCVA_PUBLIC_TTL_HOURS=24
      - CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS=6
      - DATABASE_URL=${DATABASE_URL}
      - CORS_ALLOWED_ORIGINS=${CORS_ALLOWED_ORIGINS}
    command: uvicorn app.ccva_public_module.app:app --host 0.0.0.0 --port 8000
    restart: unless-stopped
```

Run:
```bash
docker-compose -f docker-compose.ccva-public.yml up -d
```

### Integrated with Main App

The module is included by default. To exclude it, set:
```yaml
environment:
  - CCVA_PUBLIC_ENABLED=false
```

## Module Structure

```
app/ccva_public_module/
├── __init__.py          # Module initialization
├── config.py            # Configuration (environment variables)
├── app.py               # Standalone FastAPI application
├── routes.py            # Route definitions
├── scheduler.py         # Cleanup scheduler
└── README.md           # Module documentation
```

## API Endpoints

When deployed standalone:
- `POST /vman/api/v1/ccva_public/upload` - Upload CSV and run CCVA
- `GET /vman/api/v1/ccva_public/{task_id}` - Get results
- `DELETE /vman/api/v1/ccva_public/task/{task_id}` - Delete results
- `GET /vman/api/v1/ccva_public/health` - Health check
- `GET /vman/api/v1/ccva_public/docs` - API documentation

## Benefits of Modular Design

1. **Independent Deployment**: Deploy CCVA Public as a separate service
2. **Scalability**: Scale CCVA Public independently from main app
3. **Isolation**: Issues in CCVA Public don't affect main app
4. **Flexibility**: Enable/disable without code changes
5. **Maintainability**: Clear separation of concerns

## Migration Notes

- Existing functionality remains unchanged
- Module is enabled by default (backward compatible)
- Can be disabled via `CCVA_PUBLIC_ENABLED=false`
- All existing endpoints work the same way

---

## ODK Data Sync — Celery Worker Operations

### Background

The sync pipeline uses Celery (broker: Redis) to run ODK data downloads in the background. Each sync run is assigned a unique UUID as its `task_id`, which is stored in Redis under the key `sync:active_task_id`. The following Redis keys are associated with each active sync:

| Key | Purpose |
|-----|---------|
| `sync:active_task_id` | Points to the UUID of the currently active sync |
| `sync:snapshot:{task_id}` | Live progress snapshot; present only while sync is running |
| `sync:cancel:{task_id}` | Cooperative cancel flag; set by the cancel endpoint |
| `sync:celery_task_id:{task_id}` | Maps vman task_id → Celery worker UUID |

All keys carry a 1-hour TTL and are cleared automatically on completion or cancellation.

### Restarting the Celery Worker (e.g. after changing the ODK URL)

**Option 1 — API is reachable (preferred):**

```bash
# 1. Clear stale Redis sync state via the reset endpoint
curl -X POST https://your-api/odk/reset-sync-state \
  -H "Authorization: Bearer <token>"

# 2. Restart only the Celery worker — Redis, ArangoDB, and frontend are unaffected
docker compose restart celery-worker

# 3. Start a new sync normally from the UI
```

**Option 2 — API is unreachable (worker completely dead):**

```bash
# List all stale sync keys (to see what's stuck)
docker exec redis-vman3-dev redis-cli -a vman@1029 --no-auth-warning KEYS "sync:*"

# Delete all of them at once
docker exec redis-vman3-dev sh -c \
  'redis-cli -a vman@1029 --no-auth-warning KEYS "sync:*" | xargs redis-cli -a vman@1029 --no-auth-warning DEL'

docker compose restart celery-worker
```

### Diagnosing a stuck sync

```bash
# Check if the Celery worker container is running
docker compose ps celery-worker

# Tail Celery worker logs for errors
docker compose logs celery-worker --tail=60

# Inspect current sync state in Redis
docker exec redis-vman3-dev redis-cli -a vman@1029 GET sync:active_task_id
# → prints the active task UUID, or (nil) if no sync is registered

# Check if snapshot (running indicator) exists for that task
docker exec redis-vman3-dev redis-cli -a vman@1029 EXISTS sync:snapshot:<task_id>
# → 1 = task is (or appears) running; 0 = task has finished or never started
```

### Notes

- You do **not** need to restart Redis, ArangoDB, or the frontend to recover a stuck sync.
- The `POST /odk/reset-sync-state` endpoint requires `ODK_DATA_SYNC` privilege.
- If a previous cancel left `sync:cancel:{task_id}` set and the worker was restarted before the task could clear it, the stale cancel flag would cause any new task using that same ID to self-cancel immediately. The UUID-per-sync design eliminates this entirely — each run gets a fresh ID.

