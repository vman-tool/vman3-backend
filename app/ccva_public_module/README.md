# CCVA Public Module

Standalone deployable module for public CCVA (Community Cause of Death Analysis) functionality.

## Features

- ✅ Privacy-first: Automatic deletion after completion
- ✅ TTL-based cleanup: 24-hour maximum retention
- ✅ Standalone deployment: Can run independently
- ✅ Modular integration: Can be integrated into main app
- ✅ Configurable: Environment variable based configuration

## Deployment Options

### Option 1: Standalone Deployment

Deploy as a separate service:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export CCVA_PUBLIC_ENABLED=true
export CCVA_PUBLIC_CLEANUP_ENABLED=true
export CCVA_PUBLIC_TTL_HOURS=24
export CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS=6

# Run standalone
uvicorn app.ccva_public_module.app:app --host 0.0.0.0 --port 8001
```

### Option 2: Integrated Deployment

Integrate into main application (see integration guide below).

## Configuration

Environment variables:

- `CCVA_PUBLIC_ENABLED` (default: `true`) - Enable/disable the module
- `CCVA_PUBLIC_PREFIX` (default: `/ccva_public`) - API prefix
- `CCVA_PUBLIC_API_PREFIX` (default: `/vman/api/v1/ccva_public`) - Full API prefix
- `CCVA_PUBLIC_TTL_HOURS` (default: `24`) - TTL in hours
- `CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS` (default: `6`) - Cleanup job interval
- `CCVA_PUBLIC_CLEANUP_ENABLED` (default: `true`) - Enable/disable cleanup job
- `CCVA_PUBLIC_COLLECTION` (default: `ccva_public_results`) - Database collection name

## API Endpoints

- `POST /ccva_public/upload` - Upload CSV and run CCVA analysis
- `GET /ccva_public/{task_id}` - Get results by task ID
- `DELETE /ccva_public/task/{task_id}` - Delete results by task ID
- `GET /ccva_public/health` - Health check

## Integration into Main App

To integrate into the main application:

1. Update `app/routes.py`:

```python
from app.ccva_public_module.config import CCVA_PUBLIC_ENABLED
from app.ccva_public_module.routes import create_ccva_public_router

def create_main_router():
    main_router = APIRouter(prefix="/vman/api/v1")
    
    # ... other routers ...
    
    # Conditionally include CCVA Public module
    if CCVA_PUBLIC_ENABLED:
        ccva_public_router = create_ccva_public_router()
        main_router.include_router(ccva_public_router)
    
    return main_router
```

2. Update `app/utilits/schedeular.py`:

```python
from app.ccva_public_module.config import CCVA_PUBLIC_CLEANUP_ENABLED
from app.ccva_public_module.scheduler import initialize_ccva_public_scheduler

async def start_scheduler():
    # ... existing code ...
    
    # Conditionally initialize CCVA Public scheduler
    if CCVA_PUBLIC_CLEANUP_ENABLED:
        await initialize_ccva_public_scheduler()
```

## Docker Deployment

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
```

Run with:
```bash
docker-compose -f docker-compose.ccva-public.yml up -d
```

## Architecture

```
app/ccva_public_module/
├── __init__.py          # Module initialization
├── config.py            # Configuration (environment variables)
├── app.py               # Standalone FastAPI application
├── routes.py            # Route definitions
├── scheduler.py         # Cleanup scheduler
└── README.md           # This file
```

## Dependencies

- FastAPI
- APScheduler (for cleanup jobs)
- ArangoDB (for data storage)
- Existing CCVA services (shared)

## Privacy Features

1. **Immediate Deletion**: Frontend automatically deletes from server after saving to IndexedDB
2. **TTL Backup**: 24-hour TTL ensures cleanup even if frontend deletion fails
3. **Automatic Cleanup**: Cron job runs every 6 hours to remove expired records
4. **Single Collection**: All data in one collection for easy management

