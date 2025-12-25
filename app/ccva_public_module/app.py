"""
Standalone FastAPI application for CCVA Public Module
Can be run independently or integrated into main app
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from decouple import config

from app.ccva_public_module.config import (
    CCVA_PUBLIC_ENABLED,
    CCVA_PUBLIC_API_PREFIX,
    CCVA_PUBLIC_CLEANUP_ENABLED
)
from app.ccva_public_module.routes import create_ccva_public_router
from app.ccva_public_module.scheduler import initialize_ccva_public_scheduler, shutdown_ccva_public_scheduler
from app.shared.middlewares.error_handlers import register_error_handlers


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("🚀 Starting CCVA Public Module...")
    
    if CCVA_PUBLIC_CLEANUP_ENABLED:
        await initialize_ccva_public_scheduler()
        logger.info("✅ CCVA Public cleanup scheduler initialized")
    
    logger.info("✅ CCVA Public Module started successfully")
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down CCVA Public Module...")
    
    if CCVA_PUBLIC_CLEANUP_ENABLED:
        await shutdown_ccva_public_scheduler()
        logger.info("✅ CCVA Public cleanup scheduler shut down")
    
    logger.info("✅ CCVA Public Module shutdown complete")


def create_ccva_public_app() -> FastAPI:
    """
    Create standalone FastAPI application for CCVA Public Module
    
    Usage:
        # Standalone deployment
        uvicorn app.ccva_public_module.app:app --host 0.0.0.0 --port 8001
        
        # Or integrate into main app (see integration guide)
    """
    app = FastAPI(
        title="CCVA Public API",
        description="Public API for Community Cause of Death Analysis (CCVA)",
        version="1.0.0",
        docs_url=f"{CCVA_PUBLIC_API_PREFIX}/docs",
        openapi_url=f"{CCVA_PUBLIC_API_PREFIX}/openapi.json",
        lifespan=lifespan
    )
    
    # Register routes
    # The CCVA public router already has prefix="/ccva_public"
    # We add the API prefix to match the main app structure
    router = create_ccva_public_router()
    api_prefix = CCVA_PUBLIC_API_PREFIX.replace('/ccva_public', '')  # Get /vman/api/v1
    app.include_router(router, prefix=api_prefix)
    
    # Register error handlers
    register_error_handlers(app)
    
    # CORS middleware
    origins = config('CORS_ALLOWED_ORIGINS', default="*").split(',')
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Health check
    @app.get("/health")
    @app.get(f"{CCVA_PUBLIC_API_PREFIX}/health")
    async def health_check():
        return {
            "status": "healthy",
            "module": "ccva_public",
            "version": "1.0.0",
            "cleanup_enabled": CCVA_PUBLIC_CLEANUP_ENABLED
        }
    
    return app


# Create app instance for standalone deployment
app = create_ccva_public_app()

