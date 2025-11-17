"""
Routes for CCVA Public Module
"""
from fastapi import APIRouter

from app.ccva import ccva_public_routes
from app.ccva_public_module.config import CCVA_PUBLIC_API_PREFIX


def create_ccva_public_router() -> APIRouter:
    """
    Create router for CCVA Public Module
    This can be used standalone or integrated into main app
    """
    router = APIRouter()
    
    # Include the existing CCVA public routes
    # The routes already have prefix="/ccva_public"
    # For standalone: use as-is
    # For integrated: main app will add /vman/api/v1 prefix
    router.include_router(ccva_public_routes.ccva_public_router)
    
    return router

