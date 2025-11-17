#!/usr/bin/env python3
"""
Standalone runner for CCVA Public Module
Usage: python run_ccva_public.py
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.ccva_public_module.app:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )

