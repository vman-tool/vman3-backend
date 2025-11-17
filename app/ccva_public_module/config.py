"""
Configuration for CCVA Public Module
Can be overridden via environment variables
"""
from decouple import config

# Module settings
CCVA_PUBLIC_ENABLED = config('CCVA_PUBLIC_ENABLED', default=True, cast=bool)
CCVA_PUBLIC_PREFIX = config('CCVA_PUBLIC_PREFIX', default='/ccva_public')
CCVA_PUBLIC_API_PREFIX = config('CCVA_PUBLIC_API_PREFIX', default='/vman/api/v1/ccva_public')

# TTL Settings
CCVA_PUBLIC_TTL_HOURS = config('CCVA_PUBLIC_TTL_HOURS', default=24, cast=int)

# Cleanup Settings
CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS = config('CCVA_PUBLIC_CLEANUP_INTERVAL_HOURS', default=6, cast=int)
CCVA_PUBLIC_CLEANUP_ENABLED = config('CCVA_PUBLIC_CLEANUP_ENABLED', default=True, cast=bool)

# Database Collection
CCVA_PUBLIC_COLLECTION = config('CCVA_PUBLIC_COLLECTION', default='ccva_public_results')

