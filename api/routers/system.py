"""
api/routers/system.py
──────────────────────
GET /api/dashboard/health
GET /api/dashboard/database-stats
GET /api/dashboard/max-profiles
"""

import traceback
from datetime import datetime

import pymongo
from fastapi import APIRouter

from api.db import get_service

router = APIRouter()


@router.get("/health")
async def get_system_health():
    """System health check: MongoDB connectivity, collection presence, last sync."""
    try:
        svc = get_service()
        if not svc.connected:
            return {
                "mongodb":     {"status": "disconnected", "healthy": False},
                "collections": {"status": "unavailable",  "healthy": False},
                "lastSync":    None,
                "timestamp":   datetime.utcnow().isoformat(),
                "mode":        "mock_data",
            }

        svc.client.server_info()
        collections    = svc.social_scraper_db.list_collection_names()
        has_collections = "raw_social_data" in collections
        last_post      = svc.raw_data_collection.find_one(sort=[("scraped_at", pymongo.DESCENDING)])

        return {
            "mongodb":     {"status": "connected", "healthy": True},
            "collections": {"status": "ready" if has_collections else "missing", "healthy": has_collections},
            "lastSync":    last_post["scraped_at"].isoformat() if last_post else None,
            "timestamp":   datetime.utcnow().isoformat(),
            "mode":        "live_data",
        }
    except Exception as e:
        print(f"Health check error: {e}")
        return {
            "mongodb":     {"status": "error", "healthy": False},
            "collections": {"status": "error", "healthy": False},
            "lastSync":    None,
            "timestamp":   datetime.utcnow().isoformat(),
            "mode":        "error",
            "error":       str(e),
        }


@router.get("/database-stats")
async def get_database_statistics():
    """MongoDB collection sizes and document counts."""
    try:
        return get_service().get_database_stats()
    except Exception as e:
        print(f"Error in get_database_statistics: {e}")
        return {
            "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
            "offers":          {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"},
        }


@router.get("/max-profiles")
async def get_max_profiles():
    """Total profile count — used by the frontend to set dynamic limits."""
    try:
        return {"maxProfiles": get_service().get_total_profiles_count()}
    except Exception as e:
        print(f"Error in get_max_profiles: {e}")
        return {"maxProfiles": 50}
