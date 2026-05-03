"""
api/routers/config.py
──────────────────────
GET /api/dashboard/stats/config
"""

import traceback

from fastapi import APIRouter

from api.db import get_service, DashboardService

router = APIRouter()


@router.get("/stats/config")
async def get_config_stats():
    """
    Configuration tab.
    Returns all documents from social_scraper.system_config.
    Falls back to built-in mock data when MongoDB is unavailable.
    """
    try:
        return get_service().get_system_config()
    except Exception as e:
        print(f"Error in get_config_stats: {e}")
        traceback.print_exc()
        return DashboardService._get_mock_config()
