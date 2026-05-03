"""
api/routers/scraping.py
────────────────────────
GET /api/dashboard/stats/scraping
"""

import traceback

from fastapi import APIRouter, Query

from api.db import get_service, DashboardService

router = APIRouter()


@router.get("/stats/scraping")
async def get_scraping_stats(
    time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
    profile_limit: int = Query(10, ge=1),
):
    """Scraping Analytics tab: platform/profile breakdown, success rates, incomplete posts."""
    try:
        svc = get_service()
        return {
            "byPlatform":        svc.get_by_platform(time_range),
            "byProfile":         svc.get_by_profile(time_range, limit=profile_limit),
            "recentActivity":    svc.get_recent_activity(),
            "profileSuccessRate": svc.get_profile_success_rate(time_range, limit=profile_limit),
            "incompletePosts":   svc.get_incomplete_posts(time_range),
        }
    except Exception as e:
        print(f"Error in get_scraping_stats: {e}")
        traceback.print_exc()
        return {
            "byPlatform": {},
            "byProfile": {},
            "recentActivity": [],
            "profileSuccessRate": {},
            "incompletePosts": {"text_only": [], "image_only": [], "video_only": [], "total": 0},
        }
