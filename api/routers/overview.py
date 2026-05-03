"""
api/routers/overview.py
────────────────────────
GET /api/dashboard/stats/overview
"""

import traceback

from fastapi import APIRouter, Query

from api.db import get_service, DashboardService

router = APIRouter()


@router.get("/stats/overview")
async def get_overview_stats(
    time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
    profile_limit: int = Query(10, ge=1, le=200),
):
    """High-level overview: totals, alerts, AI metrics, profile performance."""
    try:
        svc = get_service()
        stats              = svc.get_stats(time_range)
        recent_activity    = svc.get_recent_activity()
        profile_perf       = svc.get_profile_performance(time_range, limit=profile_limit)
        total_profiles     = svc.get_total_profiles_in_range(time_range)
        failed_scrapes     = svc.get_failed_scrapes_count(time_range)
        inactive_offers    = svc.get_inactive_offers_count(time_range)
        stale_profiles     = svc.get_stale_profiles(hours=24)
        ai_metrics         = svc.get_ai_extraction_metrics(time_range)

        return {
            **stats,
            "recentActivity":    recent_activity,
            "profilePerformance": profile_perf,
            "totalProfiles":     total_profiles,
            "alerts": {
                "failedScrapesCount":   failed_scrapes["total"],
                "inactiveOffersCount":  inactive_offers["total"],
                "staleProfilesCount":   stale_profiles["count"],
            },
            "aiMetrics": ai_metrics,
        }
    except Exception as e:
        print(f"Error in get_overview_stats: {e}")
        traceback.print_exc()
        return DashboardService().get_mock_stats()
