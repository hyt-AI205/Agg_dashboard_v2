"""
api/routers/offers.py
──────────────────────
GET /api/dashboard/stats/offers
"""

import traceback

from fastapi import APIRouter, Query

from api.db import get_service, DashboardService

router = APIRouter()


@router.get("/stats/offers")
async def get_offers_stats(
    time_range: str = Query("24h", pattern="^(24h|7d|30d)$"),
):
    """Offer Intelligence tab: categories, brands, countries, discounts, promo codes."""
    try:
        svc = get_service()
        return {
            "offersByCategory":         svc.get_offers_by_category(time_range),
            "topBrands":                svc.get_top_brands(time_range, limit=50),
            "offersByCountry":          svc.get_offers_by_country(time_range),
            "discountTypesDistribution": svc.get_discount_types_distribution(time_range),
            "promoCodeUsage":           svc.get_promo_code_usage(time_range),
            "avgDiscountValue":         svc.get_average_discount_value(time_range),
            "offerTypeBreakdown":       svc.get_offer_type_breakdown(time_range),
        }
    except Exception as e:
        print(f"Error in get_offers_stats: {e}")
        traceback.print_exc()
        return {
            "offersByCategory": {},
            "topBrands": {},
            "offersByCountry": {},
            "discountTypesDistribution": {},
            "promoCodeUsage": {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0},
            "avgDiscountValue": {"overall": 0, "by_currency": {}},
            "offerTypeBreakdown": {},
        }
