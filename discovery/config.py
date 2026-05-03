# discovery/config.py
# ─────────────────────────────────────────────────────────────
# Central configuration for the geo-based discovery layer.
# Override any value via environment variables.
# ─────────────────────────────────────────────────────────────

import os
from dataclasses import dataclass, field
from typing import List


# ── Apify ────────────────────────────────────────────────────
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")

# Actor IDs (stable Apify store slugs)
ACTOR_INSTAGRAM_HASHTAG  = "apify/instagram-hashtag-scraper"
# ACTOR_INSTAGRAM_SEARCH   = "apify/instagram-search-scraper"
ACTOR_TIKTOK_HASHTAG     = "clockworks/tiktok-hashtag-scraper"
# ACTOR_TIKTOK_SEARCH      = "apidojo/tiktok-scraper"
# ACTOR_TIKTOK_HASHTAG    = "clockworks/tiktok-hashtag-scraper"
ACTOR_FACEBOOK_SEARCH   = "apify/facebook-search-scraper"

# ── OpenAI ───────────────────────────────────────────────────
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
GPT_MODEL       = "gpt-4o"
GPT_TEMPERATURE = 0.1          # low → consistent JSON output
GPT_MAX_TOKENS  = 512

# ── MongoDB ──────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = "social_scraper"
MONGO_COLLECTION = "scrape_targets"

# ── Discovery thresholds ─────────────────────────────────────
MIN_SCORE_TO_STORE   = 65      # GPT-4o score (0-100) to accept a profile
MAX_RESULTS_PER_TAG  = 50      # Apify resultsLimit per hashtag call
TEST_LIMIT           = None    # set to int (e.g. 10) to cap profiles for testing
DEDUP_HOURS          = 168     # re-discover same profile only after 7 days

# ── Scoring weights (for transparency / tuning) ──────────────
WEIGHT_LOCATION  = 0.45
WEIGHT_BUSINESS  = 0.35
WEIGHT_ACTIVITY  = 0.20


# ─────────────────────────────────────────────────────────────
#  Region definitions — add new regions freely
# ─────────────────────────────────────────────────────────────
@dataclass
class Region:
    name: str                          # human label
    country_code: str                  # ISO-2 for TikTok actor
    country_names: List[str] = field(default_factory=list)   # names to look for in content
    cities: List[str] = field(default_factory=list)          # city names to look for in content
    hashtags: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    city_hashtags: List[str] = field(default_factory=list)


REGIONS: dict[str, Region] = {
    "yemen": Region(
        name="Yemen",
        country_code="YE",
        country_names=["اليمن", "يمن", "Yemen"],
        cities=[
            "صنعاء", "عدن", "تعز", "الحديدة", "المكلا",
            "إب", "حضرموت", "ذمار", "سيئون", "عمران",
        ],
        hashtags=[
            "تخفيضات_اليمن"
        ],
        keywords=[
            "متجر", "محل", "عروض", "تخفيضات", "خصم",
            "للبيع", "بيع", "تسوق", "اليمن", "صنعاء",
            "عدن", "متاجر", "أسعار", "توصيل",
        ],
        city_hashtags=[
            "صنعاء", "عدن", "تعز", "الحديدة",
        ],
    ),




# REGIONS: dict[str, Region] = {
#     "yemen": Region(
#         name="Yemen",
#         country_code="YE",
#         country_names=["اليمن", "يمن", "Yemen"],
#         cities=[
#             "صنعاء", "عدن", "تعز", "الحديدة", "المكلا",
#             "إب", "حضرموت", "ذمار", "سيئون", "عمران",
#         ],
#         hashtags=[
#             # ── national geo-signals ───────────────────────
#             "يمن", "اليمن", "yemen",
#             # ── city-level ────────────────────────────────
#             "صنعاء", "عدن", "تعز", "الحديدة", "إب", "ذمار",
#             "سيئون", "حضرموت", "المكلا",
#             # ── commercial offers ─────────────────────────
#             "عروض_اليمن", "تخفيضات_اليمن", "متاجر_اليمن",
#             "عروض_صنعاء", "محلات_اليمن", "بيع_اليمن",
#             "سوق_اليمن", "عروض_عدن", "تخفيضات_صنعاء",
#         ],
#         keywords=[
#             "متجر", "محل", "عروض", "تخفيضات", "خصم",
#             "للبيع", "بيع", "تسوق", "اليمن", "صنعاء",
#             "عدن", "متاجر", "أسعار", "توصيل",
#         ],
#         city_hashtags=[
#             "صنعاء", "عدن", "تعز", "الحديدة",
#         ],
#     ),
#

    # ── template for future regions ───────────────────────────
    # "egypt": Region(
    #     name="Egypt",
    #     country_code="EG",
    #     hashtags=["مصر", "القاهرة", "عروض_مصر", ...],
    #     keywords=["متجر", "عروض", "خصم", ...],
    # ),
}