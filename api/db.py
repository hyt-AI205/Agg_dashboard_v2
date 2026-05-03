"""
api/db.py
─────────
Shared MongoDB connection and DashboardService.
All routers import `get_service()` to access the singleton.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pymongo
from dotenv import load_dotenv

load_dotenv()


# ── singleton ────────────────────────────────────────────────────────────────
_service: "DashboardService | None" = None


def get_service() -> "DashboardService":
    global _service
    if _service is None:
        _service = DashboardService()
    return _service


# ── service ──────────────────────────────────────────────────────────────────
class DashboardService:
    def __init__(self, mongo_uri: str | None = None):
        self.connected = False
        try:
            uri = mongo_uri or os.getenv("MONGO_URI")
            if not uri:
                raise ValueError("MONGO_URI is not set — add it to your .env file")

            self.client = pymongo.MongoClient(
                uri,
                serverSelectionTimeoutMS=2000,
                connectTimeoutMS=2000,
                socketTimeoutMS=2000,
            )
            self.client.server_info()

            self.social_scraper_db = self.client["social_scraper"]
            self.offer_insights_db = self.client["offer_insights"]

            self.raw_data_collection      = self.social_scraper_db["raw_social_data"]
            self.offers_collection        = self.offer_insights_db["offers"]
            self.targets_collection       = self.social_scraper_db["scrape_targets"]
            self.system_config_collection = self.social_scraper_db["system_config"]

            self.connected = True
            print("✓ Dashboard MongoDB connection successful")

        except Exception as e:
            print(f"✗ Dashboard MongoDB connection failed: {e}")
            print("  Dashboard will use mock data")
            self.connected = False

    # ── helpers ──────────────────────────────────────────────────────────────

    def get_time_filter(self, time_range: str) -> dict:
        now = datetime.utcnow()
        delta = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}
        start = now - delta.get(time_range, timedelta(hours=24))
        return {"scraped_at": {"$gte": start}}

    @staticmethod
    def _format_bytes(b: float) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if b < 1024.0:
                return f"{b:.2f} {unit}"
            b /= 1024.0
        return f"{b:.2f} TB"

    # ── mock data ─────────────────────────────────────────────────────────────

    def get_mock_stats(self) -> dict:
        return {
            "totalPosts": 156,
            "totalOffers": 89,
            "activeProfiles": 12,
            "lastScraped": datetime.utcnow().isoformat(),
            "successRate": 57.05,
            "byPlatform": {"facebook": 89, "instagram": 45, "tiktok": 22},
            "byProfile": {
                "promoofficiel": 45, "dealsalgeria": 32,
                "promodz": 28, "bestoffers_dz": 25, "others": 26,
            },
            "offersByCategory": {"shoes": 34, "fashion": 28, "electronics": 15, "beauty": 12},
            "recentActivity": [
                {"time": "2m ago",  "profile": "promoofficiel",  "platform": "facebook",  "posts": 3, "status": "success"},
                {"time": "15m ago", "profile": "dealsalgeria",   "platform": "instagram", "posts": 5, "status": "success"},
                {"time": "1h ago",  "profile": "promodz",        "platform": "facebook",  "posts": 2, "status": "success"},
                {"time": "2h ago",  "profile": "bestoffers_dz",  "platform": "tiktok",    "posts": 0, "status": "warning"},
            ],
        }

    @staticmethod
    def _get_mock_config() -> list[dict]:
        now = datetime.utcnow().isoformat()
        return [
            {
                "id": "mock-scraper-config",
                "type": "scraper_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "global_settings": {"default_results_limit": 20},
                    "providers": {
                        "apify": {
                            "platforms": {
                                "tiktok":    {"actor_id": "clockworks/tiktok-profile-scraper",    "url_template": "https://www.tiktok.com/@{username}",    "results_limit": 5},
                                "instagram": {"actor_id": "apify/instagram-profile-scraper",      "url_template": "https://www.instagram.com/{username}/", "results_limit": 5},
                                "facebook":  {"actor_id": "apify/facebook-posts-scraper",         "url_template": "https://www.facebook.com/{username}/",  "results_limit": 5},
                            }
                        },
                        "brightdata": {
                            "platforms": {
                                "tiktok":    {"dataset_id": "gd_l1villgoiiidt09ci", "url_template": "https://www.tiktok.com/@{username}",    "results_limit": 15},
                                "instagram": {"dataset_id": "gd_l1vikfch901nx3by4", "url_template": "https://www.instagram.com/{username}/", "results_limit": 20},
                                "facebook":  {"dataset_id": "gd_lkaxegm826bjpoo9m5","url_template": "https://www.facebook.com/{username}/",  "results_limit": 25},
                            }
                        },
                    },
                },
            },
            {
                "id": "mock-schedule-config",
                "type": "schedule_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "global_scrape_interval_hours": 6,
                    "per_platform_overrides": {"tiktok": 6, "instagram": 6, "facebook": 6},
                },
            },
            {
                "id": "mock-llm-config",
                "type": "llm_config",
                "version": 1,
                "is_active": True,
                "updated_at": now,
                "data": {
                    "default_provider": "openai",
                    "providers": {
                        "openai": {
                            "model": "gpt-4o",
                            "generation_config": {"temperature": 0.2, "top_p": 1, "max_output_tokens": 800},
                            "normalizer": {
                                "model": "gpt-4o-mini",
                                "generation_config": {"temperature": 0.1, "top_p": 1, "max_output_tokens": 600},
                            },
                        },
                        "groq": {
                            "model": "llama-3.3-70b-versatile",
                            "generation_config": {"temperature": 0.2, "top_p": 1, "max_output_tokens": 700},
                            "normalizer": {
                                "model": "llama-3.3-70b-versatile",
                                "generation_config": {"temperature": 0.1, "top_p": 1, "max_output_tokens": 800},
                            },
                        },
                    },
                },
            },
        ]

    # ── overview ──────────────────────────────────────────────────────────────

    def get_stats(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return self.get_mock_stats()
        try:
            tf = self.get_time_filter(time_range)
            total_posts  = self.raw_data_collection.count_documents(tf)
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            total_offers = self.offers_collection.count_documents(offer_filter)
            active_profiles = self.targets_collection.count_documents({"active": True})
            last_post = self.raw_data_collection.find_one(sort=[("scraped_at", pymongo.DESCENDING)])
            last_scraped = last_post["scraped_at"] if last_post else None
            success_rate = (total_offers / total_posts * 100) if total_posts > 0 else 0
            return {
                "totalPosts": total_posts,
                "totalOffers": total_offers,
                "activeProfiles": active_profiles,
                "lastScraped": last_scraped.isoformat() if last_scraped else None,
                "successRate": round(success_rate, 2),
            }
        except Exception as e:
            print(f"Error in get_stats: {e}")
            return self.get_mock_stats()

    def get_recent_activity(self, limit: int = 10) -> list:
        if not self.connected:
            return self.get_mock_stats()["recentActivity"]
        try:
            pipeline = [
                {"$sort": {"scraped_at": -1}},
                {"$group": {"_id": {"profile": "$profile", "platform": "$platform"}, "count": {"$sum": 1}, "latest": {"$max": "$scraped_at"}}},
                {"$sort": {"latest": -1}},
                {"$limit": limit},
            ]
            activities = []
            for doc in self.raw_data_collection.aggregate(pipeline):
                diff = datetime.utcnow() - doc["latest"]
                s = diff.total_seconds()
                if s < 60:       time_str = f"{int(s)}s ago"
                elif s < 3600:   time_str = f"{int(s // 60)}m ago"
                elif diff.days == 0: time_str = f"{int(s // 3600)}h ago"
                else:            time_str = f"{diff.days}d ago"
                activities.append({
                    "profile": doc["_id"]["profile"],
                    "platform": doc["_id"]["platform"],
                    "posts": doc["count"],
                    "time": time_str,
                    "status": "success" if doc["count"] > 0 else "warning",
                })
            return activities
        except Exception as e:
            print(f"Error in get_recent_activity: {e}")
            return self.get_mock_stats()["recentActivity"]

    def get_profile_performance(self, time_range: str = "24h", limit: int = 10) -> list:
        if not self.connected:
            return []
        try:
            tf = self.get_time_filter(time_range)
            posts_by_profile = {
                doc["_id"]: doc["total_posts"]
                for doc in self.raw_data_collection.aggregate([
                    {"$match": tf},
                    {"$group": {"_id": "$profile", "total_posts": {"$sum": 1}}},
                    {"$sort": {"total_posts": -1}},
                ])
            }
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            offers_by_profile = {
                doc["_id"]: doc["valid_offers"]
                for doc in self.offers_collection.aggregate([
                    {"$match": offer_filter},
                    {"$group": {"_id": "$profile", "valid_offers": {"$sum": 1}}},
                ])
            }
            rows = []
            for profile, total_posts in posts_by_profile.items():
                valid_offers = offers_by_profile.get(profile, 0)
                rows.append({
                    "profile": profile,
                    "total_posts": total_posts,
                    "valid_offers": valid_offers,
                    "success_rate": round((valid_offers / total_posts * 100) if total_posts > 0 else 0, 2),
                })
            rows.sort(key=lambda x: (x["valid_offers"], x["total_posts"]), reverse=True)
            return rows[:limit]
        except Exception as e:
            print(f"Error in get_profile_performance: {e}")
            return []

    def get_total_profiles_in_range(self, time_range: str = "24h") -> int:
        if not self.connected:
            return 50
        try:
            tf = self.get_time_filter(time_range)
            result = list(self.raw_data_collection.aggregate([
                {"$match": tf}, {"$group": {"_id": "$profile"}}, {"$count": "total"},
            ]))
            return result[0]["total"] if result else 0
        except Exception as e:
            print(f"Error in get_total_profiles_in_range: {e}")
            return 50

    def get_failed_scrapes_count(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {"total": 0, "by_platform": {}, "by_profile": {}}
        try:
            start = self.get_time_filter(time_range)["scraped_at"]["$gte"]
            failed_filter = {
                "$and": [
                    {"scraped_at": {"$gte": start}},
                    {"$or": [{"post_text":   {"$in": [None, ""]}}, {"post_text":   {"$exists": False}}]},
                    {"$or": [{"post_images": {"$in": [[], None, ""]}}, {"post_images": {"$exists": False}}]},
                    {"$or": [{"post_video":  {"$in": [[], None, ""]}}, {"post_video":  {"$exists": False}}]},
                ]
            }
            total = self.raw_data_collection.count_documents(failed_filter)
            by_platform = {doc["_id"]: doc["count"] for doc in self.raw_data_collection.aggregate([
                {"$match": failed_filter}, {"$group": {"_id": "$platform", "count": {"$sum": 1}}}, {"$sort": {"count": -1}},
            ])}
            by_profile = {doc["_id"]: doc["count"] for doc in self.raw_data_collection.aggregate([
                {"$match": failed_filter}, {"$group": {"_id": "$profile", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10},
            ])}
            return {"total": total, "by_platform": by_platform, "by_profile": by_profile}
        except Exception as e:
            print(f"Error in get_failed_scrapes_count: {e}")
            return {"total": 0, "by_platform": {}, "by_profile": {}}

    def get_inactive_offers_count(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {"total": 0, "by_brand": {}, "by_category": {}}
        try:
            tf = self.get_time_filter(time_range)
            current_date = datetime.now()
            inactive_filter = {
                **tf,
                "brand_name": {"$ne": None, "$exists": True},
                "confidence_score": {"$gt": 0.8},
                "$or": [
                    {"normalized_fields.valid_until": {"$exists": True, "$ne": None, "$lt": current_date.strftime("%Y-%m-%d")}},
                    {"is_active": False, "$or": [{"normalized_fields.valid_until": {"$exists": False}}, {"normalized_fields.valid_until": None}]},
                ],
            }
            total = self.offers_collection.count_documents(inactive_filter)
            by_brand = {doc["_id"]: doc["count"] for doc in self.offers_collection.aggregate([
                {"$match": inactive_filter}, {"$group": {"_id": "$brand_name", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10},
            ])}
            by_category = {doc["_id"]: doc["count"] for doc in self.offers_collection.aggregate([
                {"$match": inactive_filter}, {"$group": {"_id": "$product_category", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10},
            ]) if doc["_id"]}
            return {"total": total, "by_brand": by_brand, "by_category": by_category}
        except Exception as e:
            print(f"Error in get_inactive_offers_count: {e}")
            return {"total": 0, "by_brand": {}, "by_category": {}}

    def get_stale_profiles(self, hours: int = 24) -> dict:
        if not self.connected:
            return {"count": 0, "profiles": []}
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            query = {
                "target_type": "profile", "active": True,
                "$or": [{"last_scraped": {"$lt": cutoff}}, {"last_scraped": {"$exists": False}}, {"last_scraped": None}],
            }
            total = self.targets_collection.count_documents(query)
            return {"count": total}
        except Exception as e:
            print(f"Error in get_stale_profiles: {e}")
            return {"count": 0, "profiles": []}

    def get_ai_extraction_metrics(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {
                "extraction_success_rate": 85.5, "average_confidence": 0.87,
                "low_confidence_count": 15, "medium_confidence_count": 45, "high_confidence_count": 120,
                "confidence_distribution": {"0.0-0.3": 2, "0.3-0.5": 5, "0.5-0.7": 8, "0.7-0.8": 15, "0.8-0.9": 45, "0.9-1.0": 120},
                "extraction_over_time": [],
            }
        try:
            tf = self.get_time_filter(time_range)
            total_extracted = self.offers_collection.count_documents({**tf, "extracted_by_llm": True})
            successful = self.offers_collection.count_documents({
                **tf, "extracted_by_llm": True, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.7},
            })
            rate = (successful / total_extracted * 100) if total_extracted > 0 else 0

            avg_result = list(self.offers_collection.aggregate([
                {"$match": {**tf, "extracted_by_llm": True, "confidence_score": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": None, "avg_confidence": {"$avg": "$confidence_score"}}},
            ]))
            avg_confidence = avg_result[0]["avg_confidence"] if avg_result else 0

            low  = self.offers_collection.count_documents({**tf, "confidence_score": {"$gte": 0.7, "$lt": 0.8}})
            med  = self.offers_collection.count_documents({**tf, "confidence_score": {"$gte": 0.8, "$lt": 0.9}})
            high = self.offers_collection.count_documents({**tf, "confidence_score": {"$gte": 0.9}})

            dist = {}
            for label, lo, hi in [("0.0-0.3",0,0.3),("0.3-0.5",0.3,0.5),("0.5-0.7",0.5,0.7),("0.7-0.8",0.7,0.8),("0.8-0.9",0.8,0.9),("0.9-1.0",0.9,1.0)]:
                dist[label] = self.offers_collection.count_documents({**tf, "confidence_score": {"$gte": lo, "$lt": hi}})

            group_id = (
                {"year": {"$year": "$scraped_at"}, "month": {"$month": "$scraped_at"}, "day": {"$dayOfMonth": "$scraped_at"}, "hour": {"$hour": "$scraped_at"}}
                if time_range == "24h" else
                {"year": {"$year": "$scraped_at"}, "month": {"$month": "$scraped_at"}, "day": {"$dayOfMonth": "$scraped_at"}}
            )
            timeline = list(self.offers_collection.aggregate([
                {"$match": {**tf, "extracted_by_llm": True}},
                {"$group": {
                    "_id": group_id, "total": {"$sum": 1}, "timestamp": {"$first": "$scraped_at"},
                    "successful": {"$sum": {"$cond": [{"$and": [{"$ne": ["$brand_name", None]}, {"$gt": ["$confidence_score", 0.7]}]}, 1, 0]}},
                    "avg_confidence": {"$avg": "$confidence_score"},
                }},
                {"$sort": {"timestamp": 1}},
                {"$project": {"_id": 0, "timestamp": 1, "total": 1, "successful": 1, "avg_confidence": {"$round": ["$avg_confidence", 2]},
                              "success_rate": {"$multiply": [{"$divide": ["$successful", "$total"]}, 100]}}},
            ]))
            return {
                "extraction_success_rate": round(rate, 1),
                "average_confidence": round(avg_confidence, 2),
                "low_confidence_count": low,
                "medium_confidence_count": med,
                "high_confidence_count": high,
                "confidence_distribution": dist,
                "extraction_over_time": timeline,
            }
        except Exception as e:
            print(f"Error in get_ai_extraction_metrics: {e}")
            return {"extraction_success_rate": 0, "average_confidence": 0, "low_confidence_count": 0,
                    "medium_confidence_count": 0, "high_confidence_count": 0, "confidence_distribution": {}, "extraction_over_time": []}

    # ── scraping ──────────────────────────────────────────────────────────────

    def get_by_platform(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return self.get_mock_stats()["byPlatform"]
        try:
            tf = self.get_time_filter(time_range)
            result = self.raw_data_collection.aggregate([
                {"$match": tf}, {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
            ])
            return {doc["_id"]: doc["count"] for doc in result}
        except Exception as e:
            print(f"Error in get_by_platform: {e}")
            return self.get_mock_stats()["byPlatform"]

    def get_by_profile(self, time_range: str = "24h", limit: int = 10) -> dict:
        if not self.connected:
            return self.get_mock_stats()["byProfile"]
        try:
            tf = self.get_time_filter(time_range)
            result = self.raw_data_collection.aggregate([
                {"$match": tf}, {"$group": {"_id": "$profile", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}, {"$limit": limit},
            ])
            return {doc["_id"]: doc["count"] for doc in result}
        except Exception as e:
            print(f"Error in get_by_profile: {e}")
            return self.get_mock_stats()["byProfile"]

    def get_profile_success_rate(self, time_range: str = "24h", limit: int = 10) -> dict:
        if not self.connected:
            return {}
        try:
            tf = self.get_time_filter(time_range)
            posts_by_profile = {
                doc["_id"]: doc["total_posts"]
                for doc in self.raw_data_collection.aggregate([
                    {"$match": tf}, {"$group": {"_id": "$profile", "total_posts": {"$sum": 1}}},
                    {"$sort": {"total_posts": -1}}, {"$limit": limit},
                ])
            }
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            offers_by_profile = {
                doc["_id"]: doc["offers"]
                for doc in self.offers_collection.aggregate([
                    {"$match": offer_filter}, {"$group": {"_id": "$profile", "offers": {"$sum": 1}}},
                ])
            }
            return {
                profile: {
                    "rate": round((offers_by_profile.get(profile, 0) / posts * 100) if posts > 0 else 0, 2),
                    "posts": posts,
                    "offers": offers_by_profile.get(profile, 0),
                }
                for profile, posts in posts_by_profile.items()
            }
        except Exception as e:
            print(f"Error in get_profile_success_rate: {e}")
            return {}

    def get_incomplete_posts(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {"text_only": [], "image_only": [], "video_only": [], "total": 0}
        try:
            start = self.get_time_filter(time_range)["scraped_at"]["$gte"]

            def has(field):
                return {"$and": [{field: {"$nin": [None, "", []]}}, {field: {"$exists": True}}]}

            def has_not(field):
                return {"$or": [{field: {"$in": [None, "", []]}}, {field: {"$exists": False}}]}

            proj = {"_id": 1, "post_id": 1, "platform": 1, "profile": 1, "scraped_at": 1}

            def fetch(f):
                docs = list(self.raw_data_collection.find(f, proj).limit(50))
                for d in docs:
                    d["_id"] = str(d["_id"])
                    if "scraped_at" in d and hasattr(d["scraped_at"], "isoformat"):
                        d["scraped_at"] = d["scraped_at"].isoformat()
                return docs

            base = {"scraped_at": {"$gte": start}}
            text_only  = fetch({"$and": [base, has("post_text"),   has_not("post_images"), has_not("post_video")]})
            image_only = fetch({"$and": [base, has("post_images"), has_not("post_text"),   has_not("post_video")]})
            video_only = fetch({"$and": [base, has("post_video"),  has_not("post_text"),   has_not("post_images")]})
            return {"text_only": text_only, "image_only": image_only, "video_only": video_only,
                    "total": len(text_only) + len(image_only) + len(video_only)}
        except Exception as e:
            print(f"Error in get_incomplete_posts: {e}")
            return {"text_only": [], "image_only": [], "video_only": [], "total": 0}

    # ── offers ────────────────────────────────────────────────────────────────

    def get_offers_by_category(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return self.get_mock_stats()["offersByCategory"]
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            result = self.offers_collection.aggregate([
                {"$match": offer_filter}, {"$group": {"_id": "$product_category", "count": {"$sum": 1}}}, {"$sort": {"count": -1}},
            ])
            return {doc["_id"]: doc["count"] for doc in result if doc["_id"]}
        except Exception as e:
            print(f"Error in get_offers_by_category: {e}")
            return self.get_mock_stats()["offersByCategory"]

    def get_top_brands(self, time_range: str = "24h", limit: int = 50) -> dict:
        if not self.connected:
            return {}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            result = self.offers_collection.aggregate([
                {"$match": offer_filter}, {"$group": {"_id": "$brand_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}, {"$limit": limit},
            ])
            return {doc["_id"]: doc["count"] for doc in result if doc["_id"]}
        except Exception as e:
            print(f"Error in get_top_brands: {e}")
            return {}

    def get_offers_by_country(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {
                **tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8},
                "is_normalized": True, "normalized_fields.location.country": {"$exists": True, "$ne": None},
            }
            result = self.offers_collection.aggregate([
                {"$match": offer_filter},
                {"$group": {"_id": "$normalized_fields.location.country", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ])
            return {doc["_id"]: doc["count"] for doc in result if doc["_id"]}
        except Exception as e:
            print(f"Error in get_offers_by_country: {e}")
            return {}

    def get_discount_types_distribution(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {
                **tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8},
                "is_normalized": True, "normalized_fields.discounts": {"$exists": True, "$ne": []},
            }
            result = self.offers_collection.aggregate([
                {"$match": offer_filter}, {"$unwind": "$normalized_fields.discounts"},
                {"$group": {"_id": "$normalized_fields.discounts.discount_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ])
            return {doc["_id"]: doc["count"] for doc in result if doc["_id"]}
        except Exception as e:
            print(f"Error in get_discount_types_distribution: {e}")
            return {}

    def get_promo_code_usage(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            results = list(self.offers_collection.aggregate([
                {"$match": offer_filter},
                {"$group": {"_id": {"$cond": [{"$and": [{"$ne": ["$promo_code", None]}, {"$ne": ["$promo_code", ""]}]}, "with_code", "without_code"]}, "count": {"$sum": 1}}},
            ]))
            stats = {"with_promo_code": 0, "without_promo_code": 0}
            for doc in results:
                if doc["_id"] == "with_code": stats["with_promo_code"] = doc["count"]
                else: stats["without_promo_code"] = doc["count"]
            total = stats["with_promo_code"] + stats["without_promo_code"]
            stats["percentage_with_code"] = round((stats["with_promo_code"] / total * 100) if total > 0 else 0, 2)
            return stats
        except Exception as e:
            print(f"Error in get_promo_code_usage: {e}")
            return {"with_promo_code": 0, "without_promo_code": 0, "percentage_with_code": 0}

    def get_average_discount_value(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {"overall": 0, "by_currency": {}}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {
                **tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8},
                "is_normalized": True, "normalized_fields.discounts": {"$exists": True, "$ne": []},
            }
            results = list(self.offers_collection.aggregate([
                {"$match": offer_filter}, {"$unwind": "$normalized_fields.discounts"},
                {"$match": {"normalized_fields.discounts.discount_amount": {"$ne": None, "$exists": True, "$gt": 0}}},
                {"$group": {"_id": "$normalized_fields.discounts.discount_currency", "avg_discount": {"$avg": "$normalized_fields.discounts.discount_amount"}, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]))
            by_currency, total_discount, total_count = {}, 0, 0
            for doc in results:
                if doc["_id"]:
                    by_currency[doc["_id"]] = round(doc["avg_discount"], 2)
                    total_discount += doc["avg_discount"] * doc["count"]
                    total_count += doc["count"]
            return {"overall": round(total_discount / total_count, 2) if total_count > 0 else 0, "by_currency": by_currency}
        except Exception as e:
            print(f"Error in get_average_discount_value: {e}")
            return {"overall": 0, "by_currency": {}}

    def get_offer_type_breakdown(self, time_range: str = "24h") -> dict:
        if not self.connected:
            return {}
        try:
            tf = self.get_time_filter(time_range)
            offer_filter = {**tf, "brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}}
            result = self.offers_collection.aggregate([
                {"$match": offer_filter}, {"$group": {"_id": "$offer_type", "count": {"$sum": 1}}}, {"$sort": {"count": -1}},
            ])
            return {doc["_id"]: doc["count"] for doc in result if doc["_id"]}
        except Exception as e:
            print(f"Error in get_offer_type_breakdown: {e}")
            return {}

    # ── system ────────────────────────────────────────────────────────────────

    def get_total_profiles_count(self) -> int:
        if not self.connected:
            return 50
        try:
            return self.targets_collection.count_documents({})
        except Exception as e:
            print(f"Error in get_total_profiles_count: {e}")
            return 50

    def get_database_stats(self) -> dict:
        if not self.connected:
            return {
                "raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"},
                "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"},
            }
        try:
            raw = self.social_scraper_db.command("collStats", "raw_social_data")
            offers_s = self.offer_insights_db.command("collStats", "offers")
            valid = self.offers_collection.count_documents({"brand_name": {"$ne": None, "$exists": True}, "confidence_score": {"$gt": 0.8}})
            return {
                "raw_social_data": {"count": raw.get("count", 0), "size": self._format_bytes(raw.get("size", 0)), "avgObjSize": self._format_bytes(raw.get("avgObjSize", 0))},
                "offers": {"count": offers_s.get("count", 0), "validCount": valid, "size": self._format_bytes(offers_s.get("size", 0)), "avgObjSize": self._format_bytes(offers_s.get("avgObjSize", 0))},
            }
        except Exception as e:
            print(f"Error in get_database_stats: {e}")
            return {"raw_social_data": {"count": 0, "size": "0 B", "avgObjSize": "0 B"}, "offers": {"count": 0, "validCount": 0, "size": "0 B", "avgObjSize": "0 B"}}

    # ── config ────────────────────────────────────────────────────────────────

    def get_system_config(self) -> list:
        if not self.connected:
            return self._get_mock_config()
        try:
            docs = list(self.system_config_collection.find(
                {}, {"_id": 1, "type": 1, "version": 1, "is_active": 1, "data": 1, "updated_at": 1}
            ).sort("type", pymongo.ASCENDING))
            return [
                {
                    "id": str(doc["_id"]),
                    "type": doc.get("type"),
                    "version": doc.get("version", 1),
                    "is_active": doc.get("is_active", True),
                    "data": doc.get("data", {}),
                    "updated_at": doc["updated_at"].isoformat() if isinstance(doc.get("updated_at"), datetime) else str(doc.get("updated_at", "")),
                }
                for doc in docs
            ]
        except Exception as e:
            print(f"Error in get_system_config: {e}")
            return self._get_mock_config()