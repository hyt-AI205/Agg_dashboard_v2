"""
offer_intel.storage.raw_social_data_store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Idempotent MongoDB store for raw scraped social-media posts.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pymongo

from offer_intel.utils.settings import settings

logger = logging.getLogger(__name__)


class RawSocialDataStore:
    """
    Insert or upsert raw social-media posts into MongoDB.

    Every post is keyed on ``post_id``, so re-scraping the same post is safe —
    it will update the document rather than create a duplicate.

    The collection has a 90-day TTL index on ``scraped_at`` so old raw data is
    automatically purged by MongoDB.
    """

    TTL_SECONDS = 60 * 60 * 24 * 90  # 90 days

    def __init__(self, mongo_uri: str | None = None, db_name: str | None = None) -> None:
        uri = mongo_uri or settings.MONGO_URI
        db = db_name or "social_scraper"

        self.client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=10_000,
        )
        self.collection = self.client[db]["raw_social_data"]
        self._ensure_indexes()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index(
                [("scraped_at", 1)],
                expireAfterSeconds=self.TTL_SECONDS,
                name="ttl_scraped_at",
            )
            self.collection.create_index(
                [("post_id", pymongo.ASCENDING)],
                unique=True,
                name="unique_post_id",
            )
        except pymongo.errors.PyMongoError as exc:
            logger.debug("Index creation notice: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert_raw(
        self,
        *,
        source: str,
        platform: str,
        profile: str,
        post_id: str,
        post_text: str,
        post_images: list[str],
        post_video: list[str],
        payload: dict,
    ) -> str:
        """
        Upsert a single raw post.

        Returns
        -------
        "inserted"  – new document was created
        "updated"   – existing document was refreshed
        "unchanged" – document already exists and content is identical
        """
        if not post_id:
            raise ValueError("post_id must not be empty")

        doc = {
            "source": source,
            "platform": platform,
            "profile": profile,
            "post_text": post_text,
            "post_images": post_images,
            "post_video": post_video,
            "post_id": post_id,
            "scraped_at": datetime.utcnow(),
            "raw_payload": payload,
        }

        result = self.collection.update_one(
            {"post_id": post_id},
            {"$set": doc},
            upsert=True,
        )

        if result.upserted_id:
            return "inserted"
        if result.modified_count:
            return "updated"
        return "unchanged"

    def get_latest_by_profile(self, profile: str, limit: int = 10) -> list[dict]:
        return list(
            self.collection.find({"profile": profile})
            .sort("scraped_at", -1)
            .limit(limit)
        )
