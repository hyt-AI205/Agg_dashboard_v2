"""
offer_intel.storage.scrape_target_store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
MongoDB-backed registry of scrape targets (profiles / URLs to monitor).
"""

from __future__ import annotations

import logging
from datetime import datetime

import pymongo

from offer_intel.utils.settings import settings

logger = logging.getLogger(__name__)


class ScrapeTargetStore:
    """
    Stores and retrieves the list of social-media profiles to scrape.

    Documents are keyed on ``(platform, target_type, value)`` so adding the
    same profile twice is a no-op.
    """

    def __init__(self, mongo_uri: str | None = None, db_name: str = "social_scraper") -> None:
        uri = mongo_uri or settings.MONGO_URI
        self.client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=10_000,
        )
        self.collection = self.client[db_name]["scrape_targets"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index(
                [
                    ("platform", pymongo.ASCENDING),
                    ("target_type", pymongo.ASCENDING),
                    ("value", pymongo.ASCENDING),
                ],
                unique=True,
                name="unique_target",
            )
        except pymongo.errors.PyMongoError as exc:
            logger.debug("Index creation notice: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_target(
        self,
        platform: str,
        target_type: str,
        value: str,
        added_by: str = "admin",
    ) -> None:
        """Register a new scrape target (idempotent)."""
        doc = {
            "platform": platform,
            "target_type": target_type,
            "value": value,
            "active": True,
            "added_by": added_by,
            "added_at": datetime.utcnow(),
            "last_scraped": None,
        }
        self.collection.update_one(
            {"platform": platform, "target_type": target_type, "value": value},
            {"$setOnInsert": doc},
            upsert=True,
        )

    def get_active_targets(self, platform: str, target_type: str) -> list[dict]:
        """Return all active targets for the given platform and type."""
        return list(
            self.collection.find(
                {"platform": platform, "target_type": target_type, "active": True}
            )
        )

    def mark_scraped(self, platform: str, profile_handle: str) -> None:
        """Record the timestamp of the most recent successful scrape."""
        self.collection.update_one(
            {"platform": platform, "target_type": "profile", "value": profile_handle},
            {"$set": {"last_scraped": datetime.utcnow()}},
        )

    def deactivate_target(self, platform: str, target_type: str, value: str) -> None:
        """Soft-delete a target so it is excluded from future scrape runs."""
        self.collection.update_one(
            {"platform": platform, "target_type": target_type, "value": value},
            {"$set": {"active": False}},
        )
