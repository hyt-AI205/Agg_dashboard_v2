# discovery/storage.py
# ─────────────────────────────────────────────────────────────
# Handles all MongoDB operations for the discovery layer.
# Designed to be completely additive — never touches existing
# documents that were added manually by operators.
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.collection import Collection

from .config import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION,
    DEDUP_HOURS,
)
from .scorer import ScoredProfile

log = logging.getLogger(__name__)

UTC = timezone.utc


# ─────────────────────────────────────────────────────────────
#  Storage client
# ─────────────────────────────────────────────────────────────
class DiscoveryStorage:

    def __init__(self):
        self._mongo = MongoClient(MONGO_URI)
        self._col: Collection = self._mongo[MONGO_DB][MONGO_COLLECTION]
        self._ensure_indexes()

    # ── public API ───────────────────────────────────────────

    def upsert_candidates(
        self, scored_profiles: list[ScoredProfile]
    ) -> dict[str, int]:
        """
        Insert or update candidate profiles into scrape_targets.

        Rules:
        - Only recommended=True profiles are stored.
        - A profile already in the collection (same platform+value) is
          skipped unless DEDUP_HOURS have passed since it was last discovered.
        - Profiles added manually (added_by != 'discovery') are NEVER
          overwritten — only their discovery_meta is refreshed.

        Returns a summary dict: {inserted, updated, skipped}.
        """
        summary = {"inserted": 0, "updated": 0, "skipped": 0}
        ops: list[UpdateOne] = []

        cutoff = datetime.now(UTC) - timedelta(hours=DEDUP_HOURS)

        for sp in scored_profiles:
            if not sp.recommended:
                summary["skipped"] += 1
                continue

            doc = sp.to_scrape_target()
            platform = doc["platform"]
            value    = doc["value"]

            existing = self._col.find_one(
                {"platform": platform, "value": value},
                {"_id": 1, "added_by": 1, "last_discovered_at": 1},
            )

            if existing:
                last_seen = existing.get("last_discovered_at")
                if last_seen and last_seen.replace(tzinfo=UTC) > cutoff:
                    log.debug(
                        "Skipping @%s (%s) — re-discovered within dedup window.",
                        value, platform,
                    )
                    summary["skipped"] += 1
                    continue

                # Refresh discovery_meta but preserve operator fields
                ops.append(UpdateOne(
                    {"platform": platform, "value": value},
                    {"$set": {
                        "discovery_meta": doc["discovery_meta"],
                        "last_discovered_at": datetime.now(UTC),
                    }},
                ))
                summary["updated"] += 1

            else:
                # Brand-new candidate
                doc["added_at"]          = datetime.now(UTC)
                doc["last_discovered_at"] = datetime.now(UTC)
                ops.append(UpdateOne(
                    {"platform": platform, "value": value},
                    {"$setOnInsert": doc},
                    upsert=True,
                ))
                summary["inserted"] += 1

        if ops:
            result = self._col.bulk_write(ops, ordered=False)
            log.info(
                "MongoDB bulk_write → inserted=%d upserted=%d modified=%d",
                result.inserted_count,
                result.upserted_count,
                result.modified_count,
            )

        return summary

    def get_discovery_stats(self, region: Optional[str] = None) -> dict:
        """Return aggregate counts useful for the dashboard."""
        match: dict = {"added_by": "discovery"}
        if region:
            match["discovery_meta.region"] = region

        pipeline = [
            {"$match": match},
            {"$group": {
                "_id": "$platform",
                "total":    {"$sum": 1},
                "active":   {"$sum": {"$cond": ["$active", 1, 0]}},
                "pending":  {"$sum": {"$cond": [{"$eq": ["$active", False]}, 1, 0]}},
                "avg_score": {"$avg": "$discovery_meta.final_score"},
            }},
        ]
        rows = list(self._col.aggregate(pipeline))
        return {r["_id"]: r for r in rows}

    def get_pending_candidates(
        self,
        region: Optional[str] = None,
        platform: Optional[str] = None,
        min_score: int = 0,
        limit: int = 100,
    ) -> list[dict]:
        """
        Return pending discovery candidates for the human-review UI.
        Sorted by final_score descending.
        """
        filt: dict = {"added_by": "discovery", "active": False}
        if region:
            filt["discovery_meta.region"] = region
        if platform:
            filt["platform"] = platform
        if min_score:
            filt["discovery_meta.final_score"] = {"$gte": min_score}

        cursor = (
            self._col.find(filt)
            .sort("discovery_meta.final_score", -1)
            .limit(limit)
        )
        docs = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            docs.append(doc)
        return docs

    # ── index setup ──────────────────────────────────────────

    def _ensure_indexes(self):
        """Create indexes needed by the discovery layer (idempotent)."""
        try:
            self._col.create_index(
                [("platform", ASCENDING), ("value", ASCENDING)],
                unique=True,
                background=True,
            )
            self._col.create_index(
                [("added_by", ASCENDING), ("active", ASCENDING)],
                background=True,
            )
            self._col.create_index(
                [("discovery_meta.region", ASCENDING)],
                background=True,
            )
            self._col.create_index(
                [("discovery_meta.final_score", ASCENDING)],
                background=True,
            )
            log.debug("MongoDB indexes ensured.")
        except Exception as exc:
            log.warning("Index creation warning (may already exist): %s", exc)
