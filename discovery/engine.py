# discovery/engine.py
# ─────────────────────────────────────────────────────────────
# Discovery engine — orchestrates:
#   Apify scraping → GPT-4o scoring → MongoDB upsert
#
# Can be called from the Flask API, a CLI script, or a scheduler.
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .apify_client import ApifyDiscoveryClient
from .config import REGIONS, Region
from .scorer import ProfileScorer
from .storage import DiscoveryStorage

log = logging.getLogger(__name__)
UTC = timezone.utc


# ─────────────────────────────────────────────────────────────
#  Run result
# ─────────────────────────────────────────────────────────────
@dataclass
class DiscoveryResult:
    region_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None

    # pipeline step counts
    instagram_raw: int = 0
    tiktok_raw: int = 0
    facebook_raw: int = 0
    total_raw: int = 0

    scored: int = 0
    recommended: int = 0

    # storage summary
    inserted: int = 0
    updated: int = 0
    skipped: int = 0

    error: Optional[str] = None

    # per-platform breakdown
    platform_stats: dict = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict:
        return {
            "region": self.region_name,
            "success": self.success,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "pipeline": {
                "instagram_raw":  self.instagram_raw,
                "tiktok_raw":     self.tiktok_raw,
                "facebook_raw":   self.facebook_raw,
                "total_raw":      self.total_raw,
                "scored":         self.scored,
                "recommended":    self.recommended,
            },
            "storage": {
                "inserted": self.inserted,
                "updated":  self.updated,
                "skipped":  self.skipped,
            },
            "platform_stats": self.platform_stats,
            "error": self.error,
        }


# ─────────────────────────────────────────────────────────────
#  Engine
# ─────────────────────────────────────────────────────────────
class DiscoveryEngine:
    """
    Single entry-point for a full discovery run.

    Usage:
        engine = DiscoveryEngine()
        result = engine.run("yemen")
        print(result.to_dict())
    """

    def __init__(self):
        self._apify   = ApifyDiscoveryClient()
        self._storage = DiscoveryStorage()

    def run(
        self,
        region_key: str,
        platforms: Optional[list[str]] = None,
        limit: Optional[int] = None,
    ) -> DiscoveryResult:
        """
        Run a full discovery cycle for the given region.

        :param region_key:  Key from config.REGIONS (e.g. "yemen")
        :param platforms:   Subset of ["instagram","tiktok","facebook"]. None = all.
        :param limit:       Cap profiles per platform (for testing, saves API credits).
        """
        result = DiscoveryResult(
            region_name=region_key,
            started_at=datetime.now(UTC),
        )

        # ── resolve region ───────────────────────────────────
        region = REGIONS.get(region_key)
        if not region:
            result.error = f"Unknown region '{region_key}'. Available: {list(REGIONS)}"
            log.error(result.error)
            return result

        platforms = platforms or ["instagram", "tiktok", "facebook"]
        if limit:
            log.info("[TEST MODE] Profile limit set to %d per platform", limit)
        log.info("=== Discovery run | region=%s | platforms=%s ===", region_key, platforms)

        # ── Step 1: scrape ───────────────────────────────────
        all_candidates = []

        if "instagram" in platforms:
            try:
                ig_candidates = self._apify.discover_instagram(region, limit=limit)
                result.instagram_raw = len(ig_candidates)
                all_candidates.extend(ig_candidates)
                log.info("[Step 1] Instagram → %d candidates (single actor, aggregated by username)", result.instagram_raw)
            except Exception as exc:
                log.error("Instagram scraping failed: %s", exc)
                result.platform_stats["instagram"] = {"error": str(exc)}

        if "tiktok" in platforms:
            try:
                tt_candidates = self._apify.discover_tiktok(region, limit=limit)
                result.tiktok_raw = len(tt_candidates)
                all_candidates.extend(tt_candidates)
                log.info("[Step 1] TikTok → %d candidates", result.tiktok_raw)
            except Exception as exc:
                log.error("TikTok scraping failed: %s", exc)
                result.platform_stats["tiktok"] = {"error": str(exc)}

        if "facebook" in platforms:
            try:
                fb_candidates = self._apify.discover_facebook(region, limit=limit)
                result.facebook_raw = len(fb_candidates)
                all_candidates.extend(fb_candidates)
                log.info("[Step 1] Facebook → %d candidates", result.facebook_raw)
            except Exception as exc:
                log.error("Facebook scraping failed: %s", exc)
                result.platform_stats["facebook"] = {"error": str(exc)}

        result.total_raw = len(all_candidates)

        if not all_candidates:
            result.error = "No candidates collected from any platform."
            result.finished_at = datetime.now(UTC)
            return result

        # ── Step 2: GPT-4o scoring ───────────────────────────
        log.info("[Step 2] Scoring %d candidates with GPT-4o …", result.total_raw)
        scorer = ProfileScorer(region=region)
        scored = scorer.score_batch(all_candidates)

        result.scored      = len(scored)
        result.recommended = sum(1 for s in scored if s.recommended)
        log.info(
            "[Step 2] Scored=%d | Recommended=%d | Rejected=%d",
            result.scored, result.recommended, result.scored - result.recommended,
        )

        # ── Step 3: store ────────────────────────────────────
        log.info("[Step 3] Upserting %d recommended profiles …", result.recommended)
        summary = self._storage.upsert_candidates(scored)
        result.inserted = summary["inserted"]
        result.updated  = summary["updated"]
        result.skipped  = summary["skipped"]

        # ── per-platform breakdown ───────────────────────────
        for platform in platforms:
            platform_scored = [s for s in scored if s.candidate.platform == platform]
            result.platform_stats[platform] = {
                "raw":         sum(1 for s in platform_scored),
                "recommended": sum(1 for s in platform_scored if s.recommended),
                "avg_score":   (
                    round(
                        sum(s.final_score for s in platform_scored) / len(platform_scored), 1
                    ) if platform_scored else 0
                ),
            }

        result.finished_at = datetime.now(UTC)
        elapsed = (result.finished_at - result.started_at).total_seconds()
        log.info(
            "=== Discovery complete in %.1fs | inserted=%d updated=%d ===",
            elapsed, result.inserted, result.updated,
        )
        return result