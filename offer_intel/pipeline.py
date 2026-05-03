"""
offer_intel.pipeline
~~~~~~~~~~~~~~~~~~~~
Integrated scrape → extract → normalise → publish pipeline.

Entry point
-----------
    python -m offer_intel.pipeline          # run once then loop on schedule
    python -m offer_intel.pipeline --once   # single run, then exit
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import schedule

from offer_intel.extraction.offer_extractor import OfferExtractor
from offer_intel.normalization.offer_normalizer import OfferNormalizer
from offer_intel.normalization.platform_normalizer import NormalisedPost, PlatformNormalizer
from offer_intel.scraping.scrapers import ScraperRouter
from offer_intel.storage.raw_social_data_store import RawSocialDataStore
from offer_intel.storage.scrape_target_store import ScrapeTargetStore
from offer_intel.utils.settings import config
from dotenv import load_dotenv
load_dotenv()  # loads .env from the project root
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-post processing helper
# ---------------------------------------------------------------------------

def _process_post(
    post: NormalisedPost,
    source: str,
    platform: str,
    raw_store: RawSocialDataStore,
    extractor: OfferExtractor,
    normalizer: OfferNormalizer,
    counters: dict,
) -> None:
    """
    End-to-end processing for a single normalised post:

    1. Store raw post
    2. Extract offer via LLM
    3. Normalise offer (discount / location / dates)
    4. Promote to ``offers_public`` if confidence ≥ threshold
    """
    # ── 1. Raw storage ────────────────────────────────────────────────
    result = raw_store.insert_raw(
        source=source,
        platform=platform,
        profile=post.profile_id,
        post_id=post.post_id,
        post_text=post.post_text,
        post_images=post.post_images,
        post_video=post.post_video,
        payload=post.payload,
    )
    counters[result] = counters.get(result, 0) + 1
    logger.debug("[%s] raw store → %s", post.post_id[:30], result)

    # ── 2. Offer extraction (skip if already done) ────────────────────
    if extractor.offers_collection.find_one({"post_id": post.post_id}):
        return

    offer_data = extractor.extract_offer(post.post_text)
    saved = extractor.save_offer(
        post_id=post.post_id,
        source=source,
        platform=platform,
        profile=post.profile_id,
        post_text=post.post_text,
        post_images=post.post_images,
        post_video=post.post_video,
        scraped_at=post.payload.get("scraped_at"),
        offer_data=offer_data,
    )
    if not saved:
        return
    counters["offers_extracted"] = counters.get("offers_extracted", 0) + 1

    # ── 3. Normalisation ──────────────────────────────────────────────
    if not offer_data.get("extracted_by_llm"):
        return

    raw_doc = extractor.offers_collection.find_one({"post_id": post.post_id})
    if not raw_doc:
        return

    normalised = normalizer.normalise_offer(raw_doc)
    if not normalised:
        return

    normalizer.save_normalised_offer(post.post_id, normalised)
    counters["offers_normalised"] = counters.get("offers_normalised", 0) + 1

    # ── 4. Promote to public ──────────────────────────────────────────
    pub = extractor.public_builder.build_from_offer(post.post_id)
    if pub:
        counters["offers_published"] = counters.get("offers_published", 0) + 1


# ---------------------------------------------------------------------------
# Per-platform scrape job
# ---------------------------------------------------------------------------

async def scrape_platform(
    platform: str,
    raw_store: RawSocialDataStore,
    target_store: ScrapeTargetStore,
    normalizer_platform: PlatformNormalizer,
    extractor: OfferExtractor,
    offer_normalizer: OfferNormalizer,
) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 60)
    logger.info("[%s] Starting scrape: %s", ts, platform.upper())

    counters: dict = {}

    try:
        router = ScraperRouter(platform)
        targets = target_store.get_active_targets(platform=platform, target_type="profile")
        profiles = [t["value"] for t in targets]

        if not profiles:
            logger.warning("No active profiles for %s", platform)
            return

        logger.info("Scraping %d profiles for %s…", len(profiles), platform)
        results, source = await router.scrape_profiles(profiles)

        if not results:
            logger.error("No results returned for %s", platform)
            return

        # Resolve the correct schema key (Apify returns different payload shapes)
        schema_key = platform
        if platform == "tiktok" and source == "apify":
            schema_key = "apify_tiktok"
        elif platform == "instagram" and source == "apify":
            schema_key = "apify_instagram"

        for idx, raw_item in enumerate(results):
            # Facebook pages don't embed a handle — derive it from the payload
            if platform == "facebook":
                handle = (
                    raw_item.get("pageName")
                    or raw_item.get("facebookUrl", "").rstrip("/").rsplit("/", 1)[-1]
                )
            else:
                handle = profiles[idx] if idx < len(profiles) else None

            try:
                posts = normalizer_platform.normalise(schema_key, raw_item, handle)
                for post in posts:
                    _process_post(
                        post, source, platform,
                        raw_store, extractor, offer_normalizer, counters,
                    )
                target_store.mark_scraped(platform, handle)
            except Exception as exc:
                logger.error("Error processing item for %s: %s", platform, exc, exc_info=True)

    except Exception as exc:
        logger.error("Critical error scraping %s: %s", platform, exc, exc_info=True)

    inserted = counters.get("inserted", 0)
    updated = counters.get("updated", 0)
    skipped = counters.get("unchanged", 0)
    logger.info(
        "[%s] Done. raw: +%d / ~%d / -%d | extracted: %d | normalised: %d | published: %d",
        platform.upper(), inserted, updated, skipped,
        counters.get("offers_extracted", 0),
        counters.get("offers_normalised", 0),
        counters.get("offers_published", 0),
    )


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class PipelineScheduler:
    """
    Reads scrape-interval settings from ``config/schedule_config.json`` and
    runs each platform on its own schedule.

    Parameters
    ----------
    run_immediately:
        When ``True`` (default) each platform is scraped once before the
        scheduler loop starts, so you don't have to wait for the first interval.
    """

    def __init__(self) -> None:
        self._raw_store = RawSocialDataStore()
        self._target_store = ScrapeTargetStore()
        self._platform_normalizer = PlatformNormalizer()
        self._extractor = OfferExtractor()
        self._offer_normalizer = OfferNormalizer()

        scraper_cfg = config.scraper
        apify_platforms = set(
            scraper_cfg["providers"].get("apify", {}).get("platforms", {}).keys()
        )
        brightdata_platforms = set(
            scraper_cfg["providers"].get("brightdata", {}).get("platforms", {}).keys()
        )
        self._platforms: list[str] = sorted(apify_platforms | brightdata_platforms)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def _interval_hours(self, platform: str) -> int:
        sched = config.schedule
        return sched.get("per_platform_overrides", {}).get(
            platform, sched.get("global_scrape_interval_hours", 6)
        )

    def _make_job(self, platform: str):
        def job():
            asyncio.run(
                scrape_platform(
                    platform,
                    self._raw_store,
                    self._target_store,
                    self._platform_normalizer,
                    self._extractor,
                    self._offer_normalizer,
                )
            )
        return job

    def _setup_schedules(self) -> None:
        logger.info("=" * 60)
        logger.info("⏰  OFFER INTEL PIPELINE — SCHEDULE")
        logger.info("=" * 60)
        for platform in self._platforms:
            hours = self._interval_hours(platform)
            logger.info("  %-15s every %d hour(s)", platform, hours)
            schedule.every(hours).hours.do(self._make_job(platform))

    def _run_all_now(self) -> None:
        logger.info("Running initial scrape for all platforms…")
        for platform in self._platforms:
            asyncio.run(
                scrape_platform(
                    platform,
                    self._raw_store,
                    self._target_store,
                    self._platform_normalizer,
                    self._extractor,
                    self._offer_normalizer,
                )
            )
            time.sleep(3)

    def start(self, run_immediately: bool = True) -> None:
        """Start the scheduler (blocks until Ctrl-C)."""
        self._setup_schedules()
        if run_immediately:
            self._run_all_now()

        logger.info("Pipeline is running. Press Ctrl-C to stop.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user.")

    def run_once(self) -> None:
        """Scrape all platforms once, then return (useful for cron / testing)."""
        self._run_all_now()


# ---------------------------------------------------------------------------
# Module entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Offer Intel scrape pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Scrape all platforms once and exit (no scheduler loop)",
    )
    args = parser.parse_args()

    scheduler = PipelineScheduler()
    if args.once:
        scheduler.run_once()
    else:
        scheduler.start(run_immediately=True)
