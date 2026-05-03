"""
offer_intel.scraping.scrapers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Async scraper implementations for Apify and Bright Data, plus a router that
tries Apify first and falls back to Bright Data automatically.

Usage
-----
    from offer_intel.scraping.scrapers import ScraperRouter

    router = ScraperRouter("instagram")
    results, source = await router.scrape_profiles(["username1", "username2"])
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod

import aiohttp
from apify_client import ApifyClient

from offer_intel.utils.settings import settings, config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class SocialScraper(ABC):
    results_limit: int = 20

    @abstractmethod
    async def scrape_profiles(self, profiles: list[str], results_limit: int | None = None) -> list[dict]:
        ...


# ---------------------------------------------------------------------------
# Apify
# ---------------------------------------------------------------------------

class ApifyScraper(SocialScraper):
    """Wraps the synchronous Apify SDK in an async executor."""

    _INPUT_KEYS: dict[str, str] = {
        "tiktok": "profiles",
        "apify_tiktok": "profiles",
        "instagram": "usernames",
        "apify_instagram": "usernames",
        "instagram_post": "directUrls",
        "facebook": "startUrls",
        "facebook_post": "startUrls",
    }

    def __init__(
        self,
        api_token: str,
        actor_id: str,
        url_template: str,
        platform: str,
    ) -> None:
        self._client = ApifyClient(api_token)
        self._actor_id = actor_id
        self._url_template = url_template
        self._platform = platform

    def _build_run_input(self, profiles: list[str], results_limit: int) -> dict:
        if self._platform == "instagram_post":
            return {
                "directUrls": [self._url_template.format(username=p) for p in profiles],
                "resultsLimit": results_limit,
            }
        if self._platform in ("facebook", "facebook_post"):
            return {
                "startUrls": [{"url": self._url_template.format(username=p)} for p in profiles],
                "resultsLimit": results_limit,
            }
        key = self._INPUT_KEYS.get(self._platform, "profiles")
        return {
            key: profiles,
            "resultsLimit": results_limit,
            "shouldDownloadVideos": True,
            "shouldDownloadSubtitles": True,
        }

    async def scrape_profiles(self, profiles: list[str], results_limit: int | None = None) -> list[dict]:
        limit = results_limit or self.results_limit
        run_input = self._build_run_input(profiles, limit)
        loop = asyncio.get_event_loop()
        run = await loop.run_in_executor(
            None,
            lambda: self._client.actor(self._actor_id).call(run_input=run_input),
        )
        return self._client.dataset(run["defaultDatasetId"]).list_items().items


# ---------------------------------------------------------------------------
# Bright Data
# ---------------------------------------------------------------------------

class BrightDataScraper(SocialScraper):
    """Async Bright Data dataset scraper with polling."""

    _BASE_URL = "https://api.brightdata.com/datasets/v3"
    _POLL_WAIT_SECONDS = 10
    _MAX_RETRIES = 40

    def __init__(self, api_token: str, dataset_id: str, url_template: str) -> None:
        self._token = api_token
        self._dataset_id = dataset_id
        self._url_template = url_template

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    async def _trigger(self, session: aiohttp.ClientSession, profile_urls: list[str]) -> str:
        url = f"{self._BASE_URL}/trigger?dataset_id={self._dataset_id}&format=json"
        async with session.post(url, headers=self._headers(), json=[{"url": u} for u in profile_urls]) as resp:
            if resp.status != 200:
                raise RuntimeError(f"BrightData trigger failed: {resp.status} {await resp.text()}")
            data = await resp.json()
            return data["snapshot_id"]

    async def _poll(self, session: aiohttp.ClientSession, snapshot_id: str) -> list[dict]:
        url = f"{self._BASE_URL}/snapshot/{snapshot_id}?format=json"
        for attempt in range(1, self._MAX_RETRIES + 1):
            logger.debug("BrightData poll attempt %d/%d", attempt, self._MAX_RETRIES)
            await asyncio.sleep(self._POLL_WAIT_SECONDS)
            async with session.get(url, headers=self._headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        return data
        raise TimeoutError("BrightData polling timed out")

    async def scrape_profiles(self, profiles: list[str], results_limit: int | None = None) -> list[dict]:
        if isinstance(profiles, str):
            profiles = [profiles]
        urls = [self._url_template.format(username=p) for p in profiles]
        async with aiohttp.ClientSession() as session:
            snapshot_id = await self._trigger(session, urls)
            return await self._poll(session, snapshot_id)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class ScraperFactory:
    """Creates a configured scraper instance for a given provider + platform."""

    @staticmethod
    def create(provider: str, platform: str) -> SocialScraper:
        scraper_cfg = config.scraper
        provider_cfg = scraper_cfg.get("providers", {}).get(provider)
        if not provider_cfg:
            raise ValueError(f"Unknown scraper provider: '{provider}'")

        platform_cfg = provider_cfg.get("platforms", {}).get(platform)
        if not platform_cfg:
            raise ValueError(f"No config for platform '{platform}' under provider '{provider}'")

        global_limit = scraper_cfg.get("global_settings", {}).get("default_results_limit", 20)
        results_limit = platform_cfg.get("results_limit", global_limit)

        if provider == "brightdata":
            scraper: SocialScraper = BrightDataScraper(
                api_token=settings.BRIGHTDATA_API_TOKEN,
                dataset_id=platform_cfg["dataset_id"],
                url_template=platform_cfg["url_template"],
            )
        elif provider == "apify":
            scraper = ApifyScraper(
                api_token=settings.APIFY_API_TOKEN,
                actor_id=platform_cfg["actor_id"],
                url_template=platform_cfg["url_template"],
                platform=platform,
            )
        else:
            raise ValueError(f"Unsupported provider: '{provider}'")

        scraper.results_limit = results_limit
        return scraper


# ---------------------------------------------------------------------------
# Router  (Apify → Bright Data fallback)
# ---------------------------------------------------------------------------

class ScraperRouter:
    """
    Tries Apify first for a given platform; on failure falls back to Bright Data.

    Parameters
    ----------
    platform:
        Social platform key (e.g. ``"tiktok"``, ``"instagram"``, ``"facebook"``).
    """

    def __init__(self, platform: str) -> None:
        self._platform = platform
        scraper_cfg = config.scraper
        providers = scraper_cfg.get("providers", {})

        self._apify: SocialScraper | None = None
        if "apify" in providers and platform in providers["apify"].get("platforms", {}):
            self._apify = ScraperFactory.create("apify", platform)

        self._brightdata: SocialScraper | None = None
        if "brightdata" in providers and platform in providers["brightdata"].get("platforms", {}):
            self._brightdata = ScraperFactory.create("brightdata", platform)

    async def scrape_profiles(
        self,
        profiles: list[str],
        results_limit: int | None = None,
    ) -> tuple[list[dict], str]:
        """
        Scrape *profiles* and return ``(results, source_name)``.
        ``source_name`` is ``"apify"``, ``"brightdata"``, or ``"none"``.
        """
        if self._apify:
            try:
                logger.info("Trying Apify for %s…", self._platform)
                limit = results_limit or getattr(self._apify, "results_limit", 20)
                results = await self._apify.scrape_profiles(profiles, limit)
                return results, "apify"
            except Exception as exc:
                logger.warning("Apify failed for %s: %s", self._platform, exc)

        if self._brightdata:
            try:
                logger.info("Falling back to Bright Data for %s…", self._platform)
                results = await self._brightdata.scrape_profiles(profiles, results_limit)
                return results, "brightdata"
            except Exception as exc:
                logger.error("Bright Data also failed for %s: %s", self._platform, exc)

        logger.error("No scraper succeeded for %s", self._platform)
        return [], "none"
