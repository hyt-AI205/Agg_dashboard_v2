# discovery/apify_client.py
# ─────────────────────────────────────────────────────────────
# Apify scraping layer for Instagram, TikTok, and Facebook.
#
# Instagram  → apify/instagram-hashtag-scraper
# TikTok     → clockworks/tiktok-hashtag-scraper
# Facebook   → apify/facebook-search-scraper
#
# Flow (Instagram + TikTok):
#   1. Scrape posts under region hashtags
#   2. Aggregate all posts per username
#   3. Return one ProfileCandidate per unique username
#
# Flow (Facebook):
#   1. Search pages by keyword × city pairs
#   2. Return one ProfileCandidate per unique pageName
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional

from apify_client import ApifyClient

from .config import (
    APIFY_API_TOKEN,
    ACTOR_INSTAGRAM_HASHTAG,
    ACTOR_TIKTOK_HASHTAG,
    ACTOR_FACEBOOK_SEARCH,
    MAX_RESULTS_PER_TAG,
    Region,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Normalised candidate
# ─────────────────────────────────────────────────────────────
@dataclass
class ProfileCandidate:
    platform: str
    username: str
    display_name: str = ""
    profile_url: str = ""
    recent_captions: list[str] = field(default_factory=list)
    location_text: str = ""         # hashtags or address joined as string
    discovery_hashtag: str = ""     # keyword/hashtag that surfaced this account
    region_name: str = ""
    raw: dict = field(default_factory=dict, repr=False)


# ─────────────────────────────────────────────────────────────
#  Client
# ─────────────────────────────────────────────────────────────
class ApifyDiscoveryClient:
    def __init__(self):
        if not APIFY_API_TOKEN:
            raise EnvironmentError("APIFY_API_TOKEN is not set.")
        self._client = ApifyClient(APIFY_API_TOKEN)

    # ── Instagram ────────────────────────────────────────────

    def discover_instagram(
        self,
        region: Region,
        max_per_tag: int = MAX_RESULTS_PER_TAG,
        limit: Optional[int] = None,
    ) -> list[ProfileCandidate]:
        """
        Single actor call → aggregate posts per username → ProfileCandidates.
        """
        log.info("[Instagram] Scraping %d hashtags (max %d results each) …",
                 len(region.hashtags), max_per_tag)

        raw_items = self._run_actor(ACTOR_INSTAGRAM_HASHTAG, {
            "hashtags":     region.hashtags,
            "resultsLimit": max_per_tag,
        })
        log.info("[Instagram] Actor returned %d post items", len(raw_items))

        aggregated: dict[str, dict] = {}

        for item in raw_items:
            username = item.get("ownerUsername") or ""
            if not username:
                continue

            caption  = item.get("caption") or item.get("text") or ""
            hashtags = item.get("hashtags") or []

            if username not in aggregated:
                aggregated[username] = {
                    "display_name":  item.get("ownerFullName", ""),
                    "captions":      [],
                    "hashtags":      set(),
                    "discovery_tag": (item.get("hashtags") or [""])[0],
                    "raw":           item,
                }

            if caption:
                aggregated[username]["captions"].append(caption)
            for tag in hashtags:
                aggregated[username]["hashtags"].add(tag)

        log.info("[Instagram] %d unique accounts found", len(aggregated))

        if limit:
            aggregated = dict(list(aggregated.items())[:limit])
            log.info("[Instagram] Test limit applied → %d accounts", limit)

        candidates = []
        for username, data in aggregated.items():
            hashtag_str = " ".join(f"#{t}" for t in data["hashtags"])
            candidates.append(ProfileCandidate(
                platform="instagram",
                username=username,
                display_name=data["display_name"],
                profile_url=f"https://instagram.com/{username}",
                recent_captions=data["captions"],
                location_text=hashtag_str,
                discovery_hashtag=data["discovery_tag"],
                region_name=region.name,
                raw=data["raw"],
            ))

        log.info("[Instagram] %d candidates ready for scoring", len(candidates))
        return candidates

    # ── TikTok ───────────────────────────────────────────────

    def discover_tiktok(
        self,
        region: Region,
        max_per_tag: int = MAX_RESULTS_PER_TAG,
        limit: Optional[int] = None,
    ) -> list[ProfileCandidate]:
        """
        Single actor call → aggregate posts per username → ProfileCandidates.
        TikTok post items include full authorMeta so no enrichment needed.
        """
        log.info("[TikTok] Scraping %d hashtags (max %d results each) …",
                 len(region.hashtags), max_per_tag)

        raw_items = self._run_actor(ACTOR_TIKTOK_HASHTAG, {
            "hashtags":       region.hashtags,
            "resultsPerPage": max_per_tag,
        })
        log.info("[TikTok] Actor returned %d post items", len(raw_items))

        aggregated: dict[str, dict] = {}

        for item in raw_items:
            author   = item.get("authorMeta") or {}
            username = author.get("name") or ""
            if not username:
                continue

            text     = item.get("text") or ""
            hashtags = item.get("hashtags") or []

            # hashtags is a list of dicts: [{"name": "..."}]
            tag_names = [
                h["name"] for h in hashtags
                if isinstance(h, dict) and h.get("name")
            ]

            if username not in aggregated:
                aggregated[username] = {
                    "nickName":      author.get("nickName", ""),
                    "captions":      [],
                    "hashtags":      set(),
                    "discovery_tag": tag_names[0] if tag_names else "",
                    "raw":           item,
                }

            if text:
                aggregated[username]["captions"].append(text)
            for tag in tag_names:
                aggregated[username]["hashtags"].add(tag)

        log.info("[TikTok] %d unique accounts found", len(aggregated))

        if limit:
            aggregated = dict(list(aggregated.items())[:limit])
            log.info("[TikTok] Test limit applied → %d accounts", limit)

        candidates = []
        for username, data in aggregated.items():
            hashtag_str = " ".join(f"#{t}" for t in data["hashtags"])
            candidates.append(ProfileCandidate(
                platform="tiktok",
                username=username,
                display_name=data["nickName"],
                profile_url=f"https://tiktok.com/@{username}",
                recent_captions=data["captions"],
                location_text=hashtag_str,
                discovery_hashtag=data["discovery_tag"],
                region_name=region.name,
                raw=data["raw"],
            ))

        log.info("[TikTok] %d candidates ready for scoring", len(candidates))
        return candidates

    # ── Facebook ─────────────────────────────────────────────

    def discover_facebook(
        self,
        region: Region,
        max_per_keyword: int = MAX_RESULTS_PER_TAG,
        limit: Optional[int] = None,
    ) -> list[ProfileCandidate]:
        """
        Searches Facebook Pages by keyword × city pairs.
        Uses apify/facebook-search-scraper.

        Actor output fields used:
          pageName, pageUrl/facebookUrl, title, categories,
          info, about_me.text, address, phone, email,
          website, followers/likes
        """
        log.info("[Facebook] Searching %d keywords × %d cities …",
                 len(region.keywords), len(region.cities))

        # cap to avoid too many actor calls during testing
        locations = region.cities[:5]
        search_queries = [
            {"keyword": kw, "location": f"{city}, {region.country_names[0]}"}
            for kw in region.keywords[:8]
            for city in locations
        ]

        aggregated: dict[str, dict] = {}

        for query in search_queries:
            if limit and len(aggregated) >= limit:
                break

            log.info("[Facebook] '%s' in %s …", query["keyword"], query["location"])

            items = self._run_actor(ACTOR_FACEBOOK_SEARCH, {
                "categories":   [query["keyword"]],
                "locations":    [query["location"]],
                "resultsLimit": max_per_keyword,
            })

            for item in items:
                page_id  = item.get("pageId") or item.get("facebookId") or ""
                page_url = item.get("pageUrl") or item.get("facebookUrl") or ""
                if not page_id or page_id in aggregated:
                    continue
                # use pageName if clean, otherwise derive from pageId
                page_name = item.get("pageName") or page_id
                if page_name == "p":   # new-style FB URL has pageName="p"
                    page_name = page_id

                # info is a list of strings — use as captions
                info_texts = item.get("info") or []
                about_text = (item.get("about_me") or {}).get("text", "")
                intro_text = item.get("intro", "")
                captions   = [t for t in info_texts if t]
                if intro_text:
                    captions.append(intro_text)
                if about_text:
                    captions.append(about_text)

                # categories is a list — skip generic "Page" entry
                categories = [
                    c for c in (item.get("categories") or [])
                    if c.lower() != "page"
                ]

                aggregated[page_id] = {
                    "display_name":      item.get("title", ""),
                    "page_name":         page_name,
                    "page_url":          page_url,
                    "category":          ", ".join(categories),
                    "captions":          captions,
                    "address":           item.get("address", ""),
                    "phone":             item.get("phone", ""),
                    "email":             item.get("email", ""),
                    "website":           item.get("website", ""),
                    "followers":         item.get("followers", 0) or item.get("likes", 0),
                    "discovery_keyword": query["keyword"],
                    "discovery_city":    query["location"],
                    "raw":               item,
                }

        log.info("[Facebook] %d unique pages found", len(aggregated))

        if limit:
            aggregated = dict(list(aggregated.items())[:limit])
            log.info("[Facebook] Test limit → %d pages", limit)

        candidates = []
        for page_id, data in aggregated.items():
            location_parts = [p for p in [data["address"], data["discovery_city"]] if p]
            candidates.append(ProfileCandidate(
                platform="facebook",
                username=data["page_name"],
                display_name=data["display_name"],
                profile_url=data["page_url"],
                recent_captions=data["captions"],
                location_text=" | ".join(location_parts),
                discovery_hashtag=data["discovery_keyword"],
                region_name=region.name,
                raw=data["raw"],
            ))

        log.info("[Facebook] %d candidates ready for scoring", len(candidates))
        return candidates

    # ── Apify helper ─────────────────────────────────────────

    def _run_actor(self, actor_id: str, run_input: dict) -> list[dict]:
        try:
            run = self._client.actor(actor_id).call(run_input=run_input)
            items = list(
                self._client.dataset(run["defaultDatasetId"]).iterate_items()
            )
            log.debug("Actor %s → %d items", actor_id, len(items))
            return items
        except Exception as exc:
            log.error("Actor %s failed: %s", actor_id, exc)
            return []