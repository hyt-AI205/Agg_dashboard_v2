"""
offer_intel.normalization.platform_normalizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Converts raw scraper payloads into a consistent internal format.

Each platform has its own field-mapping schema defined in
``config/platform_config.json``.  The normalizer reads that schema at startup
and uses it to extract ``profile_id``, ``post_id``, ``post_text``,
``post_images``, and ``post_video`` from whatever the scraper returned.

Normalised posts are yielded as :class:`NormalisedPost` named-tuples so
consumers have typed, named access to the fields.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from offer_intel.utils.settings import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------

class NormalisedPost(NamedTuple):
    profile_id: str
    post_id: str
    post_text: str
    post_images: list[str]
    post_video: list[str]
    payload: dict  # enriched raw payload stored verbatim in MongoDB


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------

class PlatformNormalizer:
    """
    Normalises raw scraper payloads for all supported platforms.

    Parameters
    ----------
    schema_path:
        Optional override for the platform_config.json path.  If omitted the
        path is resolved automatically from ``offer_intel.utils.settings``.
    """

    def __init__(self, schema_path: Path | None = None) -> None:
        if schema_path is None:
            self.schemas: dict = config.platform
        else:
            with open(schema_path, encoding="utf-8") as fh:
                self.schemas = json.load(fh)

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_nested(data: dict, path: str):
        """Resolve a dot-separated key path against a nested dict."""
        for key in path.split("."):
            if not isinstance(data, dict):
                return None
            data = data.get(key)
            if data is None:
                return None
        return data or None

    @staticmethod
    def _to_list(value) -> list[str]:
        """Ensure a value is always a non-empty list of strings."""
        if not value:
            return []
        if isinstance(value, str):
            return [value] if value.strip() else []
        if isinstance(value, list):
            return [v for v in value if v]
        return []

    # ------------------------------------------------------------------
    # Facebook-specific helpers
    # ------------------------------------------------------------------

    def _facebook_post_id(self, post: dict) -> str:
        """Extract a stable unique ID from a Facebook post dict."""
        if post.get("postId"):
            return post["postId"]

        url: str = post.get("url", "")
        if url:
            if "/posts/" in url:
                candidate = url.split("/posts/")[-1].split("/")[0].split("?")[0]
                if candidate:
                    return candidate
            if "story_fbid=" in url:
                m = re.search(r"story_fbid=(\d+)", url)
                if m:
                    return m.group(1)
            if "/videos/" in url:
                candidate = url.split("/videos/")[-1].split("/")[0].split("?")[0]
                if candidate:
                    return f"video_{candidate}"
            clean = url.split("?")[0].replace("https://", "").replace("http://", "").replace("/", "_")
            return clean

        # Last resort — stable hash of content
        ts = post.get("time") or post.get("timestamp") or post.get("date", "")
        text = post.get("text", "")
        key = f"{ts}_{text[:50]}"
        return hashlib.md5(key.encode()).hexdigest()[:16]

    @staticmethod
    def _facebook_images(raw_payload: dict) -> list[str]:
        """Extract photo URIs from the Facebook media array."""
        return [
            item["image"]["uri"]
            for item in raw_payload.get("media", [])
            if item.get("__typename") == "Photo" and item.get("image", {}).get("uri")
        ]

    def _normalise_facebook(self, post: dict, profile_handle: str) -> list[NormalisedPost]:
        post_id = self._facebook_post_id(post)
        post_text = post.get("text", "")
        post_images = self._facebook_images(post)
        post_video = self._to_list(post.get("video"))

        page_info = post.get("pageInfo", {})
        profile_meta: dict = {
            "profile_handle": profile_handle,
            "platform": "facebook",
            **({
                "page_name": page_info.get("name"),
                "page_url": page_info.get("url"),
                "page_id": page_info.get("id"),
            } if page_info else {}),
        }

        enriched = {
            **post,
            "profile_meta": profile_meta,
            "post_text": post_text,
            "post_images": post_images,
            "post_video": post_video,
            "scraped_at": datetime.utcnow().isoformat(),
        }

        logger.debug("Facebook post_id extracted: %s", post_id)
        return [NormalisedPost(profile_handle, post_id, post_text, post_images, post_video, enriched)]

    # ------------------------------------------------------------------
    # Generic normalizer
    # ------------------------------------------------------------------

    def normalise(
        self,
        platform: str,
        raw_item: dict,
        profile_handle: str | None = None,
    ) -> list[NormalisedPost]:
        """
        Normalise a single raw scraper item for *platform*.

        Parameters
        ----------
        platform:
            One of the keys in ``platform_config.json`` (e.g. ``"instagram"``,
            ``"apify_tiktok"``, ``"facebook"`` …).
        raw_item:
            The raw dict returned by the scraper.
        profile_handle:
            The username / handle that was scraped (required for platforms
            where the handle is not embedded in the payload).

        Returns
        -------
        List of :class:`NormalisedPost`.  Most platforms return exactly one
        post per raw item; platforms with ``posts_key`` set to a list key may
        return multiple.
        """
        if platform in ("facebook", "facebook_post"):
            return self._normalise_facebook(raw_item, profile_handle or "unknown")

        schema = self.schemas.get(platform)
        if not schema:
            raise ValueError(f"No schema defined for platform: '{platform}'")

        profile_id = self._get_nested(raw_item, schema["profile_id"]) or "unknown"
        posts_key: str = schema["posts_key"]
        post_id_key: str = schema["post_id"]
        post_text_key: str | None = schema.get("post_text_key")
        post_images_key: str | None = schema.get("post_images_key")
        post_video_key: str | None = schema.get("post_video_key")

        posts: list[dict] = (
            [raw_item] if posts_key == "__single__" else (self._get_nested(raw_item, posts_key) or [])
        )

        # Build profile-level metadata once
        profile_meta: dict = {}
        for target_field, source_field in schema.get("profile_meta", {}).items():
            if isinstance(source_field, list):
                for field_path in source_field:
                    val = self._get_nested(raw_item, field_path)
                    if val:
                        profile_meta[target_field] = val
                        break
            else:
                profile_meta[target_field] = self._get_nested(raw_item, source_field)

        results: list[NormalisedPost] = []
        for post in posts:
            post_id = post.get(post_id_key)
            if not post_id:
                continue

            post_text = post.get(post_text_key, "") if post_text_key else ""
            post_images = self._to_list(post.get(post_images_key) if post_images_key else None)
            post_video = self._to_list(post.get(post_video_key) if post_video_key else None)

            enriched = {
                **post,
                "profile_meta": profile_meta,
                "post_text": post_text,
                "post_images": post_images,
                "post_video": post_video,
                "scraped_at": datetime.utcnow().isoformat(),
            }
            results.append(
                NormalisedPost(profile_id, post_id, post_text, post_images, post_video, enriched)
            )

        return results
