"""
offer_intel.extraction.offer_extractor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
LLM-powered extraction of structured promotional data from raw social-media
captions.

The extractor uses OpenAI as the primary provider and Groq as a fallback.
Both provider configs (model, temperature, token limits) are read dynamically
from ``config/llm_config.json`` so you never need to touch the code to swap
models.

Extracted offers are stored in the ``offers`` collection and, if the
confidence score passes the threshold, promoted to ``offers_public``.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timedelta

import pymongo
from dateutil import parser as dateutil_parser
from groq import Groq
from openai import OpenAI
from pymongo import ASCENDING, MongoClient

from offer_intel.utils.settings import settings, config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public Offer Builder
# ---------------------------------------------------------------------------

class PublicOfferBuilder:
    """
    Promotes a normalised offer to ``offers_public`` when its confidence score
    is above the threshold.  The operation is idempotent — calling it twice for
    the same offer is safe.
    """

    def __init__(self, offers_col, offers_public_col) -> None:
        self._offers = offers_col
        self._public = offers_public_col
        self._threshold = settings.PUBLIC_CONFIDENCE_THRESHOLD
        self._lifetime_days = settings.PUBLIC_LIFETIME_DAYS
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._public.create_index(
                [("internal_offer_id", ASCENDING)],
                unique=True,
                name="unique_internal_offer_id",
            )
            self._public.create_index(
                [("expire_at", ASCENDING)],
                expireAfterSeconds=0,
                name="ttl_expire_at",
            )
        except Exception as exc:
            logger.debug("offers_public index notice: %s", exc)

    def build_from_offer(self, post_id: str) -> dict | None:
        """Fetch the offer by *post_id* and insert it into the public collection."""
        raw = self._offers.find_one({"post_id": post_id})
        if not raw:
            logger.warning("PublicOfferBuilder: offer not found for post_id=%s", post_id)
            return None

        confidence = raw.get("confidence_score", 0)
        if confidence < self._threshold:
            logger.debug(
                "Offer skipped for public (confidence %.2f < %.2f)", confidence, self._threshold
            )
            return None

        existing = self._public.find_one({"internal_offer_id": post_id})
        if existing:
            return existing

        now = datetime.utcnow()
        doc = {
            "internal_offer_id": post_id,
            "public_id": uuid.uuid4().hex[:12],
            "title": raw.get("offer_title"),
            "brand_name": raw.get("brand_name"),
            "platform": raw.get("platform"),
            "category": raw.get("product_category"),
            "images": raw.get("post_images", []),
            "video": raw.get("post_video", []),
            "promo_code": raw.get("promo_code"),
            "discounts": raw.get("normalized_fields", {}).get("discounts", []),
            "valid_from": raw.get("normalized_fields", {}).get("valid_from"),
            "valid_until": raw.get("normalized_fields", {}).get("valid_until"),
            "location": raw.get("normalized_fields", {}).get("location"),
            "tags": raw.get("tags", []),
            "language": raw.get("language"),
            "confidence_score": confidence,
            "is_active": True,
            "created_at": now,
            "expire_at": now + timedelta(days=self._lifetime_days),
        }

        try:
            self._public.insert_one(doc)
            logger.info(
                "Published to offers_public: [%s] %s (confidence: %.2f)",
                doc["public_id"], doc.get("title", "N/A"), confidence,
            )
            return doc
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return self._public.find_one({"internal_offer_id": post_id})
            logger.error("Failed to publish offer: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Offer Extractor
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT_TEMPLATE = """\
You are a commercial intelligence extraction assistant for a Middle East–focused \
social media analytics platform.

Your task is to EXTRACT structured promotional information from social media captions.
Do NOT normalize, enrich, translate, or guess missing information.

Captions may be written in Arabic (Saudi, Yemeni, Egyptian dialects), English, or mixed language.
Captions may contain emojis, hype language, slang, or informal expressions.

CRITICAL EXTRACTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━
1. Extract ONLY information explicitly stated in the caption.
2. DO NOT guess or infer missing details.
3. DO NOT infer country, city, or region from dialect, brand, or emojis.
4. DO NOT treat venue names (farms, malls, cafes, halls, shops) as geographic locations.
5. If a field is missing, unclear, or ambiguous → return null.
6. Preserve the original wording exactly as written.
7. Return ONLY valid JSON (no explanations, no markdown).

FIELDS TO RETURN (JSON)
━━━━━━━━━━━━━━━━━━━━━━━
offer_title, discount_value, valid_from, valid_until, location, language,
is_active, promo_code, offer_type, tags, image_alt_text, confidence_score,
brand_name, product_category

ALLOWED offer_type VALUES (choose one or null)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
percentage_discount, flat_discount, promo_code, bundle_offer, limited_time_offer,
flash_sale, category_discount, product_discount, free_shipping, product_listing,
event_promo, location_offer, giveaway, loyalty_offer, announcement

FALLBACK
If the caption is empty, irrelevant, or non-promotional:
  All fields null except confidence_score: 0.0 and is_active: false

CAPTION:
"{description}"
"""


class OfferExtractor:
    """
    Extracts structured offer data from social-media captions using an LLM.

    The class owns its own MongoDB connection (``offers`` + ``offers_public``
    collections) and the :class:`PublicOfferBuilder` instance.
    """

    def __init__(
        self,
        mongo_uri: str | None = None,
        db_name: str | None = None,
    ) -> None:
        uri = mongo_uri or settings.MONGO_URI
        db = db_name or settings.MONGO_DB_NAME

        self._mongo = MongoClient(uri)
        _db = self._mongo[db]
        self.offers_collection = _db["offers"]
        self.offers_public_collection = _db["offers_public"]

        try:
            self.offers_collection.create_index(
                [("post_id", ASCENDING)], unique=True, name="unique_post_id"
            )
        except Exception as exc:
            logger.debug("Index notice: %s", exc)

        self._openai = OpenAI(api_key=settings.OPENAI_API_KEY)
        self._groq = Groq(api_key=settings.GROQ_API_KEY)

        self.public_builder = PublicOfferBuilder(
            self.offers_collection, self.offers_public_collection
        )
        logger.info("OfferExtractor initialised")

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(content: str) -> str:
        content = content.strip()
        content = re.sub(r"^```[\w]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content)
        return content.strip()

    def extract_offer(self, description: str) -> dict:
        """
        Run LLM extraction on *description*.

        Returns a dict with at minimum ``extracted_by_llm``, ``confidence_score``,
        and ``is_active``.
        """
        if not description or not description.strip():
            return {"extracted_by_llm": False, "confidence_score": 0.0, "is_active": False}

        prompt = _EXTRACTION_PROMPT_TEMPLATE.format(description=description)
        llm_providers = config.llm.get("providers", {})

        # 1️⃣  OpenAI
        try:
            cfg = llm_providers["openai"]
            gen = cfg["generation_config"]
            resp = self._openai.chat.completions.create(
                model=cfg["model"],
                messages=[{"role": "user", "content": prompt}],
                temperature=gen.get("temperature", 0.2),
                top_p=gen.get("top_p", 1.0),
                max_tokens=gen.get("max_output_tokens", 800),
            )
            offer = json.loads(self._clean(resp.choices[0].message.content))
            offer.update({"extracted_by_llm": True, "llm_provider": "openai", "llm_model": cfg["model"]})
            return offer
        except Exception as exc:
            logger.warning("OpenAI extraction failed (%s); falling back to Groq…", exc)

        # 2️⃣  Groq fallback
        try:
            cfg = llm_providers["groq"]
            gen = cfg["generation_config"]
            resp = self._groq.chat.completions.create(
                model=cfg["model"],
                messages=[{"role": "user", "content": prompt}],
                temperature=gen.get("temperature", 0.2),
                top_p=gen.get("top_p", 1.0),
                max_tokens=gen.get("max_output_tokens", 800),
            )
            offer = json.loads(self._clean(resp.choices[0].message.content))
            offer.update({"extracted_by_llm": True, "llm_provider": "groq", "llm_model": cfg["model"]})
            return offer
        except Exception as exc:
            logger.error("Groq extraction also failed: %s", exc)

        return {"extracted_by_llm": False, "confidence_score": 0.0, "is_active": False}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_scraped_at(raw) -> datetime:
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            try:
                return dateutil_parser.parse(raw)
            except (ValueError, TypeError):
                pass
        return datetime.utcnow()

    def save_offer(
        self,
        *,
        post_id: str,
        source: str,
        platform: str,
        profile: str,
        post_text: str,
        post_images: list[str],
        post_video: list[str],
        scraped_at,
        offer_data: dict,
    ) -> bool:
        """Insert the extracted offer document into MongoDB."""
        doc = {
            "post_id": post_id,
            "source": source,
            "platform": platform,
            "profile": profile,
            "post_text": post_text,
            "post_images": post_images,
            "post_video": post_video,
            **offer_data,
            "scraped_at": self._parse_scraped_at(scraped_at),
        }
        try:
            self.offers_collection.insert_one(doc)
            if offer_data.get("extracted_by_llm"):
                score = offer_data.get("confidence_score", 0)
                logger.info(
                    "Offer saved: %s (confidence: %.2f)", offer_data.get("offer_title", "N/A"), score
                )
            return True
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                logger.debug("Offer already exists: %s", post_id)
            else:
                logger.error("Failed to save offer (%s): %s", post_id, exc)
            return False
