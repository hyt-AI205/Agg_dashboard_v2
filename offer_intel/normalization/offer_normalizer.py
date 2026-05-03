"""
offer_intel.normalization.offer_normalizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Two-step LLM-based normalization of raw extracted offers.

Step 1 – Discounts + Location  (via ``normalize_discounts_and_location``)
Step 2 – Dates                 (via ``normalize_dates``)

Both steps try OpenAI first and fall back to Groq automatically.
Normalized data is written back into the same ``offers`` collection document
under a ``normalized_fields`` sub-document plus two top-level flags:

    is_normalized: true
    normalized_at: <datetime>
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

from bson import ObjectId
from groq import Groq
from openai import OpenAI
from pymongo import MongoClient

from offer_intel.utils.settings import settings, config

logger = logging.getLogger(__name__)


class OfferNormalizer:
    """
    Normalises raw offer documents stored in MongoDB.

    Parameters
    ----------
    mongo_uri / db_name:
        Override the defaults coming from environment variables.
    """

    def __init__(
        self,
        mongo_uri: str | None = None,
        db_name: str | None = None,
    ) -> None:
        llm_cfg = config.llm
        self._openai_cfg = llm_cfg["providers"]["openai"]
        self._groq_cfg = llm_cfg["providers"]["groq"]

        self._openai = OpenAI(api_key=settings.OPENAI_API_KEY)
        self._groq = Groq(api_key=settings.GROQ_API_KEY)

        uri = mongo_uri or settings.MONGO_URI
        db = db_name or settings.MONGO_DB_NAME
        self._col = MongoClient(uri)[db]["offers"]

        self._ensure_indexes()
        logger.info("OfferNormalizer initialised")

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_index([("is_normalized", 1)])
            self._col.create_index([("normalized_at", -1)])
            self._col.create_index([("normalized_fields.location.country_code", 1)])
            self._col.create_index([("normalized_fields.discounts.discount_type", 1)])
        except Exception as exc:
            logger.debug("Index notice: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_json_output(content: str) -> str:
        """Strip markdown fences that some models wrap JSON in."""
        content = content.strip()
        content = re.sub(r"^```[\w]*\n?", "", content)
        content = re.sub(r"\n?```$", "", content)
        return content.strip()

    @staticmethod
    def _clean_for_serialisation(data):
        """Recursively make MongoDB documents JSON-serialisable."""
        if isinstance(data, dict):
            return {k: OfferNormalizer._clean_for_serialisation(v) for k, v in data.items() if k != "_id"}
        if isinstance(data, list):
            return [OfferNormalizer._clean_for_serialisation(i) for i in data]
        if isinstance(data, ObjectId):
            return str(data)
        if isinstance(data, datetime):
            return data.isoformat()
        return data

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _discount_location_prompt(self, normalisation_input: dict) -> str:
        return f"""You are a DATA NORMALIZATION ASSISTANT for Middle East promotional content.

Your task: Normalize ONLY discount and location fields.
Do NOT normalize dates — they are handled separately.

INPUT:
{json.dumps(normalisation_input, ensure_ascii=False, indent=2)}

━━━━━━━━━━━━━━━━━━━━━━
DISCOUNT NORMALIZATION
━━━━━━━━━━━━━━━━━━━━━━
Return an ARRAY even for a single discount.

discount_type values: "percentage" | "fixed_amount" | "buy_x_get_y" | null
discount_currency:    "SAR" | "AED" | "EGP" | "YER" | "USD" | null

━━━━━━━━━━━━━━━━━━━━━━
LOCATION NORMALIZATION
━━━━━━━━━━━━━━━━━━━━━━
Extract from the location field ONLY. Never infer country from dialect.
city → always English (الرياض→Riyadh, جدة→Jeddah, صنعاء→Sanaa, عدن→Aden,
       القاهرة→Cairo, دبي→Dubai, أبوظبي→Abu Dhabi).
country → always English | country_code → ISO 2-letter.
Venue names (مزرعة, مول, قاعة) go into "venue", never "city".

━━━━━━━━━━━━━━━━━━━━━━
OUTPUT — valid JSON only, no markdown:
{{
  "discounts": [{{"discount_amount": null, "discount_currency": null, "discount_type": null}}],
  "location": {{"country": null, "country_code": null, "city": null, "venue": null}}
}}"""

    def _date_prompt(self, post_text: str, raw_from, raw_until) -> str:
        today = datetime.now()
        tomorrow = today + timedelta(days=1)
        return f"""You are a DATE NORMALIZATION EXPERT for Middle East promotional content.
Convert dates to ISO 8601 (YYYY-MM-DD). Return only JSON.

Today: {today.strftime('%Y-%m-%d')} ({today.strftime('%A')})
GCC work week: Sunday–Thursday. Weekend: Friday–Saturday.

Post text: "{post_text}"
valid_from:  {raw_from}
valid_until: {raw_until}

RULES
- Missing year → assume {today.year}
- "نهاية الأسبوع" / "آخر الأسبوع" → next Thursday
- "اليوم" → {today.strftime('%Y-%m-%d')}
- "غدًا" → {tomorrow.strftime('%Y-%m-%d')}
- "نهاية الشهر" → last day of current month
- Arabic numerals (٠١٢٣…) → Western (0123…)
- "حتى نفاد الكمية" alone → null (but extract accompanying date if present)
- "مستمر" / "دائم" → null

OUTPUT — valid JSON only:
{{"valid_from": "YYYY-MM-DD or null", "valid_until": "YYYY-MM-DD or null"}}"""

    # ------------------------------------------------------------------
    # LLM calls (OpenAI → Groq fallback)
    # ------------------------------------------------------------------

    def _call_llm(self, system: str, prompt: str, max_tokens: int = 600) -> str | None:
        """Call OpenAI; on failure fall back to Groq. Returns raw content string."""
        openai_model = self._openai_cfg["normalizer"]["model"]
        groq_model = self._groq_cfg["normalizer"]["model"]

        try:
            resp = self._openai.chat.completions.create(
                model=openai_model,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception as exc:
            logger.warning("OpenAI failed (%s); trying Groq…", exc)

        try:
            resp = self._groq.chat.completions.create(
                model=groq_model,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception as exc:
            logger.error("Groq also failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Normalisation steps
    # ------------------------------------------------------------------

    def normalise_discounts_and_location(self, raw_offer: dict) -> dict | None:
        normalisation_input = {
            "post_text": raw_offer.get("post_text"),
            "location": raw_offer.get("location"),
            "language": raw_offer.get("language"),
            "discount_value": raw_offer.get("discount_value"),
        }
        prompt = self._discount_location_prompt(normalisation_input)
        content = self._call_llm("You are a data normalisation expert. Return only valid JSON.", prompt)
        if not content:
            return None
        try:
            return json.loads(self._clean_json_output(content))
        except json.JSONDecodeError as exc:
            logger.error("JSON decode error (discount/location): %s", exc)
            return None

    def normalise_dates(self, post_text: str, raw_from, raw_until) -> dict:
        prompt = self._date_prompt(post_text, raw_from, raw_until)
        content = self._call_llm(
            "You are a date normalisation expert. Return only valid JSON with ISO dates.",
            prompt,
            max_tokens=200,
        )
        if not content:
            return {"valid_from": None, "valid_until": None}
        try:
            return json.loads(self._clean_json_output(content))
        except json.JSONDecodeError as exc:
            logger.error("JSON decode error (dates): %s", exc)
            return {"valid_from": None, "valid_until": None}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalise_offer(self, raw_offer: dict) -> dict | None:
        """
        Run both normalisation steps and merge the results.

        Returns a dict with keys: discounts, location, valid_from, valid_until,
        normalization_metadata.  Returns ``None`` on failure.
        """
        dl = self.normalise_discounts_and_location(raw_offer)
        if dl is None:
            logger.error("Discount/location normalisation failed for post_id=%s", raw_offer.get("post_id"))
            return None

        dates = self.normalise_dates(
            raw_offer.get("post_text", ""),
            raw_offer.get("valid_from"),
            raw_offer.get("valid_until"),
        )

        return {
            "discounts": dl.get("discounts"),
            "location": dl.get("location"),
            "valid_from": dates.get("valid_from"),
            "valid_until": dates.get("valid_until"),
            "normalization_metadata": {
                "provider": dl.get("normalization_provider"),
                "model": dl.get("normalization_model"),
            },
        }

    def save_normalised_offer(self, post_id: str, normalised: dict) -> bool:
        """Persist normalised fields back into the offers collection."""
        try:
            result = self._col.update_one(
                {"post_id": post_id},
                {
                    "$set": {
                        "normalized_fields": {
                            "discounts": normalised.get("discounts"),
                            "valid_from": normalised.get("valid_from"),
                            "valid_until": normalised.get("valid_until"),
                            "location": normalised.get("location"),
                            "normalization_metadata": normalised.get("normalization_metadata"),
                        },
                        "is_normalized": True,
                        "normalized_at": datetime.utcnow(),
                    }
                },
            )
            if result.modified_count:
                logger.info("Normalised offer saved: %s", post_id[:50])
                return True
            logger.warning("No document updated for post_id: %s", post_id)
            return False
        except Exception as exc:
            logger.error("Failed to save normalised offer (%s): %s", post_id, exc)
            return False

    def normalise_all_pending(self, limit: int | None = None) -> dict:
        """
        Normalise every offer that has been extracted by the LLM but not yet
        normalised.  Returns a summary dict with keys: total, normalized, failed.
        """
        query = {"extracted_by_llm": True, "is_normalized": {"$ne": True}}
        cursor = self._col.find(query)
        if limit:
            cursor = cursor.limit(limit)

        offers = list(cursor)
        logger.info("Found %d offers pending normalisation", len(offers))

        normalised_count = failed_count = 0
        for idx, offer in enumerate(offers, 1):
            post_id = offer.get("post_id", "")
            logger.info("[%d/%d] Normalising %s…", idx, len(offers), post_id[:50])
            try:
                data = self.normalise_offer(offer)
                if data and self.save_normalised_offer(post_id, data):
                    normalised_count += 1
                else:
                    failed_count += 1
            except Exception as exc:
                failed_count += 1
                logger.error("Exception normalising %s: %s", post_id, exc)

        logger.info(
            "Normalisation complete — ✅ %d  ❌ %d  total %d",
            normalised_count, failed_count, len(offers),
        )
        return {"total": len(offers), "normalized": normalised_count, "failed": failed_count}

    def normalise_by_platform(self, platform: str, limit: int | None = None) -> dict:
        """Normalise only offers from a specific platform."""
        query = {"extracted_by_llm": True, "platform": platform, "is_normalized": {"$ne": True}}
        cursor = self._col.find(query)
        if limit:
            cursor = cursor.limit(limit)
        offers = list(cursor)
        logger.info("Normalising %d offers for platform '%s'", len(offers), platform)

        normalised_count = failed_count = 0
        for offer in offers:
            post_id = offer.get("post_id", "")
            data = self.normalise_offer(offer)
            if data and self.save_normalised_offer(post_id, data):
                normalised_count += 1
            else:
                failed_count += 1
        return {"normalized": normalised_count, "failed": failed_count}
