# discovery/scorer.py
# ─────────────────────────────────────────────────────────────
# Sends each ProfileCandidate to GPT-4o for structured scoring.
# Returns a ScoredProfile with a 0-100 confidence score and
# metadata used to write the final MongoDB document.
# ─────────────────────────────────────────────────────────────

from __future__ import annotations
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from openai import OpenAI

from .apify_client import ProfileCandidate
from .config import (
    OPENAI_API_KEY,
    GPT_MODEL,
    GPT_TEMPERATURE,
    GPT_MAX_TOKENS,
    MIN_SCORE_TO_STORE,
    WEIGHT_LOCATION,
    WEIGHT_BUSINESS,
    WEIGHT_ACTIVITY,
    Region,
    REGIONS,
)

log = logging.getLogger(__name__)

_client: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise EnvironmentError("OPENAI_API_KEY is not set.")
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


# ─────────────────────────────────────────────────────────────
#  Output shape
# ─────────────────────────────────────────────────────────────
@dataclass
class ScoredProfile:
    candidate: ProfileCandidate

    # GPT-4o scores (each 0-100)
    location_score: int = 0     # how confident location matches target region
    business_score: int = 0     # how likely this is a commercial/promo account
    activity_score: int = 0     # how actively posting offers/deals

    # Weighted composite
    final_score: int = 0

    # GPT-4o derived metadata
    detected_location: str = ""         # city / country as understood by model
    business_type: str = ""             # e.g. "clothing store", "supermarket"
    language: str = ""                  # detected posting language
    recommended: bool = False           # final accept/reject

    # Rejection reason when recommended=False
    reject_reason: str = ""

    def to_scrape_target(self) -> dict:
        """Return the MongoDB document ready to upsert into scrape_targets."""
        c = self.candidate
        return {
            # ── core fields — same as human-added profiles ────
            "platform": c.platform,
            "target_type": "profile",
            "value": c.username,
            "active": False,  # pending human review
            "added_by": "discovery",
            "last_scraped": None,  # set by scraper after first scrape
            # ── discovery-only metadata ───────────────────────
            "discovery_meta": {
                "region": c.region_name,
                "discovery_hashtag": c.discovery_hashtag,
                "final_score": self.final_score,
                "location_score": self.location_score,
                "business_score": self.business_score,
                "activity_score": self.activity_score,
                "detected_location": self.detected_location,
                "business_type": self.business_type,
                "language": self.language,
                "profile_url": c.profile_url,
            },
        }


# ─────────────────────────────────────────────────────────────
#  System + user prompt templates
# ─────────────────────────────────────────────────────────────
def _build_system_prompt(region, platform: str) -> str:
    country_names = ", ".join(region.country_names)
    cities        = ", ".join(region.cities)
    return f"""
You are a business-profile classifier for a social-media offer aggregation system.
You will receive data scraped from {platform} — not a full profile.
The data includes the account display name, username, hashtags, and all post captions.

Your job is to determine:
1. Whether the account is located in the target geographic region.
2. Whether the account is a business posting promotional offers / deals / discounts.
3. How actively it posts commercial content.

TARGET REGION: {region.name}
- Country names to look for: {country_names}
- Cities to look for: {cities}

HOW TO FIND LOCATION — check all three sources in order:
1. location_text — contains hashtags from posts, look for country or city names inside them
2. recent_captions — read the FULL caption, location is often mentioned at the end
3. If still unsure — check the raw field which contains the complete original post data

You MUST respond ONLY with a single valid JSON object — no markdown, no explanation.
Use this exact schema:

{{
  "location_score":   <int 0-100>,
  "business_score":   <int 0-100>,
  "activity_score":   <int 0-100>,
  "detected_location": "<detected city or country, or 'unknown'>",
  "business_type":    "<e.g. clothing store, supermarket, restaurant, or 'personal account'>",
  "language":         "<primary language detected>",
  "recommended":      <true | false>,
  "reject_reason":    "<empty string if recommended=true, else brief reason>"
}}

Scoring guide:
- location_score  100 = country or city name found in captions, hashtags, or raw data; 50 = language/dialect match only, no explicit place name; 0 = no signal or clearly a different country.
- business_score  100 = store/shop posting prices and deals; 50 = occasional product posts; 0 = personal or non-commercial account.
- activity_score  100 = multiple captions with prices/offers/discounts; 50 = some commercial captions; 0 = no commercial content.
- recommended = true only when location_score >= 40 AND account appears to be a business posting offers.
""".strip()


def _build_user_prompt(candidate: ProfileCandidate, target_region: str) -> str:
    captions_text = "\n---\n".join(candidate.recent_captions) or "N/A"
    import json
    raw_text = json.dumps(candidate.raw, ensure_ascii=False)[:2000]
    return f"""
Target region: {target_region}
Platform: {candidate.platform}
Username: {candidate.username}
Display name: {candidate.display_name}
Hashtags from posts: {candidate.location_text or "N/A"}
Discovery hashtag: {candidate.discovery_hashtag or "N/A"}

Post captions (read fully — location is often at the end):
{captions_text}

Raw post data (use if unsure about location):
{raw_text}
""".strip()


# ─────────────────────────────────────────────────────────────
#  Main scorer
# ─────────────────────────────────────────────────────────────
class ProfileScorer:

    def __init__(self, region: Region):
        self.region = region

    def score(self, candidate: ProfileCandidate) -> ScoredProfile:
        """
        Pre-filter cheaply before hitting GPT-4o.
        Returns a ScoredProfile (recommended=False if pre-filtered).
        """
        sp = ScoredProfile(candidate=candidate)

        # ── cheap pre-filters ────────────────────────────────
        if not candidate.username:
            sp.reject_reason = "empty username"
            return sp

        # ── GPT-4o scoring ───────────────────────────────────
        try:
            raw = self._call_gpt(candidate, self.region)
        except Exception as exc:
            log.error("GPT scoring failed for @%s: %s", candidate.username, exc)
            sp.reject_reason = f"scoring error: {exc}"
            return sp

        # ── parse GPT response ───────────────────────────────
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("GPT returned non-JSON for @%s: %s", candidate.username, raw[:200])
            sp.reject_reason = "gpt json parse error"
            return sp

        sp.location_score   = int(data.get("location_score", 0))
        sp.business_score   = int(data.get("business_score", 0))
        sp.activity_score   = int(data.get("activity_score", 0))
        sp.detected_location = data.get("detected_location", "")
        sp.business_type    = data.get("business_type", "")
        sp.language         = data.get("language", "")
        sp.reject_reason    = data.get("reject_reason", "")

        # ── weighted composite ───────────────────────────────
        sp.final_score = round(
            sp.location_score * WEIGHT_LOCATION
            + sp.business_score * WEIGHT_BUSINESS
            + sp.activity_score * WEIGHT_ACTIVITY
        )

        # ── final accept gate ────────────────────────────────
        sp.recommended = (
            bool(data.get("recommended", False))
            and sp.final_score >= MIN_SCORE_TO_STORE
        )
        if not sp.recommended and not sp.reject_reason:
            sp.reject_reason = f"final score {sp.final_score} < threshold {MIN_SCORE_TO_STORE}"

        log.info(
            "[score] @%s (%s) → loc=%d biz=%d act=%d final=%d recommended=%s",
            candidate.username, candidate.platform,
            sp.location_score, sp.business_score, sp.activity_score,
            sp.final_score, sp.recommended,
        )
        return sp

    def score_batch(
        self, candidates: list[ProfileCandidate], delay: float = 0.5
    ) -> list[ScoredProfile]:
        """Score a list of candidates, returning all ScoredProfiles."""
        results = []
        total = len(candidates)
        for i, c in enumerate(candidates, 1):
            log.info("Scoring %d/%d: @%s", i, total, c.username)
            results.append(self.score(c))
            if i < total:
                time.sleep(delay)   # respect rate limits
        return results

    # ── internal ─────────────────────────────────────────────

    def _call_gpt(self, candidate: ProfileCandidate, region: Region) -> str:
        response = _get_client().chat.completions.create(
            model=GPT_MODEL,
            temperature=GPT_TEMPERATURE,
            max_tokens=GPT_MAX_TOKENS,
            messages=[
                {"role": "system", "content": _build_system_prompt(region, candidate.platform)},
                {"role": "user",   "content": _build_user_prompt(candidate, region.name)},
            ],
        )
        return response.choices[0].message.content.strip()