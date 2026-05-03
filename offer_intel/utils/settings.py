"""
offer_intel.utils.settings
~~~~~~~~~~~~~~~~~~~~~~~~~~
Centralised settings loaded from environment variables (.env) and JSON config files.

Usage
-----
    from offer_intel.utils.settings import settings, config

    settings.OPENAI_API_KEY   # str
    config.llm                # dict parsed from llm_config.json
    config.scraper            # dict parsed from scraper_config.json
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
# The config directory lives at <repo_root>/config/.
# We walk up from this file: offer_intel/utils/settings.py → offer_intel/ → repo root
_PACKAGE_DIR = Path(__file__).resolve().parents[1]  # offer_intel/
_REPO_ROOT = _PACKAGE_DIR.parent                    # repo root
CONFIG_DIR = _PACKAGE_DIR / "config"

# Load .env from repo root (no-op if already loaded or file missing)
load_dotenv(_REPO_ROOT / ".env")


# ---------------------------------------------------------------------------
# Environment-backed settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    # MongoDB
    MONGO_URI: str = field(default_factory=lambda: os.environ["MONGO_URI"])
    MONGO_DB_NAME: str = field(default_factory=lambda: os.getenv("MONGO_DB_NAME", "offer_insights"))

    # LLM providers
    OPENAI_API_KEY: str = field(default_factory=lambda: os.environ["OPENAI_API_KEY"])
    GROQ_API_KEY: str = field(default_factory=lambda: os.environ["GROQ_API_KEY"])

    # Scraper providers
    APIFY_API_TOKEN: str = field(default_factory=lambda: os.getenv("APIFY_API_TOKEN", ""))
    BRIGHTDATA_API_TOKEN: str = field(default_factory=lambda: os.getenv("BRIGHTDATA_API_TOKEN", ""))

    # Pipeline tunables
    PUBLIC_CONFIDENCE_THRESHOLD: float = field(
        default_factory=lambda: float(os.getenv("PUBLIC_CONFIDENCE_THRESHOLD", "0.7"))
    )
    PUBLIC_LIFETIME_DAYS: int = field(
        default_factory=lambda: int(os.getenv("PUBLIC_LIFETIME_DAYS", "60"))
    )


def _load_json(filename: str) -> dict:
    path = CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            f"Make sure '{filename}' exists inside the 'config/' directory."
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# JSON-backed config
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    llm: dict = field(default_factory=lambda: _load_json("llm_config.json"))
    scraper: dict = field(default_factory=lambda: _load_json("scraper_config.json"))
    platform: dict = field(default_factory=lambda: _load_json("platform_config.json"))
    schedule: dict = field(default_factory=lambda: _load_json("schedule_config.json"))


# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------
settings = Settings()
config = Config()
