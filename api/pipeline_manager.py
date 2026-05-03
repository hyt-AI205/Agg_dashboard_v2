"""
api/pipeline_manager.py
───────────────────────
Singleton that owns the lifecycle of `offer_intel.pipeline.PipelineScheduler`.

Responsibilities
────────────────
• Start / stop one-shot or scheduled pipeline runs in a background thread.
• Capture log output and store the last N lines so the dashboard can poll them.
• Persist run history (in-memory + MongoDB optional).
• Read / write the four JSON config files used by offer_intel:
      offer_intel/config/scraper_config.json
      offer_intel/config/schedule_config.json
      offer_intel/config/llm_config.json
      offer_intel/config/platform_config.json

Thread safety
─────────────
All public methods acquire `_lock` before touching shared state.  The
pipeline itself runs in a daemon thread; asyncio work inside the pipeline
is handled with `asyncio.run()` (each platform gets its own event loop).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)
UTC = timezone.utc

# ── Config file locations ─────────────────────────────────────────────────
#   Paths are resolved relative to the project root (where main.py lives).
#   Adjust _CONFIG_DIR if offer_intel keeps its configs elsewhere.
_CONFIG_DIR = Path("offer_intel") / "config"

_CFG_FILES: dict[str, str] = {
    "scraper":  "scraper_config.json",
    "schedule": "schedule_config.json",
    "llm":      "llm_config.json",
    "platform": "platform_config.json",
}

# ── History / log settings ────────────────────────────────────────────────
_MAX_LOG_LINES  = 500   # rolling buffer kept in memory
_MAX_HISTORY    = 50    # run-history entries kept in memory


# ═════════════════════════════════════════════════════════════════════════════
#  Log handler that feeds the in-memory ring buffer
# ═════════════════════════════════════════════════════════════════════════════

class _RingBufferHandler(logging.Handler):
    """Captures log records into a fixed-size deque."""

    def __init__(self, buf: deque):
        super().__init__()
        self._buf = buf
        self.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        ))

    def emit(self, record: logging.LogRecord) -> None:
        level_map = {
            logging.DEBUG:    "dim",
            logging.INFO:     "info",
            logging.WARNING:  "warn",
            logging.ERROR:    "error",
            logging.CRITICAL: "error",
        }
        self._buf.append({
            "text":  self.format(record),
            "level": level_map.get(record.levelno, "info"),
            "ts":    datetime.now(UTC).isoformat(),
        })


# ═════════════════════════════════════════════════════════════════════════════
#  PipelineManager
# ═════════════════════════════════════════════════════════════════════════════

class PipelineManager:
    """
    Singleton wrapper around offer_intel's PipelineScheduler.

    Usage (from the FastAPI router)::

        mgr = get_pipeline_manager()
        mgr.run_now(platforms=["tiktok", "instagram"])
    """

    def __init__(self) -> None:
        self._lock        = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event  = threading.Event()

        # ── live state ──
        self._running     = False
        self._platforms:  list[str] = []
        self._mode        = "once"          # "once" | "scheduled"
        self._last_run_at: Optional[datetime] = None
        self._next_run_at: Optional[datetime] = None
        self._scheduler_active = False
        self._scheduler_timer: Optional[threading.Timer] = None

        # ── logging ──
        self._log_buf: deque = deque(maxlen=_MAX_LOG_LINES)
        self._log_cursor = 0   # how many lines the client has already seen
        self._handler = _RingBufferHandler(self._log_buf)

        # Attach to the root offer_intel logger so we capture everything
        oi_log = logging.getLogger("offer_intel")
        oi_log.addHandler(self._handler)
        oi_log.setLevel(logging.DEBUG)

        # Also attach to pipeline logger
        pl_log = logging.getLogger(__name__)
        pl_log.addHandler(self._handler)

        # ── run history ──
        self._history: deque = deque(maxlen=_MAX_HISTORY)  # in-memory cache for speed
        self._history_col = None  # set below
        try:
            from pymongo import MongoClient
            import os
            _mongo = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
            self._history_col = _mongo["social_scraper"]["pipeline_runs"]
            # index for fast latest-first queries
            self._history_col.create_index([("started_at", -1)])
        except Exception as e:
            log.warning("Pipeline history DB unavailable, using in-memory only: %s", e)

        # ── last aggregated counters (for quick status reads) ──
        self._last_result: dict = {}

        # ── schedule settings (persisted to schedule_config.json) ──
        self._schedule_settings: dict = self._load_schedule_settings()

    # ──────────────────────────────────────────────────────────────────────
    #  Public API
    # ──────────────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        """Return a snapshot of current pipeline state for polling."""
        with self._lock:
            # New log lines since last call
            buf_list   = list(self._log_buf)
            new_lines  = buf_list[self._log_cursor:]
            self._log_cursor = len(buf_list)

            return {
                "running":          self._running,
                "platforms":        list(self._platforms),
                "mode":             self._mode,
                "last_run_at":      self._last_run_at.isoformat() if self._last_run_at else None,
                "next_run_at":      self._next_run_at.isoformat() if self._next_run_at else None,
                "scheduler_active": self._scheduler_active,
                "last_result":      self._last_result,
                "new_logs":         new_lines,
            }

    def run_now(self, platforms: list[str], mode: str = "once") -> dict:
        """
        Trigger a pipeline run in a daemon thread.

        Returns immediately; callers should poll `get_status()`.
        """
        with self._lock:
            if self._running:
                return {"success": False, "message": "Pipeline is already running."}
            if not platforms:
                return {"success": False, "message": "No platforms specified."}

            self._platforms = list(platforms)
            self._mode = mode
            self._stop_event.clear()
            self._running = True

            if mode == "scheduled" and self._scheduler_active:
                from datetime import timedelta
                interval_h = self._schedule_settings.get("global_scrape_interval_hours", 6)
                self._next_run_at = datetime.now(UTC) + timedelta(hours=interval_h)
            else:
                self._next_run_at = None

        self._thread = threading.Thread(
            target=self._run_worker,
            args=(platforms, mode),
            daemon=True,
            name="pipeline-worker",
        )
        self._thread.start()
        self._append_log(f"▶ Pipeline started — platforms: {', '.join(platforms)} — mode: {mode}", "step")
        return {"success": True, "message": f"Pipeline started for {', '.join(platforms)}."}

    def stop(self) -> dict:
        """Signal the running pipeline to stop after the current platform finishes."""
        with self._lock:
            if not self._running:
                return {"success": False, "message": "Pipeline is not running."}
            self._stop_event.set()
            self._scheduler_active = False
            self._next_run_at = None
            if self._scheduler_timer and self._scheduler_timer.is_alive():
                self._scheduler_timer.cancel()
                self._scheduler_timer = None
        self._append_log("■ Stop signal sent — will finish current platform then halt.", "warn")
        return {"success": True, "message": "Stop signal sent."}

    # ── Schedule ──────────────────────────────────────────────────────────

    def get_schedule(self) -> dict:
        return dict(self._schedule_settings)

    def save_schedule(self, settings: dict) -> dict:
        """Persist schedule settings (merges with existing, writes to JSON)."""
        with self._lock:
            self._schedule_settings.update(settings)
            enabled = settings.get("enabled", self._schedule_settings.get("enabled", False))
            self._scheduler_active = bool(enabled)
            if not enabled:
                if self._scheduler_timer and self._scheduler_timer.is_alive():
                    self._scheduler_timer.cancel()
                    self._scheduler_timer = None
                self._next_run_at = None

        # Write schedule_config.json
        cfg_path = _CONFIG_DIR / _CFG_FILES["schedule"]
        try:
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "global_scrape_interval_hours": settings.get(
                    "global_scrape_interval_hours",
                    self._schedule_settings.get("global_scrape_interval_hours", 6),
                ),
                "per_platform_overrides": settings.get(
                    "per_platform_overrides",
                    self._schedule_settings.get("per_platform_overrides", {}),
                ),
            }
            cfg_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            log.info("schedule_config.json updated.")
        except Exception as exc:
            log.warning("Could not write schedule_config.json: %s", exc)

        return {"success": True, "settings": self._schedule_settings}

    # ── Config files ──────────────────────────────────────────────────────

    def get_all_configs(self) -> dict:
        """Return all four config files as a dict of {name: parsed_dict}."""
        result = {}
        for name, filename in _CFG_FILES.items():
            result[name] = self._read_cfg(filename)
        return result

    def get_config(self, name: str) -> dict:
        if name not in _CFG_FILES:
            raise KeyError(f"Unknown config '{name}'. Valid: {list(_CFG_FILES)}")
        return self._read_cfg(_CFG_FILES[name])

    def save_config(self, name: str, data: dict) -> dict:
        if name not in _CFG_FILES:
            raise KeyError(f"Unknown config '{name}'.")
        path = _CONFIG_DIR / _CFG_FILES[name]
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            log.info("%s saved (%d bytes)", path.name, len(path.read_bytes()))

            # If schedule was updated, also refresh in-memory settings
            if name == "schedule":
                self._schedule_settings.update(data)

            return {"success": True, "message": f"{path.name} saved."}
        except Exception as exc:
            log.error("Failed to save %s: %s", name, exc)
            return {"success": False, "message": str(exc)}

    # ── History ───────────────────────────────────────────────────────────

    def get_history(self, limit: int = 15) -> list[dict]:
        if self._history_col is not None:
            try:
                docs = list(
                    self._history_col.find({}, {"_id": 0})
                    .sort("started_at", -1)
                    .limit(limit)
                )
                return docs
            except Exception:
                pass
        # fallback to in-memory
        with self._lock:
            items = list(self._history)
        items.reverse()
        return items[:limit]

    # ──────────────────────────────────────────────────────────────────────
    #  Internal helpers
    # ──────────────────────────────────────────────────────────────────────
    def _scheduled_trigger(self, platforms: list[str]) -> None:
        with self._lock:
            if not self._scheduler_active or self._running:
                return
            if self._stop_event.is_set():
                return
        self._append_log("⏰ Scheduled trigger fired — starting next run…", "step")
        self.run_now(platforms=platforms, mode="scheduled")

    def _run_worker(self, platforms: list[str], mode: str) -> None:
        """
        Background thread that calls into offer_intel.pipeline.

        We import lazily so the dashboard can start even if offer_intel
        is not installed / partially broken.
        """
        started_at = datetime.now(UTC)
        counters   = {
            "total_scraped":    0,
            "total_extracted":  0,
            "total_normalised": 0,
            "total_published":  0,
        }
        success = True
        error   = None

        try:
            import asyncio
            from offer_intel.extraction.offer_extractor import OfferExtractor
            from offer_intel.normalization.offer_normalizer import OfferNormalizer
            from offer_intel.normalization.platform_normalizer import PlatformNormalizer
            from offer_intel.scraping.scrapers import ScraperRouter
            from offer_intel.storage.raw_social_data_store import RawSocialDataStore
            from offer_intel.storage.scrape_target_store import ScrapeTargetStore as OiScrapeTargetStore
            from offer_intel.pipeline import scrape_platform

            raw_store          = RawSocialDataStore()
            target_store       = OiScrapeTargetStore()
            platform_normalizer = PlatformNormalizer()
            extractor          = OfferExtractor()
            offer_normalizer   = OfferNormalizer()

            for platform in platforms:
                if self._stop_event.is_set():
                    self._append_log(f"⏹ Stop requested — skipping {platform}", "warn")
                    break

                self._append_log(f"── Scraping {platform.upper()} ──", "step")

                # Run the async scrape_platform in a fresh event loop
                plat_counters: dict = {}

                async def _run(plat=platform, pc=plat_counters):
                    from offer_intel.pipeline import _process_post
                    router  = ScraperRouter(plat)
                    targets = target_store.get_active_targets(platform=plat, target_type="profile")
                    profiles = [t["value"] for t in targets]

                    if not profiles:
                        self._append_log(f"  No active profiles for {plat}", "warn")
                        return

                    self._append_log(f"  {len(profiles)} profiles queued", "info")
                    results, source = await router.scrape_profiles(profiles)

                    if not results:
                        self._append_log(f"  No results returned for {plat}", "warn")
                        return

                    schema_key = plat
                    if plat == "tiktok"    and source == "apify": schema_key = "apify_tiktok"
                    if plat == "instagram" and source == "apify": schema_key = "apify_instagram"

                    for idx, raw_item in enumerate(results):
                        if plat == "facebook":
                            handle = (
                                raw_item.get("pageName")
                                or raw_item.get("facebookUrl", "").rstrip("/").rsplit("/", 1)[-1]
                            )
                        else:
                            handle = profiles[idx] if idx < len(profiles) else None

                        try:
                            posts = platform_normalizer.normalise(schema_key, raw_item, handle)
                            for post in posts:
                                _process_post(
                                    post, source, plat,
                                    raw_store, extractor, offer_normalizer, pc,
                                )
                            target_store.mark_scraped(plat, handle)
                        except Exception as ex:
                            self._append_log(f"  Error on {handle}: {ex}", "error")

                asyncio.run(_run())

                # Accumulate counters
                counters["total_scraped"]    += plat_counters.get("inserted",         0) \
                                              + plat_counters.get("updated",           0) \
                                              + plat_counters.get("unchanged",         0)
                counters["total_extracted"]  += plat_counters.get("offers_extracted",  0)
                counters["total_normalised"] += plat_counters.get("offers_normalised", 0)
                counters["total_published"]  += plat_counters.get("offers_published",  0)

                self._append_log(
                    f"  ✓ {platform.upper()} — "
                    f"scraped:{counters['total_scraped']} "
                    f"extracted:{counters['total_extracted']} "
                    f"published:{counters['total_published']}",
                    "success",
                )

                time.sleep(2)   # brief pause between platforms

        except ImportError as exc:
            msg = (
                f"offer_intel package not found: {exc}\n"
                "Make sure offer_intel is installed and on PYTHONPATH."
            )
            self._append_log(f"✗ {msg}", "error")
            success = False
            error   = msg

        except Exception as exc:
            self._append_log(f"✗ Pipeline error: {exc}", "error")
            log.exception("Pipeline worker error")
            success = False
            error   = str(exc)

        finally:
            finished_at = datetime.now(UTC)
            duration_s  = round((finished_at - started_at).total_seconds(), 1)

            run_record = {
                "started_at":       started_at.isoformat(),
                "finished_at":      finished_at.isoformat(),
                "duration_s":       duration_s,
                "platforms":        platforms,
                "mode":             mode,
                "success":          success,
                "error":            error,
                **counters,
            }

            with self._lock:
                self._running = False
                self._last_run_at = finished_at
                self._last_result = run_record
                self._history.append(run_record)
                if self._history_col is not None:
                    try:
                        self._history_col.insert_one({**run_record})
                    except Exception as e:
                        log.warning("Could not save run to DB: %s", e)

                if mode == "scheduled" and self._scheduler_active and not self._stop_event.is_set():
                    from datetime import timedelta
                    interval_h = self._schedule_settings.get("global_scrape_interval_hours", 6)
                    self._next_run_at = finished_at + timedelta(hours=interval_h)
                    self._append_log(
                        f"⏱ Next run in {interval_h}h ({self._next_run_at.strftime('%H:%M:%S')})",
                        "info",
                    )
                    if self._scheduler_timer and self._scheduler_timer.is_alive():
                        self._scheduler_timer.cancel()
                    self._scheduler_timer = threading.Timer(
                        interval_h * 3600,
                        self._scheduled_trigger,
                        args=(platforms,),
                    )
                    self._scheduler_timer.daemon = True
                    self._scheduler_timer.start()
                else:
                    self._next_run_at = None

            status_msg = "✓ Pipeline finished" if success else "✗ Pipeline failed"
            self._append_log(
                f"{status_msg} in {duration_s}s — "
                f"scraped:{counters['total_scraped']} "
                f"extracted:{counters['total_extracted']} "
                f"normalised:{counters['total_normalised']} "
                f"published:{counters['total_published']}",
                "success" if success else "error",
            )

    def _append_log(self, text: str, level: str = "info") -> None:
        self._log_buf.append({
            "text":  text,
            "level": level,
            "ts":    datetime.now(UTC).isoformat(),
        })

    def _load_schedule_settings(self) -> dict:
        """Load schedule_config.json if it exists, else sensible defaults."""
        path = _CONFIG_DIR / _CFG_FILES["schedule"]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            data.setdefault("enabled", False)
            return data
        except Exception:
            return {
                "global_scrape_interval_hours": 6,
                "per_platform_overrides": {},
                "enabled": False,
            }

    def _read_cfg(self, filename: str) -> dict:
        path = _CONFIG_DIR / filename
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            log.warning("Config file not found: %s", path)
            return {}
        except json.JSONDecodeError as exc:
            log.error("Invalid JSON in %s: %s", path, exc)
            return {}


# ── Module-level singleton ────────────────────────────────────────────────

_manager: Optional[PipelineManager] = None
_manager_lock = threading.Lock()


def get_pipeline_manager() -> PipelineManager:
    """Return (or lazily create) the process-wide PipelineManager."""
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = PipelineManager()
    return _manager