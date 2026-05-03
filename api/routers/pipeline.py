"""
api/routers/pipeline.py
───────────────────────
FastAPI router for all /api/pipeline/* endpoints consumed by the
Pipeline tab in dashboard.html.

Endpoints
─────────
  GET  /api/pipeline/status            — live state + new log lines (polled every 4 s)
  POST /api/pipeline/run               — trigger a run  { platforms, mode }
  POST /api/pipeline/stop              — send stop signal
  GET  /api/pipeline/history           — last N run records
  GET  /api/pipeline/configs           — all four config files as JSON
  GET  /api/pipeline/configs/{name}    — single config file
  POST /api/pipeline/configs/{name}    — overwrite single config file
  GET  /api/pipeline/schedule          — current schedule settings
  POST /api/pipeline/schedule          — save schedule settings

All write endpoints return  { "success": bool, "message": str }.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.pipeline_manager import get_pipeline_manager

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


# ── Request / Response models ─────────────────────────────────────────────

class RunRequest(BaseModel):
    platforms: list[str] = Field(
        default=["tiktok", "instagram", "facebook"],
        description="Which platforms to scrape in this run.",
    )
    mode: str = Field(
        default="once",
        description="'once' runs all platforms once and stops. 'scheduled' hands off to the scheduler.",
    )


class ScheduleSettings(BaseModel):
    global_scrape_interval_hours: int = Field(
        default=6,
        ge=1, le=168,
        description="Default interval (hours) applied to platforms without an override.",
    )
    per_platform_overrides: dict[str, int] = Field(
        default_factory=dict,
        description="Per-platform interval overrides, e.g. {'tiktok': 4, 'instagram': 8}.",
    )
    enabled: bool = Field(
        default=False,
        description="Whether the background scheduler is active.",
    )


# ── Status ────────────────────────────────────────────────────────────────

@router.get("/status", summary="Get live pipeline state + new log lines")
def pipeline_status() -> dict:
    """
    Polled every 4 s by the dashboard.  Returns:

    * running (bool)
    * platforms currently being processed
    * last / next run timestamps
    * scheduler_active flag
    * last_result counters
    * new_logs — log lines emitted since the previous call (incremental)
    """
    mgr = get_pipeline_manager()
    return mgr.get_status()


# ── Run / Stop ────────────────────────────────────────────────────────────

@router.post("/run", summary="Trigger a pipeline run")
def pipeline_run(req: RunRequest) -> dict:
    """
    Launch offer_intel scraping in a background thread.

    * ``mode = "once"``      → scrape each platform once and stop.
    * ``mode = "scheduled"`` → hand off to the internal scheduler loop
      (respects ``schedule_config.json`` intervals).

    Returns immediately; poll ``/api/pipeline/status`` for progress.
    """
    valid_platforms = {"tiktok", "instagram", "facebook"}
    bad = set(req.platforms) - valid_platforms
    if bad:
        raise HTTPException(400, detail=f"Unknown platforms: {bad}. Valid: {valid_platforms}")

    mgr = get_pipeline_manager()
    result = mgr.run_now(platforms=req.platforms, mode=req.mode)
    status_code = 200 if result["success"] else 409
    return result


@router.post("/stop", summary="Signal the running pipeline to stop")
def pipeline_stop() -> dict:
    """
    Sets a stop-event that the worker thread checks between platforms.
    The current platform will finish before halting.
    """
    mgr = get_pipeline_manager()
    return mgr.stop()


# ── Run history ───────────────────────────────────────────────────────────

@router.get("/history", summary="Last N pipeline run records")
def pipeline_history(
    limit: int = Query(default=15, ge=1, le=100, description="Max records to return"),
) -> list[dict]:
    """
    Returns run records newest-first.  Each record contains:

    ``started_at``, ``finished_at``, ``duration_s``, ``platforms``,
    ``mode``, ``success``, ``error``,
    ``total_scraped``, ``total_extracted``, ``total_normalised``, ``total_published``.
    """
    mgr = get_pipeline_manager()
    return mgr.get_history(limit=limit)


# ── Config files ──────────────────────────────────────────────────────────

_VALID_CFG_NAMES = {"scraper", "schedule", "llm", "platform"}


@router.get("/configs", summary="Return all four config files")
def get_all_configs() -> dict:
    """
    Returns a dict keyed by config name:
    ``{ "scraper": {...}, "schedule": {...}, "llm": {...}, "platform": {...} }``

    Each value is the parsed JSON object from the corresponding file in
    ``offer_intel/config/``.  Missing files return an empty dict ``{}``.
    """
    mgr = get_pipeline_manager()
    return mgr.get_all_configs()


@router.get("/configs/{name}", summary="Return a single config file")
def get_config(name: str) -> dict:
    """
    Valid names: ``scraper``, ``schedule``, ``llm``, ``platform``.
    """
    if name not in _VALID_CFG_NAMES:
        raise HTTPException(404, detail=f"Unknown config '{name}'. Valid: {sorted(_VALID_CFG_NAMES)}")
    mgr = get_pipeline_manager()
    try:
        return mgr.get_config(name)
    except KeyError as exc:
        raise HTTPException(404, detail=str(exc))


@router.post("/configs/{name}", summary="Overwrite a single config file")
def save_config(name: str, body: dict[str, Any]) -> dict:
    """
    Replaces the entire config file with the supplied JSON body.

    * Validates that the body is valid JSON (FastAPI does this automatically).
    * Writes to ``offer_intel/config/<name>_config.json``.
    * Returns ``{ "success": true, "message": "..." }``.

    **Note**: changes take effect on the *next* pipeline run; a running
    pipeline continues with the config it loaded at start-up.
    """
    if name not in _VALID_CFG_NAMES:
        raise HTTPException(404, detail=f"Unknown config '{name}'.")
    mgr = get_pipeline_manager()
    try:
        return mgr.save_config(name, body)
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


# ── Schedule ──────────────────────────────────────────────────────────────

@router.get("/schedule", summary="Get current schedule settings")
def get_schedule() -> dict:
    """
    Returns the in-memory schedule settings (loaded from
    ``schedule_config.json`` at startup and updated via POST).

    Fields: ``global_scrape_interval_hours``, ``per_platform_overrides``,
    ``enabled``.
    """
    mgr = get_pipeline_manager()
    return {"settings": mgr.get_schedule()}


@router.post("/schedule", summary="Save schedule settings")
def save_schedule(settings: ScheduleSettings) -> dict:
    """
    Persists schedule settings both in memory and to
    ``offer_intel/config/schedule_config.json``.

    If ``enabled`` is ``true`` the dashboard will show the scheduler as
    active; the actual APScheduler / schedule loop is controlled by
    ``offer_intel.pipeline`` itself — this endpoint only stores the
    configuration.
    """
    mgr = get_pipeline_manager()
    return mgr.save_schedule(settings.model_dump())


@router.post("/cancel-schedule", summary="Cancel pending scheduled runs without stopping current run")
def pipeline_cancel_schedule() -> dict:
    """
    Cancels the scheduler timer so no future runs are triggered.
    If a run is currently in progress it will finish normally.
    """
    mgr = get_pipeline_manager()
    with mgr._lock:
        mgr._scheduler_active = False
        mgr._next_run_at = None
        if mgr._scheduler_timer and mgr._scheduler_timer.is_alive():
            mgr._scheduler_timer.cancel()
            mgr._scheduler_timer = None
    mgr._append_log("⏹ Schedule cancelled by user.", "warn")
    return {"success": True, "message": "Schedule cancelled."}