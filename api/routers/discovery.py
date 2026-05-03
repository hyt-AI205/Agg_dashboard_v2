"""
api/routers/discovery.py
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient, DESCENDING
from collections import deque

log = logging.getLogger(__name__)
class _DiscLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        level_map = {
            logging.DEBUG:    "dim",
            logging.INFO:     "info",
            logging.WARNING:  "warn",
            logging.ERROR:    "error",
            logging.CRITICAL: "error",
        }
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                datefmt="%H:%M:%S")
        _log_buf.append({
            "text":  fmt.format(record),
            "level": level_map.get(record.levelno, "info"),
            "ts":    datetime.now(UTC).isoformat(),
        })
UTC = timezone.utc

router = APIRouter(prefix="/api/discovery", tags=["discovery"])

# ── MongoDB ───────────────────────────────────────────────────────────────────
_mongo_client: Optional[MongoClient] = None

def _get_db():
    global _mongo_client
    if _mongo_client is None:
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=3000)
    return _mongo_client["social_scraper"]

def _col(name: str):
    return _get_db()[name]

def _serialize(doc: dict) -> dict:
    for k, v in list(doc.items()):
        if isinstance(v, ObjectId):
            doc[k] = str(v)
        elif isinstance(v, datetime):
            doc[k] = v.isoformat()
        elif isinstance(v, dict):
            doc[k] = _serialize(v)
        elif isinstance(v, list):
            doc[k] = [_serialize(i) if isinstance(i, dict) else i for i in v]
    return doc


# ── Robust package finder ─────────────────────────────────────────────────────
_pkg_name: Optional[str] = None   # cached after first successful find

def _find_pkg() -> str:
    global _pkg_name
    if _pkg_name:
        return _pkg_name

    candidates = ["discovery", "profile_discovery"]
    this_file   = os.path.abspath(__file__)
    project_dir = os.path.dirname(os.path.dirname(os.path.dirname(this_file)))
    parent_dir  = os.path.dirname(project_dir)
    for p in [project_dir, parent_dir]:
        if p not in sys.path:
            sys.path.insert(0, p)

    for pkg in candidates:
        try:
            importlib.import_module(pkg)
            _pkg_name = pkg
            log.info("Discovery package: '%s'", pkg)
            return pkg
        except ModuleNotFoundError:
            continue

    raise ImportError(
        "Cannot find discovery package (tried: discovery, profile_discovery). "
        "Make sure the folder is next to main.py and has __init__.py + engine.py."
    )

def _import(submodule: str):
    return importlib.import_module(f"{_find_pkg()}.{submodule}")


# ── In-memory run state ───────────────────────────────────────────────────────
_run_state: dict = {
    "running": False,
    "last_result": None,
    "last_run_at": None,
    "next_run_at": None,
    "scheduler_active": False,
    "scheduler_thread": None,
}
_log_buf: deque = deque(maxlen=500)
_log_cursor: int = 0

def _disc_log(text: str, level: str = "info") -> None:
    """Append a line to the discovery live-log buffer."""
    _log_buf.append({
        "text":  text,
        "level": level,
        "ts":    datetime.now(UTC).isoformat(),
    })

# ── Build a Region object from user inputs ────────────────────────────────────
def _build_region(region_key: str, region_mode: str, hashtags: list):
    """
    Return a fully populated Region object ready for the engine.

    preset mode:
      - Start from the config Region
      - If user supplied hashtags → replace region.hashtags (Instagram/TikTok)
        AND region.keywords (Facebook), so all three platforms get something useful
      - If no hashtags supplied → use config defaults unchanged

    custom (manual) mode:
      - Build a minimal Region from scratch
      - hashtags → region.hashtags  (Instagram / TikTok)
      - hashtags → region.keywords  (Facebook — used as search keywords)
      - region_key is used as both name and country_names entry
    """
    cfg_mod  = _import("config")
    REGIONS  = cfg_mod.REGIONS
    Region   = cfg_mod.Region

    import copy

    if region_mode == "preset" and region_key in REGIONS:
        r = copy.copy(REGIONS[region_key])
        if hashtags:
            # Override search inputs for all platforms
            r.hashtags = list(hashtags)   # Instagram + TikTok
            r.keywords  = list(hashtags)  # Facebook
            log.info("Preset region '%s' — overriding hashtags+keywords with: %s",
                     region_key, hashtags)
        else:
            log.info("Preset region '%s' — using config defaults", region_key)
        return region_key, r

    # custom mode OR unknown key → build from scratch
    region_name = region_key  # whatever the user typed, e.g. "Algeria"
    r = Region(
        name=region_name,
        country_code="XX",
        country_names=[region_name],
        # Facebook builds search_queries as keyword × city — if cities=[] the
        # product is empty and zero Apify calls are made.  Use the region name
        # itself as a single city so at least one query is generated.
        cities=[region_name],
        hashtags=list(hashtags or []),   # Instagram + TikTok
        keywords=list(hashtags or []),   # Facebook search keywords
        city_hashtags=[],
    )
    # Use a sanitised key for REGIONS dict (lowercase, no spaces)
    safe_key = region_name.lower().replace(" ", "_")
    log.info("Custom region '%s' (key='%s') — hashtags/keywords: %s cities: %s",
             region_name, safe_key, hashtags, r.cities)
    return safe_key, r


# ── Background runner ─────────────────────────────────────────────────────────
def _do_run(region: str, platforms: list, limit,
            hashtags: list = None, region_mode: str = "preset"):
    """Blocking discovery run — called inside a thread."""
    _run_state["running"] = True
    _run_state["last_run_at"] = datetime.now(UTC).isoformat()

    # Attach log handler so all discovery logs flow into the live buffer
    _disc_log(f"▶ Discovery run started — region: {region} | platforms: {platforms}", "step")
    _handler = _DiscLogHandler()
    for logger_name in ["discovery", "profile_discovery", __name__]:
        logging.getLogger(logger_name).addHandler(_handler)

    try:
        DiscoveryEngine = _import("engine").DiscoveryEngine
        cfg_mod = _import("config")

        # Build the correct region object
        region_key, region_obj = _build_region(
            region, region_mode, hashtags or []
        )

        log.info(
            "Run | key=%s name=%s | hashtags=%s | keywords=%s | platforms=%s | limit=%s",
            region_key, region_obj.name,
            region_obj.hashtags, region_obj.keywords,
            platforms, limit,
        )

        # Temporarily inject into REGIONS so engine.run() can find it
        _orig = cfg_mod.REGIONS.copy()
        cfg_mod.REGIONS[region_key] = region_obj
        try:
            engine = DiscoveryEngine()
            result = engine.run(
                region_key,
                platforms=platforms or None,
                limit=limit or None,
            )
        finally:
            cfg_mod.REGIONS = _orig   # always restore

        _run_state["last_result"] = result.to_dict()
        log.info("Run finished: inserted=%d updated=%d",
                 result.inserted, result.updated)

        _col("discovery_runs").insert_one({
            "ran_at": datetime.now(UTC),
            "result": result.to_dict(),
        })

    except Exception as exc:
        log.error("Discovery run failed: %s", exc)
        _run_state["last_result"] = {"error": str(exc), "success": False}
        try:
            _col("discovery_runs").insert_one({
                "ran_at": datetime.now(UTC),
                "result": {"error": str(exc), "success": False},
            })
        except Exception:
            pass
    finally:
        _run_state["running"] = False
        for logger_name in ["discovery", "profile_discovery", __name__]:
            logging.getLogger(logger_name).removeHandler(_handler)
        _disc_log("■ Discovery run finished.", "success")


def _scheduler_loop():
    log.info("Discovery scheduler started")
    while _run_state["scheduler_active"]:
        cfg = _load_schedule_cfg()
        interval_hours = cfg.get("interval_hours", 48)

        last_run = _run_state.get("last_run_at")
        if last_run:
            last_dt = datetime.fromisoformat(last_run).replace(tzinfo=UTC)
            next_dt = last_dt + timedelta(hours=interval_hours)
        else:
            # No previous run — wait a full interval before the first scheduled run.
            # User can always click "Run Now" manually if they want immediate execution.
            next_dt = datetime.now(UTC) + timedelta(hours=interval_hours)

        _run_state["next_run_at"] = next_dt.isoformat()
        now = datetime.now(UTC)

        if now >= next_dt:
            if not _run_state["running"]:
                _do_run(
                    region=cfg.get("region", "yemen"),
                    platforms=cfg.get("platforms", []),
                    limit=cfg.get("limit"),
                    hashtags=cfg.get("hashtags", []),
                    region_mode=cfg.get("region_mode", "preset"),
                )
        else:
            time.sleep(min((next_dt - now).total_seconds(), 60))
            continue

        time.sleep(60)

    log.info("Discovery scheduler stopped")


def _load_schedule_cfg() -> dict:
    try:
        doc = _col("discovery_schedule").find_one({}, sort=[("updated_at", DESCENDING)])
        if doc:
            return doc.get("settings", {})
    except Exception:
        pass
    return {
        "interval_hours": 48, "region": "yemen", "region_mode": "preset",
        "platforms": [], "hashtags": [], "limit": None,
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/status")
async def get_status():
    global _log_cursor
    try:
        last_db = _col("discovery_runs").find_one(
            {}, sort=[("ran_at", DESCENDING)], projection={"_id": 0}
        )
    except Exception:
        last_db = None

    # Incremental log lines since last poll
    buf_list  = list(_log_buf)
    new_lines = buf_list[_log_cursor:]
    _log_cursor = len(buf_list)

    return {
        "running":          _run_state["running"],
        "scheduler_active": _run_state["scheduler_active"],
        "last_run_at":      _run_state.get("last_run_at"),
        "next_run_at":      _run_state.get("next_run_at"),
        "last_result":      _run_state.get("last_result") or (
            last_db["result"] if last_db else None
        ),
        "new_logs":         new_lines,
    }


@router.get("/regions")
async def list_regions():
    try:
        REGIONS = _import("config").REGIONS
        return [
            {
                "key":           k,
                "name":          r.name,
                "hashtag_count": len(r.hashtags),
                "city_count":    len(r.cities),
                "hashtags":      list(r.hashtags),
            }
            for k, r in REGIONS.items()
        ]
    except Exception as e:
        log.warning("Could not load regions: %s", e)
        return [{"key": "yemen", "name": "Yemen", "hashtag_count": 1,
                 "city_count": 10, "hashtags": ["تخفيضات_اليمن"]}]


@router.get("/regions/discovered")
async def list_discovered_regions():
    """
    Return distinct region values actually stored in MongoDB.
    Used to populate the filter dropdown — includes manual/custom regions
    that are not defined in config.py.
    """
    try:
        regions = _col("scrape_targets").distinct(
            "discovery_meta.region",
            {"added_by": "discovery", "discovery_meta.region": {"$exists": True, "$ne": None}}
        )
        return sorted([r for r in regions if r])
    except Exception as e:
        log.warning("list_discovered_regions error: %s", e)
        return []


@router.get("/schedule")
async def get_schedule():
    try:
        doc = _col("discovery_schedule").find_one(
            {}, sort=[("updated_at", DESCENDING)], projection={"_id": 0}
        )
        if doc:
            return doc
    except Exception as e:
        log.warning("get_schedule error: %s", e)
    return {
        "settings": {
            "interval_hours": 48, "region": "yemen", "region_mode": "preset",
            "hashtags": [], "platforms": ["instagram", "tiktok", "facebook"],
            "limit": None, "enabled": False,
        },
        "updated_at": None,
    }


@router.post("/schedule")
async def save_schedule(payload: dict):
    try:
        settings = {
            "interval_hours": int(payload.get("interval_hours", 48)),
            "region":         payload.get("region", "yemen"),
            "region_mode":    payload.get("region_mode", "preset"),
            "hashtags":       payload.get("hashtags") or [],
            "platforms":      payload.get("platforms", ["instagram", "tiktok", "facebook"]),
            "limit":          payload.get("limit") or None,
            "enabled":        bool(payload.get("enabled", False)),
        }
        _col("discovery_schedule").insert_one({
            "settings": settings, "updated_at": datetime.now(UTC),
        })

        if settings["enabled"] and not _run_state["scheduler_active"]:
            _run_state["scheduler_active"] = True
            t = threading.Thread(target=_scheduler_loop, daemon=True)
            t.start()
            _run_state["scheduler_thread"] = t
        elif not settings["enabled"] and _run_state["scheduler_active"]:
            _run_state["scheduler_active"] = False

        return {"success": True, "settings": settings}
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@router.post("/run")
async def trigger_run(payload: dict, background_tasks: BackgroundTasks):
    if _run_state["running"]:
        return JSONResponse(
            {"success": False, "message": "A run is already in progress."},
            status_code=409,
        )
    background_tasks.add_task(
        _do_run,
        region=payload.get("region", "yemen"),
        platforms=payload.get("platforms") or [],
        limit=payload.get("limit") or None,
        hashtags=payload.get("hashtags") or [],
        region_mode=payload.get("region_mode", "preset"),
    )
    ht = payload.get("hashtags") or []
    msg = f"Run started for '{payload.get('region')}'"
    if ht:
        msg += f" with {len(ht)} custom hashtag(s): {', '.join(ht)}"
    return {"success": True, "message": msg}


@router.get("/candidates")
async def get_candidates(
    region:    str = Query(None),
    platform:  str = Query(None),
    min_score: int = Query(0, ge=0, le=100),
    page:  int = Query(1, ge=1),
    limit: int = Query(20, ge=5, le=100),
):
    try:
        filt: dict = {"added_by": "discovery", "discovery_reviewed": {"$ne": True}}
        if region:    filt["discovery_meta.region"] = region
        if platform:  filt["platform"] = platform
        if min_score: filt["discovery_meta.final_score"] = {"$gte": min_score}

        col   = _col("scrape_targets")
        total = col.count_documents(filt)
        skip  = (page - 1) * limit
        docs  = [_serialize(d) for d in
                 col.find(filt).sort("discovery_meta.final_score", DESCENDING)
                 .skip(skip).limit(limit)]
        return {"candidates": docs, "total": total, "page": page,
                "limit": limit, "total_pages": max(1, (total + limit - 1) // limit)}
    except Exception as e:
        log.error("get_candidates: %s", e)
        return JSONResponse({"candidates": [], "total": 0, "page": 1,
                             "limit": limit, "total_pages": 1}, status_code=500)


@router.post("/candidates/{candidate_id}/approve")
async def approve_candidate(candidate_id: str):
    try:
        r = _col("scrape_targets").update_one(
            {"_id": ObjectId(candidate_id)},
            {"$set": {"active": True, "approved_at": datetime.now(UTC), "discovery_reviewed": True}},
        )
        if r.matched_count:
            return {"success": True, "message": "Approved and activated."}
        return JSONResponse({"success": False, "message": "Not found."}, status_code=404)
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(candidate_id: str):
    try:
        r = _col("scrape_targets").delete_one({"_id": ObjectId(candidate_id)})
        if r.deleted_count:
            return {"success": True, "message": "Rejected and removed."}
        return JSONResponse({"success": False, "message": "Not found."}, status_code=404)
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@router.get("/logs")
async def get_run_logs(limit: int = Query(10, ge=1, le=50)):
    try:
        docs = list(_col("discovery_runs").find({}, {"_id": 0})
                    .sort("ran_at", DESCENDING).limit(limit))
        return [_serialize(d) for d in docs]
    except Exception:
        return []


@router.get("/stats")
async def get_discovery_stats(region: str = Query(None)):
    # Always use our own aggregation — DiscoveryStorage.get_discovery_stats()
    # counts pending as active=False which breaks when a profile is paused
    # from Target Manager. We count pending as discovery_reviewed != true instead.
    try:
        match: dict = {"added_by": "discovery"}
        if region: match["discovery_meta.region"] = region
        rows = list(_col("scrape_targets").aggregate([
            {"$match": match},
            {"$group": {
                "_id": "$platform",
                "total":     {"$sum": 1},
                "active":    {"$sum": {"$cond": ["$active", 1, 0]}},
                "pending":   {"$sum": {"$cond": [{"$ne": ["$discovery_reviewed", True]}, 1, 0]}},
                "avg_score": {"$avg": "$discovery_meta.final_score"},
            }},
        ]))
        return {r["_id"]: {
            "total":     r["total"],
            "active":    r["active"],
            "pending":   r["pending"],
            "avg_score": round(r.get("avg_score") or 0, 1),
        } for r in rows}
    except Exception as e:
        log.error("get_discovery_stats: %s", e)
        return {}


@router.post("/cancel-schedule")
async def cancel_schedule():
    """Cancel pending scheduled runs without stopping a currently running one."""
    _run_state["scheduler_active"] = False
    _run_state["next_run_at"] = None
    log.info("Discovery schedule cancelled by user.")
    return {"success": True, "message": "Schedule cancelled."}
