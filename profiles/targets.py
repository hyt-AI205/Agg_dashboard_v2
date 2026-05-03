"""
api/routers/targets.py
───────────────────────
GET    /api/targets
POST   /api/targets
POST   /api/targets/{value}/toggle
DELETE /api/targets/{value}
"""

import urllib.parse

from fastapi import APIRouter, Form, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api", tags=["targets"])

# Injected at startup via init_store()
_store = None
_mongodb_available = False


def init_store(store, available: bool) -> None:
    """Called once from main.py after the ScrapeTargetStore is created."""
    global _store, _mongodb_available
    _store = store
    _mongodb_available = available


@router.get("/targets")
async def get_targets(
    search: str = Query("", description="Search by username or platform"),
    view_filter: str = Query("active", description="'active' | 'inactive' | 'all'"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=10, le=200),
):
    """Paginated, searchable list of scrape targets."""
    if not _mongodb_available or _store is None:
        return JSONResponse({
            "targets": [], "total": 0, "page": page,
            "limit": limit, "total_pages": 1, "has_more": False,
        })

    try:
        query_filter = {}
        if view_filter == "active":
            query_filter["active"] = True
        elif view_filter == "inactive":
            query_filter["active"] = False

        if search.strip():
            query_filter["$or"] = [
                {"value":    {"$regex": search, "$options": "i"}},
                {"platform": {"$regex": search, "$options": "i"}},
            ]

        total      = _store.collection.count_documents(query_filter)
        skip       = (page - 1) * limit
        total_pages = max(1, (total + limit - 1) // limit)

        targets = list(
            _store.collection.find(query_filter, {"_id": 0})
            .sort("added_at", -1)
            .skip(skip)
            .limit(limit)
        )
        for t in targets:
            for field in ("added_at", "last_scraped"):
                if field in t and t[field]:
                    t[field] = t[field].isoformat()

        return JSONResponse({
            "targets": targets, "total": total, "page": page,
            "limit": limit, "total_pages": total_pages, "has_more": page < total_pages,
        })

    except Exception as e:
        print(f"[targets] query failed: {e}")
        return JSONResponse(
            {"error": str(e), "targets": [], "total": 0, "page": page, "limit": limit, "total_pages": 0},
            status_code=500,
        )


@router.post("/targets")
async def add_target(platform: str = Form(...), target: str = Form(...)):
    """Add a new scrape target (returns JSON — no page reload)."""
    if not _mongodb_available or _store is None:
        return JSONResponse(
            {"success": False, "message": "⚠️ Database not available — running in view-only mode"},
            status_code=503,
        )
    try:
        existing = _store.collection.find_one({"value": target, "platform": platform})
        if existing:
            status = "active" if existing.get("active", True) else "paused"
            return JSONResponse(
                {"success": False, "message": f"⚠️ '{target}' already exists on {platform} (status: {status})"},
                status_code=409,
            )
        _store.add_target(platform=platform, target_type="profile", value=target, added_by="user")
        return JSONResponse({"success": True, "message": f"✓ Added {target} on {platform}"}, status_code=201)

    except Exception as e:
        print(f"[targets] add failed: {e}")
        return JSONResponse({"success": False, "message": f"✗ Error: {e}"}, status_code=500)


@router.post("/targets/{value}/toggle")
async def toggle_target(value: str):
    """Toggle active / paused status."""
    if not _mongodb_available or _store is None:
        return JSONResponse({"success": False, "message": "DB unavailable"}, status_code=503)
    try:
        value = urllib.parse.unquote(value)
        doc = _store.collection.find_one({"value": value})
        if not doc:
            return JSONResponse({"success": False, "message": "Target not found"}, status_code=404)
        new_status = not doc.get("active", True)
        _store.collection.update_one({"value": value}, {"$set": {"active": new_status}})
        label = "active" if new_status else "paused"
        return JSONResponse({"success": True, "active": new_status, "message": f"Target is now {label}"})

    except Exception as e:
        print(f"[targets] toggle failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.delete("/targets/{value}")
async def delete_target(value: str):
    """Permanently remove a target."""
    if not _mongodb_available or _store is None:
        return JSONResponse({"success": False, "message": "DB unavailable"}, status_code=503)
    try:
        value = urllib.parse.unquote(value)
        result = _store.collection.delete_one({"value": value})
        if result.deleted_count:
            return JSONResponse({"success": True, "message": f"Deleted {value}"})
        return JSONResponse({"success": False, "message": "Target not found"}, status_code=404)

    except Exception as e:
        print(f"[targets] delete failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)
