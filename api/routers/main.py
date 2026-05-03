"""
main.py — Application entry point.

Responsibilities:
  • Boot FastAPI and mount static files / templates
  • Connect to MongoDB (graceful fallback to mock mode)
  • Register routers:
      /api/dashboard/*  → api.dashboard_router   (stats, health, config)
      /api/targets/*    → api.targets_router      (CRUD for scrape targets)
  • Serve HTML pages (GET /dashboard)

All API logic lives in api/; this file only wires things together.
"""

import urllib.parse
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Scraper Dashboard", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Static files & templates
# ---------------------------------------------------------------------------
Path("static").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")
templates.env.filters["urlencode"] = lambda v: urllib.parse.quote(str(v))

# ---------------------------------------------------------------------------
# MongoDB / Store
# ---------------------------------------------------------------------------
MONGODB_AVAILABLE = False
store = None

try:
    from ScrapeTargetStore import ScrapeTargetStore
    store = ScrapeTargetStore()
    MONGODB_AVAILABLE = True
    print("✓ MongoDB is ready")
except Exception as e:
    print(f"✗ MongoDB failed: {e}")
    print("  Server will run in VIEW-ONLY mode with mock data")

# ---------------------------------------------------------------------------
# Routers  (only these three lines changed vs the old main.py)
# ---------------------------------------------------------------------------
try:
    from dashboard_v2 import dashboard_router, targets_router, init_targets_store

    init_targets_store(store, MONGODB_AVAILABLE)

    app.include_router(dashboard_router)   # /api/dashboard/*
    app.include_router(targets_router)     # /api/targets/*

    print("✓ Dashboard API endpoints loaded  (/api/dashboard/*)")
    print("✓ Targets  API endpoints loaded   (/api/targets/*)")
except Exception as e:
    print(f"✗ Router registration failed: {e}")
    raise  # Hard-fail; don't silently limp along

# ---------------------------------------------------------------------------
# Page routes  (HTML only — no business logic here)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect / → /dashboard"""
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Serve the unified dashboard SPA."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


# ---------------------------------------------------------------------------
# Dev server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
