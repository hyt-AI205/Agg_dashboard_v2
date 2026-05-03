"""
main.py — Application entry point.

Responsibilities:
  • Boot FastAPI and mount static files / templates
  • Connect to MongoDB (graceful fallback to mock mode)
  • Register routers:
      /api/dashboard/*  → api.dashboard_router   (stats, health, config)
      /api/targets/*    → api.targets_router      (CRUD for scrape targets)
      /api/discovery/*  → api.discovery_router    (discovery scheduling & review)
      /api/pipeline/*   → api.pipeline_router     (offer_intel pipeline control)
  • Serve HTML pages (GET /dashboard)
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
    allow_origins=["*"],
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
# Routers
# ---------------------------------------------------------------------------
try:
    from api import (
        dashboard_router,
        targets_router,
        discovery_router,
        pipeline_router,
        init_targets_store,
    )

    init_targets_store(store, MONGODB_AVAILABLE)

    app.include_router(dashboard_router)   # /api/dashboard/*
    app.include_router(targets_router)     # /api/targets/*
    app.include_router(discovery_router)   # /api/discovery/*
    app.include_router(pipeline_router)    # /api/pipeline/*

    print("✓ Dashboard API endpoints loaded  (/api/dashboard/*)")
    print("✓ Targets  API endpoints loaded   (/api/targets/*)")
    print("✓ Discovery API endpoints loaded  (/api/discovery/*)")
    print("✓ Pipeline  API endpoints loaded  (/api/pipeline/*)")

except Exception as e:
    print(f"✗ Router registration failed: {e}")
    raise

# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


# ---------------------------------------------------------------------------
# Dev server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)