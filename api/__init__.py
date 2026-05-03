from fastapi import APIRouter

from api.routers import config, offers, overview, scraping, system, targets
from api.routers import discovery
from api.routers import pipeline

# ── Dashboard router (/api/dashboard/*) ──────────────────────────────────────
dashboard_router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
dashboard_router.include_router(overview.router)
dashboard_router.include_router(scraping.router)
dashboard_router.include_router(offers.router)
dashboard_router.include_router(config.router)
dashboard_router.include_router(system.router)

# ── Targets router (/api/targets/*) ──────────────────────────────────────────
targets_router = targets.router

# ── Discovery router (/api/discovery/*) ──────────────────────────────────────
discovery_router = discovery.router

# ── Pipeline router (/api/pipeline/*) ────────────────────────────────────────
pipeline_router = pipeline.router


def init_targets_store(store, available: bool) -> None:
    """Proxy so main.py call signature stays identical to the old dashboard.py."""
    targets.init_store(store, available)