"""
OpenClaw Backend — FastAPI
Main application entry point with CORS and router registration.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import trino, postgres, airflow, lineage, storage, orchestrator, health, files

app = FastAPI(
    title="OpenClaw API",
    description="Backend API for OpenClaw — unified data platform",
    version="1.0.0",
)

# ─── CORS ───
# Allow requests from both Next.js (dev) and Vite (after migration)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3010",   # Next.js dev server
        "http://localhost:5173",   # Vite dev server
        "http://localhost:3000",   # Alternative ports
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Register Routers ───
app.include_router(trino.router,        prefix="/api", tags=["Trino"])
app.include_router(postgres.router,     prefix="/api", tags=["PostgreSQL"])
app.include_router(airflow.router,      prefix="/api", tags=["Airflow"])
app.include_router(lineage.router,      prefix="/api", tags=["Lineage"])
app.include_router(storage.router,      prefix="/api", tags=["Storage"])
app.include_router(orchestrator.router, prefix="/api", tags=["Orchestrator"])
app.include_router(health.router,       prefix="/api", tags=["Health"])
app.include_router(files.router,        prefix="/api", tags=["Files"])


@app.get("/")
async def root():
    return {"status": "ok", "service": "OpenClaw API", "version": "1.0.0"}
