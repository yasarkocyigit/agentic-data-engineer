"""
OpenClaw Backend — FastAPI
Main application entry point with CORS and router registration.
"""
from dotenv import load_dotenv
load_dotenv()  # Load .env before any router imports read os.getenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import trino, postgres, airflow, lineage, storage, orchestrator, health, files, gitea, docker, notebook, spark

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
app.include_router(gitea.router,       prefix="/api", tags=["Gitea"])
app.include_router(docker.router,       prefix="/api", tags=["Docker"])
app.include_router(notebook.router,     prefix="/api", tags=["Notebook"])
app.include_router(spark.router,        prefix="/api", tags=["Spark"])


@app.get("/")
async def root():
    return {"status": "ok", "service": "OpenClaw API", "version": "1.0.0"}
