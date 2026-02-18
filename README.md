
<div align="center">

# OpenClaw

### Agentic Data Platform

[![Airflow 3.0](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Spark 4.1.1](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.10-4E8EE9?logo=apacheiceberg&logoColor=white)](https://iceberg.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React 19](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev/)
[![Vite](https://img.shields.io/badge/Vite-6-646CFF?logo=vite&logoColor=white)](https://vite.dev/)
[![Trino](https://img.shields.io/badge/Trino-SQL%20Engine-DD00A1?logo=trino&logoColor=white)](https://trino.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

*A full-stack data platform with an IDE-style web interface, FastAPI backend, declarative Spark pipelines, and end-to-end observability — all running locally on a single machine.*

---

[Architecture](#architecture) · [Quick Start](#quick-start) · [Web UI](#web-ui-openclaw-workspace) · [API Reference](#api-reference) · [Pipeline Flow](#medallion-pipeline-flow) · [Project Structure](#project-structure)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Web UI — OpenClaw Workspace](#web-ui-openclaw-workspace)
- [API Reference](#api-reference)
- [Medallion Pipeline Flow](#medallion-pipeline-flow)
- [Configuration-as-Code](#configuration-as-code)
- [Data Quality Framework](#data-quality-framework)
- [Query Engine (Trino)](#query-engine-trino)
- [Data Lineage (Marquez)](#data-lineage-marquez)
- [Data Visualization (Superset)](#data-visualization-superset)
- [CI/CD (Gitea)](#cicd-gitea)
- [Project Structure](#project-structure)
- [Service Endpoints](#service-endpoints)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

**OpenClaw** is a fully self-hosted, end-to-end data platform that combines:

- **An IDE-style web interface** for exploring files, querying databases, monitoring workflows, and managing infrastructure
- **A FastAPI backend** that acts as a unified API gateway to all platform services
- **Declarative Spark pipelines** implementing the Medallion Architecture (Bronze → Silver → Gold)
- **Full observability** with OpenLineage-based data lineage tracking

Everything runs locally via Docker Compose — no cloud dependencies required.

### Key Principles

| Principle | Description |
|---|---|
| **Config-as-Code** | All pipeline logic, transformations, DQ rules, and connections defined in YAML |
| **Unified API Layer** | FastAPI backend proxies all platform services under a single `/api` prefix |
| **IDE-Style Interface** | JetBrains-inspired dark UI with file explorer, SQL editor, DAG graph, and terminal |
| **Metadata-Driven** | A shared metadata engine resolves configs, applies transforms, and manages runtime state |
| **Declarative Gold Layer** | Gold tables use Spark 4.1+ Declarative Pipelines (SDP) with `@dp.materialized_view` |
| **Full Observability** | OpenLineage via Marquez for end-to-end data lineage |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PRESENTATION LAYER                             │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │            OpenClaw Workspace (React 19 + Vite)                 │   │
│   │                                                                 │   │
│   │  ┌──────────┬──────────┬────────┬─────────┬────────┬─────────┐ │   │
│   │  │Dashboard │  Data    │Compute │Workflows│Lineage │ CI/CD   │ │   │
│   │  │ Explorer │SQL Editor│ Spark  │DAG Graph│ Marquez│  Gitea  │ │   │
│   │  └──────────┴──────────┴────────┴─────────┴────────┴─────────┘ │   │
│   └───────────────────────────┬─────────────────────────────────────┘   │
│                               │  Vite Proxy (:3010 → :8000)            │
└───────────────────────────────┼─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│                           API LAYER                                     │
│                   FastAPI Backend (:8000)                                │
│                                                                         │
│   ┌────────┬──────────┬─────────┬─────────┬─────────┬──────────────┐   │
│   │ /trino │/postgres │/airflow │/lineage │/storage │/orchestrator │   │
│   │        │          │         │         │         │              │   │
│   │ Query  │ Schema   │ CLI     │ Marquez │ MinIO   │ DAGs, Runs   │   │
│   │ Engine │ Explorer │ Exec    │ Proxy   │ Client  │ Tasks, Logs  │   │
│   └───┬────┴────┬─────┴────┬───┴────┬────┴────┬────┴──────┬───────┘   │
└───────┼─────────┼──────────┼────────┼─────────┼───────────┼────────────┘
        │         │          │        │         │           │
┌───────▼─────────▼──────────▼────────▼─────────▼───────────▼────────────┐
│                        SERVICE LAYER                                    │
│                                                                         │
│  ┌───────────┐ ┌───────────┐ ┌──────────┐ ┌───────┐ ┌───────────────┐ │
│  │   Trino   │ │PostgreSQL │ │ Airflow  │ │Marquez│ │     MinIO     │ │
│  │ SQL Engine│ │    17     │ │   3.0    │ │Lineage│ │ Object Store  │ │
│  │  :8083    │ │  :5433    │ │  :8081   │ │ :5002 │ │ :9000 / :9001 │ │
│  └─────┬─────┘ └──────────┘ └────┬─────┘ └───────┘ └───────┬───────┘ │
│        │                         │                          │         │
│        └─────────────────────────┼──────────────────────────┘         │
│                                  │                                     │
│  ┌────────────────────── COMPUTE ┼──────────────────────────────────┐ │
│  │              Apache Spark 4.1.1 Cluster                          │ │
│  │         ┌──────────┐    ┌──────────────┐                         │ │
│  │         │  Master   │◄──│   Worker(s)  │                         │ │
│  │         │  :8082    │   │  2G / 2 cores│                         │ │
│  │         └──────────┘    └──────────────┘                         │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                  │                                     │
│  ┌────────────────────── STORAGE ┼──────────────────────────────────┐ │
│  │                    MinIO (S3-Compatible)                          │ │
│  │      ┌──────┐  ┌──────┐  ┌──────┐  ┌───────────┐               │ │
│  │      │bronze│  │silver│  │ gold │  │ warehouse │               │ │
│  │      └──────┘  └──────┘  └──────┘  └───────────┘               │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                       │
│  │  Superset   │  │   Gitea    │  │Gitea Runner│                       │
│  │ Dashboards  │  │  Git + CI  │  │  Act Jobs  │                       │
│  │   :8089     │  │   :3030    │  │            │                       │
│  └────────────┘  └────────────┘  └────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

### Backend

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **API Gateway** | FastAPI | 0.115 | Unified REST API for all platform services |
| **ASGI Server** | Uvicorn | 0.34 | High-performance Python web server |
| **HTTP Client** | httpx | 0.28 | Async HTTP requests to downstream services |
| **PostgreSQL Driver** | psycopg | 3.2 | Modern async PostgreSQL driver |
| **S3 Client** | boto3 | 1.36 | MinIO/S3 object storage operations |

### Frontend

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **Framework** | React | 19.2 | UI component library |
| **Build Tool** | Vite | 6.3 | Lightning-fast dev server and bundler |
| **Routing** | React Router | 7.5 | Client-side navigation |
| **Code Editor** | Monaco Editor | 4.7 | SQL editor with syntax highlighting |
| **Styling** | Tailwind CSS | 4.x | Utility-first CSS framework |
| **Animations** | Framer Motion | 12.x | Smooth UI transitions |
| **Icons** | Lucide React | 0.564 | Icon library |

### Data Platform

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **Orchestration** | Apache Airflow | 3.0 | DAG scheduling, task orchestration |
| **Compute** | Apache Spark | 4.1.1 | Distributed data processing, SDP pipelines |
| **Table Format** | Apache Iceberg | 1.10.1 | ACID transactions, schema evolution, time travel |
| **Object Storage** | MinIO | Latest | S3-compatible storage for all lakehouse layers |
| **SQL Engine** | Trino | Latest | Federated SQL queries over Iceberg tables |
| **Data Lineage** | Marquez | Latest | OpenLineage-based lineage tracking |
| **Metadata Store** | PostgreSQL | 17 | JDBC Iceberg catalog, Airflow metadata |
| **Visualization** | Apache Superset | Latest | Interactive dashboards and charts |
| **Source Control** | Gitea | Latest | Self-hosted Git server with CI/CD |

---

## Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose v2+)
- **PostgreSQL 17** running on `localhost:5433`
- **Node.js 20+** (for frontend development)
- **Python 3.12+** (for backend development)
- **8 GB RAM** minimum for Docker

### 1. Clone & Configure

```bash
git clone https://github.com/yasarkocyigit/agentic-data-engineer.git
cd agentic-data-engineer
cp .env.example .env
# Edit .env with your credentials
```

### 2. Download JARs

```bash
chmod +x setup.sh
./setup.sh
```

<details>
<summary>Downloaded JARs</summary>

| JAR | Version | Purpose |
|---|---|---|
| `iceberg-spark-runtime-4.0_2.13` | 1.10.1 | Iceberg Spark integration |
| `bundle` (AWS SDK v2) | 2.31.1 | S3A filesystem for MinIO |
| `hadoop-aws` | 3.4.1 | Hadoop AWS connector |
| `postgresql` | 42.7.5 | JDBC driver for PostgreSQL |

</details>

### 3. Set Up Source Database

```bash
psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE sourcedb;"
psql -h localhost -p 5433 -U postgres -d sourcedb -f scripts/setup_sourcedb.sql
pip install psycopg2-binary
python3 scripts/generate_tpch_data.py --scale 0.1
```

### 4. Launch Everything

```bash
# Start all platform services (17 containers)
docker compose up -d

# Start the API backend (development)
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Start the frontend (development)
cd web-ui
npm install
npm run dev
```

### 5. Open the Platform

Navigate to **[http://localhost:3010](http://localhost:3010)** — the OpenClaw Workspace.

---

## Web UI — OpenClaw Workspace

The web interface is a **JetBrains-inspired IDE** built with React 19 and Vite. It provides a unified view of the entire data platform.

### Pages

| Page | Route | Description |
|---|---|---|
| **Dashboard** | `/` | File explorer, agent console, Iceberg catalog browser, service health |
| **Data** | `/data` | DataGrip-style SQL editor with Trino + PostgreSQL, schema explorer, query history |
| **Compute** | `/compute` | Spark cluster monitoring — master/workers, running jobs, resource usage |
| **Workflows** | `/workflows` | Airflow DAG list with run history, status, and schedule info |
| **Lineage** | `/lineage` | Marquez data lineage graph — table-level and job-level |
| **Storage** | `/storage` | MinIO object browser — buckets, objects, file preview |
| **CI/CD** | `/cicd` | Gitea repository and workflow run viewer |
| **Visualize** | `/visualize` | Apache Superset integration for dashboards |
| **Agent** | `/agent` | AI agent interaction interface |

### UI Components

- **Sidebar** — Icon-based navigation rail (JetBrains-style) with expandable panels for file explorer, database tree, and search
- **DagGraph** — Interactive DAG visualization with node details, run states, and zoom controls
- **Monaco SQL Editor** — Full-featured SQL editor with syntax highlighting, auto-complete, and multi-tab support

### Frontend Architecture

```
web-ui/
├── index.html              ← Vite entry point (Google Fonts loaded here)
├── vite.config.ts          ← Dev server (:3010), proxy rules, path aliases
├── package.json            ← React 19, Vite 6, React Router 7, Tailwind 4
├── tsconfig.json           ← TypeScript config with @/ path alias
└── src/
    ├── main.tsx            ← React entry + BrowserRouter with all routes
    ├── app/
    │   ├── globals.css     ← Global styles (Tailwind base)
    │   ├── layout.tsx      ← Root layout wrapper
    │   ├── page.tsx        ← Dashboard / Home page
    │   ├── data/           ← SQL editor page
    │   ├── compute/        ← Spark monitoring page
    │   ├── workflows/      ← Airflow DAGs page
    │   ├── lineage/        ← Data lineage page
    │   ├── storage/        ← MinIO browser page
    │   ├── cicd/           ← Gitea CI/CD page
    │   ├── visualize/      ← Superset page
    │   └── agent/          ← AI agent page
    └── components/
        ├── Sidebar.tsx     ← Navigation rail + file tree + DB explorer
        └── DagGraph.tsx    ← Interactive DAG visualization
```

### Vite Proxy Configuration

The Vite dev server proxies API calls to the backend services:

```typescript
// vite.config.ts
proxy: {
    '/api':         → 'http://localhost:8000'   // FastAPI backend
    '/gitea-proxy': → 'http://localhost:3030'   // Gitea API
    '/assets':      → 'http://localhost:3030'   // Gitea static assets
    '/avatars':     → 'http://localhost:3030'   // Gitea avatars
}
```

---

## API Reference

The FastAPI backend runs on **port 8000** and serves as the unified API gateway. All endpoints are prefixed with `/api`.

Interactive API documentation: **[http://localhost:8000/docs](http://localhost:8000/docs)** (Swagger UI)

### Routers

| Router | Prefix | Endpoints | Description |
|---|---|---|---|
| **trino** | `/api/trino` | `POST /query` | Execute SQL queries against Trino with pagination |
| **postgres** | `/api/postgres` | `POST /query`, `GET /schemas`, `GET /tables`, `GET /columns` | PostgreSQL query execution and schema exploration |
| **airflow** | `/api/airflow` | `POST /cli` | Execute Airflow CLI commands via `docker exec` |
| **lineage** | `/api/lineage` | `GET /namespaces`, `GET /jobs`, `GET /datasets` | Marquez API proxy for lineage data |
| **storage** | `/api/storage` | `GET /buckets`, `GET /objects`, `GET /preview` | MinIO/S3 operations — list, browse, preview files |
| **orchestrator** | `/api/orchestrator` | `GET /dags`, `GET /dag-runs`, `GET /tasks`, `POST /trigger`, `GET /graph`, `GET /notebook` | Full Airflow REST API client with JWT authentication |
| **health** | `/api/health` | `GET /check` | Health check for any HTTP endpoint |
| **files** | `/api/files` | `GET /tree`, `GET /content` | Project file system browser (with path traversal protection) |

### Key API Details

#### Trino Query

```bash
curl -X POST http://localhost:8000/api/trino/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM iceberg.silver.clean_orders LIMIT 10"}'
```

#### PostgreSQL Schema Explorer

```bash
# List all schemas
curl http://localhost:8000/api/postgres/schemas

# List tables in a schema
curl http://localhost:8000/api/postgres/tables?schema=public

# Get column details
curl http://localhost:8000/api/postgres/columns?schema=public&table=orders
```

#### Airflow Orchestrator (JWT-authenticated)

```bash
# List all DAGs
curl http://localhost:8000/api/orchestrator/dags

# Trigger a DAG run
curl -X POST http://localhost:8000/api/orchestrator/trigger \
  -H "Content-Type: application/json" \
  -d '{"dag_id": "medallion_pipeline"}'

# Get DAG graph (task dependencies)
curl http://localhost:8000/api/orchestrator/graph?dag_id=medallion_pipeline
```

#### MinIO Storage

```bash
# List buckets
curl http://localhost:8000/api/storage/buckets

# List objects in a bucket
curl http://localhost:8000/api/storage/objects?bucket=silver&prefix=fact_orders/

# Preview a file
curl http://localhost:8000/api/storage/preview?bucket=bronze&key=tpch/orders/part-00000.parquet
```

### Backend Architecture

```
backend/
├── Dockerfile              ← python:3.12-slim, port 8000
├── main.py                 ← FastAPI app, CORS middleware, router registration
├── requirements.txt        ← 6 dependencies (fastapi, uvicorn, httpx, psycopg, boto3, python-dotenv)
└── routers/
    ├── __init__.py
    ├── trino.py            ← Trino query execution via HTTP API
    ├── postgres.py         ← PostgreSQL queries + schema introspection (psycopg v3)
    ├── airflow.py          ← Docker CLI exec with auto-recovery
    ├── lineage.py          ← Marquez REST API proxy
    ├── storage.py          ← MinIO/S3 client (boto3)
    ├── health.py           ← Generic HTTP health checker
    ├── files.py            ← File system tree + content (with security guards)
    └── orchestrator.py     ← Airflow REST API (JWT auth, token caching, auto-refresh)
```

---

## Medallion Pipeline Flow

The `medallion_pipeline` DAG orchestrates the full data lifecycle:

```
read_config → run_bronze_ingestion → run_silver_transforms → log_success
                                                             ↘ log_failure
```

### Bronze Layer — Raw Ingestion

| Attribute | Detail |
|---|---|
| **Source** | PostgreSQL TPC-H (8 tables via JDBC) |
| **Format** | Parquet |
| **Target** | `s3a://bronze/tpch/<table>` |
| **Strategy** | Full load (dimensions) / Incremental (facts with `modified_at` watermark) |

**Tables:** `orders`, `lineitem`, `customer`, `part`, `supplier`, `nation`, `region`, `partsupp`

Each record is enriched with: `_ingested_at`, `_source_table`, `_pipeline`.

### Silver Layer — Transformations & Data Quality

| Attribute | Detail |
|---|---|
| **Source** | Bronze Parquet tables |
| **Format** | Apache Iceberg (ACID, schema evolution) |
| **Target** | `lakehouse.silver.<table>` |
| **DQ Engine** | YAML-configured rules → SQL expectations |

Transformations: column renaming, type casting, string trimming, null handling, SCD Type 2 tracking, deduplication.

### Gold Layer — Analytics Materialized Views

| Attribute | Detail |
|---|---|
| **Framework** | Spark Declarative Pipelines (SDP, Spark 4.1+) |
| **Pattern** | `@dp.materialized_view` / `@dp.temporary_view` |

| View | Grain | Use Case |
|---|---|---|
| `revenue_by_territory` | Territory + Month | Regional sales dashboards |
| `daily_sales_summary` | Day | Operational KPIs, trend analysis |
| `product_performance` | Product | Product analytics, inventory planning |

---

## Configuration-as-Code

All pipeline behavior is driven by three YAML files — no code changes needed to add tables or modify transforms.

### `config/pipelines.yml`

```yaml
bronze:
  - name: orders
    source_table: "public.orders"
    target_path: "s3a://bronze/tpch/orders"
    connection: postgres_sourcedb
    load_type: incremental
    watermark_column: modified_at
    primary_key: o_orderkey
    table_type: fact
    scd_type: 1

silver:
  - name: clean_orders
    source: "s3a://bronze/tpch/orders"
    target_path: "s3a://silver/fact_orders"
    primary_key: order_key
    transformations:
      - { source: o_orderkey, target: order_key, type: rename }
      - { source: o_totalprice, target: total_price, type: cast, expression: double }
    data_quality:
      - { name: pk_not_null, column: order_key, type: not_null, severity: critical }
      - { name: price_positive, column: total_price, type: range, expression: "0,", severity: warning }
```

### `config/connections.yml`

```yaml
postgres_sourcedb:
  type: jdbc
  url: "jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${SOURCE_DB}"
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"
  driver: "org.postgresql.Driver"
```

### `config/settings.yml`

```yaml
sla:
  bronze:
    freshness_hours: 6
    completeness_percent: 99
    alert_channels: ["slack:#data-alerts"]

feature_flags:
  enable_schema_drift_detection: true
  enable_scd_type2: true
  max_parallel_tables: 4
```

---

## Data Quality Framework

DQ rules are defined in `pipelines.yml` and converted to SQL expectations at runtime.

| Severity | SDP Mode | Behavior |
|---|---|---|
| `critical` / `error` | **FAIL** | Halt the pipeline |
| `warning` | **DROP** | Filter out bad rows |
| `info` | **WARN** | Log the issue, keep all rows |

| Rule Type | Example | Description |
|---|---|---|
| `not_null` | — | Column must not be NULL |
| `unique` | — | Column values must be unique |
| `range` | `"0,"` or `"1,100"` | Value within range |
| `regex` | `"^[A-Z]{2}"` | Pattern matching |
| `custom_sql` | `"col_a > col_b"` | Arbitrary SQL boolean |

---

## Query Engine (Trino)

Trino is a **federated SQL engine** over the Iceberg lakehouse, sharing the same JDBC catalog as Spark:

```properties
# trino/etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://host.docker.internal:5433/controldb
```

Tables created by Spark are instantly queryable via Trino:

```sql
SELECT order_key, total_price, order_status
FROM lakehouse.silver.clean_orders
WHERE total_price > 1000
ORDER BY total_price DESC LIMIT 10;
```

The OpenClaw web UI provides a **DataGrip-style SQL editor** that connects to Trino through the FastAPI backend.

---

## Data Lineage (Marquez)

[Marquez](https://marquezproject.ai/) tracks **OpenLineage-compatible** lineage:

- Automatic DAG-level lineage from Airflow
- Table-level lineage across Bronze, Silver, and Gold
- Impact analysis and root cause tracking
- Full REST API (proxied through FastAPI at `/api/lineage`)

UI: [http://localhost:8085](http://localhost:8085)

---

## Data Visualization (Superset)

[Apache Superset](https://superset.apache.org/) provides interactive dashboards:

- Pre-configured with Trino as the datasource
- Auto-bootstrapped admin user and database connections
- Accessible via the OpenClaw Workspace's Visualize page

UI: [http://localhost:8089](http://localhost:8089) (username: `admin`, password: `admin`)

---

## CI/CD (Gitea)

[Gitea](https://about.gitea.com/) is the self-hosted Git server with built-in CI/CD:

- Source control for DAGs, notebooks, and configuration
- GitHub Actions-compatible workflows via Gitea Actions
- Dedicated runner (`act_runner`) for pipeline jobs
- Integrated into the OpenClaw Workspace's CI/CD page

UI: [http://localhost:3030](http://localhost:3030)

---

## Project Structure

```
airflow-agentic-ai/
│
├── backend/                             # FastAPI Backend (API Gateway)
│   ├── Dockerfile                       #   Python 3.12-slim container
│   ├── main.py                          #   App entry: CORS, router registration
│   ├── requirements.txt                 #   fastapi, uvicorn, httpx, psycopg, boto3
│   └── routers/
│       ├── trino.py                     #   Trino query execution
│       ├── postgres.py                  #   PostgreSQL queries + schema explorer
│       ├── airflow.py                   #   Docker CLI execution
│       ├── lineage.py                   #   Marquez API proxy
│       ├── storage.py                   #   MinIO/S3 object operations
│       ├── orchestrator.py              #   Airflow REST API (JWT auth)
│       ├── health.py                    #   HTTP health checker
│       └── files.py                     #   File system browser
│
├── web-ui/                              # React Frontend (OpenClaw Workspace)
│   ├── index.html                       #   Vite entry point
│   ├── vite.config.ts                   #   Dev server + proxy configuration
│   ├── package.json                     #   React 19, Vite 6, Tailwind 4
│   ├── tsconfig.json                    #   TypeScript with @/ path alias
│   └── src/
│       ├── main.tsx                     #   React entry + route definitions
│       ├── components/
│       │   ├── Sidebar.tsx              #   Navigation rail + file/DB explorer
│       │   └── DagGraph.tsx             #   Interactive DAG visualization
│       └── app/
│           ├── page.tsx                 #   Dashboard (file explorer, stats)
│           ├── data/page.tsx            #   SQL editor (Trino + PostgreSQL)
│           ├── compute/page.tsx         #   Spark cluster monitoring
│           ├── workflows/page.tsx       #   Airflow DAG management
│           ├── lineage/page.tsx         #   Data lineage graph
│           ├── storage/page.tsx         #   MinIO object browser
│           ├── cicd/page.tsx            #   Gitea CI/CD viewer
│           ├── visualize/page.tsx       #   Superset dashboards
│           └── agent/page.tsx           #   AI agent interface
│
├── config/                              # Configuration-as-Code
│   ├── pipelines.yml                    #   Bronze/Silver/Gold table definitions
│   ├── connections.yml                  #   Database connection configs
│   ├── settings.yml                     #   SLA definitions & feature flags
│   ├── superset_config.py               #   Superset server config
│   ├── superset_bootstrap.py            #   Superset auto-provisioning
│   ├── Dockerfile.superset              #   Custom Superset image
│   └── simple_auth_manager_passwords.json
│
├── dags/                                # Airflow DAG Definitions
│   └── medallion_pipeline.py            #   Bronze → Silver → Gold orchestration
│
├── notebooks/                           # Spark Job Scripts
│   ├── _metadata.py                     #   Shared metadata engine
│   ├── spark-pipeline.yml               #   SDP pipeline manifest
│   ├── bronze/
│   │   └── ingestion.py                 #   JDBC → Parquet ingestion
│   ├── silver/
│   │   └── transformations.py           #   Parquet → Iceberg with DQ & SCD2
│   └── gold/
│       ├── revenue_analytics.py         #   Revenue materialized views
│       ├── customer_analytics.py        #   Customer analytics views
│       └── operational_metrics.py       #   Operational metrics views
│
├── scripts/                             # Utility Scripts
│   ├── setup_sourcedb.sql               #   TPC-H DDL (8 tables + indexes)
│   ├── generate_tpch_data.py            #   TPC-H data generator
│   ├── schema_migration_v2.sql          #   DB migration v2
│   ├── schema_migration_v3.sql          #   DB migration v3
│   └── schema_migration_v4.sql          #   DB migration v4
│
├── trino/                               # Trino SQL Engine
│   └── etc/
│       ├── config.properties
│       ├── jvm.config
│       ├── node.properties
│       └── catalog/
│           └── iceberg.properties       #   Iceberg JDBC catalog connector
│
├── docker-compose.yml                   # Full-stack Docker Compose (17 containers)
├── Dockerfile.airflow                   #   Custom Airflow image (PySpark + Java)
├── setup.sh                             #   JAR dependency downloader
├── .env.example                         #   Environment variable template
└── README.md                            #   This file
```

---

## Service Endpoints

| Service | URL | Port | Credentials |
|---|---|---|---|
| **OpenClaw Workspace** | [http://localhost:3010](http://localhost:3010) | 3010 | — |
| **FastAPI (Swagger)** | [http://localhost:8000/docs](http://localhost:8000/docs) | 8000 | — |
| **Airflow UI** | [http://localhost:8081](http://localhost:8081) | 8081 | `admin` / `admin` |
| **Spark Master UI** | [http://localhost:8082](http://localhost:8082) | 8082 | — |
| **Trino UI** | [http://localhost:8083](http://localhost:8083) | 8083 | — |
| **Marquez Lineage UI** | [http://localhost:8085](http://localhost:8085) | 8085 | — |
| **Superset** | [http://localhost:8089](http://localhost:8089) | 8089 | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | 9001 | See `.env` |
| **MinIO S3 API** | [http://localhost:9000](http://localhost:9000) | 9000 | See `.env` |
| **Marquez API** | [http://localhost:5002](http://localhost:5002) | 5002 | — |
| **Gitea** | [http://localhost:3030](http://localhost:3030) | 3030 | — |
| **Gitea SSH** | `ssh://localhost:2222` | 2222 | — |

---

## Development

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The `--reload` flag enables hot-reloading on file changes.

### Frontend

```bash
cd web-ui
npm install
npm run dev        # Starts Vite dev server on :3010
npm run build      # Production build → dist/
npm run preview    # Preview production build
```

Vite provides sub-second hot module replacement (HMR).

### Docker (Production)

```bash
# Start everything
docker compose up -d

# View logs
docker compose logs -f openclaw-api

# Rebuild after changes
docker compose up -d --build openclaw-api
```

---

## Schema Migrations

| File | Description |
|---|---|
| `scripts/setup_sourcedb.sql` | TPC-H OLTP schema (8 tables + indexes) |
| `scripts/schema_migration_v2.sql` | AI runtime schema (`ai.audit_logs`, `ai.execution_metrics`) |
| `scripts/schema_migration_v3.sql` | Extended metadata tables, pipeline registry |
| `scripts/schema_migration_v4.sql` | Latest schema additions |

---

## TPC-H Data Generator

```bash
python3 scripts/generate_tpch_data.py --scale 0.1
```

| Flag | Default | Description |
|---|---|---|
| `--scale` | `0.1` | Scale factor (`0.01`=tiny, `0.1`=~800K rows, `1.0`=~8M rows) |
| `--fresh` | `false` | Truncate all tables before loading |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add your table to `config/pipelines.yml` — no code changes needed for new Bronze/Silver tables
4. Commit your changes (`git commit -m 'feat: add amazing-feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**OpenClaw** — Agentic Data Platform

*FastAPI · React 19 · Vite · Airflow 3.0 · Spark 4.1.1 · Iceberg · Trino · Marquez · Superset · Gitea*

</div>
