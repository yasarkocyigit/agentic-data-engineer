
<div align="center">

# Agentic Data Engineer

### Metadata-Driven Medallion Lakehouse

[![Airflow 3.0](https://img.shields.io/badge/Airflow-3.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Spark 4.1.1](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.10-4E8EE9?logo=apacheiceberg&logoColor=white)](https://iceberg.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Trino](https://img.shields.io/badge/Trino-SQL%20Engine-DD00A1?logo=trino&logoColor=white)](https://trino.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

*A production-grade, config-as-code data lakehouse that transforms raw OLTP data into analytics-ready Gold tables — fully orchestrated, fully observable, zero hardcoded logic.*

---

[Architecture](#architecture) · [Quick Start](#quick-start) · [Pipeline Flow](#medallion-pipeline-flow) · [Configuration](#configuration-as-code) · [Project Structure](#project-structure) · [Contributing](#contributing)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Medallion Pipeline Flow](#medallion-pipeline-flow)
- [Configuration-as-Code](#configuration-as-code)
- [Data Quality Framework](#data-quality-framework)
- [Query Engine (Trino)](#query-engine-trino)
- [Data Lineage (Marquez)](#data-lineage-marquez)
- [Project Structure](#project-structure)
- [Service Endpoints](#service-endpoints)
- [Schema Migrations](#schema-migrations)
- [TPC-H Data Generator](#tpc-h-data-generator)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

**Agentic Data Engineer** is a fully containerized, end-to-end data lakehouse platform that implements the **Medallion Architecture** (Bronze → Silver → Gold) using entirely **declarative, YAML-driven pipelines**.

The platform ingests transactional data from PostgreSQL (TPC-H benchmark), transforms and validates it through configurable rules, and materializes analytics-ready views — all orchestrated by Apache Airflow and computed on a distributed Apache Spark cluster.

### Key Principles

| Principle | Description |
|---|---|
| **Config-as-Code** | All pipeline logic, transformations, DQ rules, and connections defined in YAML — zero hardcoded SQL |
| **Metadata-Driven** | A shared metadata engine (`_metadata.py`) resolves configs, applies transforms, and manages runtime state |
| **Declarative Gold Layer** | Gold tables use Spark 4.1+ Declarative Pipelines (SDP) with `@dp.materialized_view` decorators |
| **Schema Evolution** | Iceberg table format with SCD Type 2 tracking for slowly changing dimensions |
| **Full Observability** | OpenLineage integration via Marquez for end-to-end data lineage |
| **Federated SQL** | Trino provides a unified SQL interface over the entire Iceberg lakehouse |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION LAYER                         │
│                     Apache Airflow 3.0 (DAG)                       │
│         ┌────────────┬──────────────┬──────────────┐               │
│         │ read_config│  run_bronze  │  run_silver   │              │
│         │  (Python)  │(SparkSubmit) │(SparkSubmit)  │              │
│         └─────┬──────┴──────┬───────┴──────┬────────┘              │
└───────────────┼─────────────┼──────────────┼───────────────────────┘
                │             │              │
┌───────────────▼─────────────▼──────────────▼───────────────────────┐
│                         COMPUTE LAYER                              │
│                   Apache Spark 4.1.1 Cluster                       │
│               ┌──────────┐    ┌──────────────┐                     │
│               │  Master  │◄───│   Worker(s)  │                     │
│               └──────────┘    └──────────────┘                     │
└────────────────────────────────────────────────────────────────────┘
                │             │              │
   ┌────────────▼──┐  ┌──────▼──────┐  ┌────▼─────────┐
   │    BRONZE     │  │   SILVER    │  │    GOLD      │
   │   (Parquet)   │  │  (Iceberg)  │  │  (Iceberg)   │
   │   8 tables    │  │  4 tables   │  │  3 views     │
   │   Raw dump    │  │  DQ + SCD2  │  │  SDP @dp.mv  │
   └───────┬───────┘  └──────┬──────┘  └──────┬───────┘
           └─────────────────┼─────────────────┘
                             │
┌────────────────────────────▼───────────────────────────────────────┐
│                        STORAGE LAYER                               │
│                    MinIO (S3-Compatible)                            │
│        ┌──────┐  ┌──────┐  ┌──────┐  ┌───────────┐               │
│        │bronze│  │silver│  │ gold │  │ warehouse │               │
│        └──────┘  └──────┘  └──────┘  └───────────┘               │
└────────────────────────────────────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐    ┌─────▼──────┐    ┌──────▼──────┐
   │   Trino     │    │  Marquez   │    │ PostgreSQL  │
   │ SQL Engine  │    │  Lineage   │    │  Metadata   │
   │ (Iceberg    │    │ (OpenLine- │    │  (JDBC Cat- │
   │  Catalog)   │    │   age)     │    │   alog)     │
   └─────────────┘    └────────────┘    └─────────────┘
```

---

## Tech Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **Orchestration** | Apache Airflow | 3.0 | DAG scheduling, task orchestration, API server |
| **Compute** | Apache Spark | 4.1.1 | Distributed data processing, SDP pipelines |
| **Table Format** | Apache Iceberg | 1.10.1 | ACID transactions, schema evolution, time travel |
| **Object Storage** | MinIO | Latest | S3-compatible storage for all lakehouse layers |
| **SQL Engine** | Trino | Latest | Federated SQL queries over Iceberg tables |
| **Data Lineage** | Marquez | Latest | OpenLineage-based lineage and metadata tracking |
| **Metadata Store** | PostgreSQL | 17 | JDBC Iceberg catalog, Airflow metadata, AI runtime data |
| **Source Database** | PostgreSQL | 17 | TPC-H OLTP source for bronze ingestion |
| **Containerization** | Docker Compose | — | Full-stack orchestration with 9 services |
| **Language** | Python / PySpark | 3.x / 4.1.1 | Pipeline logic, metadata engine, data generation |

---

## Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose v2+)
- **PostgreSQL 17** running on `localhost:5433` (metadata & source database)
- **8 GB RAM** minimum recommended for Docker
- **Python 3.10+** (for TPC-H data generation only)

### 1. Clone the Repository

```bash
git clone https://github.com/yasarkocyigit/agentic-data-engineer.git
cd agentic-data-engineer
```

### 2. Configure Environment Variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
# Edit .env with your PostgreSQL password, MinIO credentials, and Airflow secrets
```

### 3. Download Required JARs

The setup script downloads all necessary JAR dependencies (Iceberg runtime, AWS SDK, Hadoop AWS, PostgreSQL JDBC):

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
| `postgresql` | 42.7.5 | JDBC driver for source database |

</details>

### 4. Set Up Source Database

Create the TPC-H source database and generate sample data:

```bash
# Create schema
psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE sourcedb;"
psql -h localhost -p 5433 -U postgres -d sourcedb -f scripts/setup_sourcedb.sql

# Generate TPC-H data (~800K rows at default scale 0.1)
pip install psycopg2-binary
python3 scripts/generate_tpch_data.py --scale 0.1
```

### 5. Launch the Platform

```bash
docker compose up -d
```

This brings up **9 services**: Spark Master, Spark Worker, MinIO (+ auto-bucket provisioning), Trino, Marquez (API + DB + Web), Airflow (Init + Webserver + Scheduler + DAG Processor).

### 6. Run the Pipeline

1. Open **Airflow UI** at [http://localhost:8081](http://localhost:8081) (username: `admin`, password: `admin`)
2. Enable the `medallion_pipeline` DAG
3. Trigger a manual run — watch data flow through Bronze → Silver → Gold

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

**Tables ingested:** `orders`, `lineitem`, `customer`, `part`, `supplier`, `nation`, `region`, `partsupp`

Each record is enriched with metadata columns: `_ingested_at`, `_source_table`, `_pipeline`.

### Silver Layer — Transformations & Data Quality

| Attribute | Detail |
|---|---|
| **Source** | Bronze Parquet tables |
| **Format** | Apache Iceberg (ACID, schema evolution) |
| **Target** | `lakehouse.silver.<table>` |
| **DQ Engine** | YAML-configured rules converted to SQL expectations |

**Transformations applied** (all config-driven):
- Column renaming (`o_orderkey` → `order_key`)
- Type casting (`DECIMAL` → `DOUBLE`)
- String trimming, null handling
- SCD Type 2 tracking (`_valid_from`, `_valid_to`, `_is_current`)
- Deduplication by primary key (keep latest `_ingested_at`)

### Gold Layer — Analytics Materialized Views

| Attribute | Detail |
|---|---|
| **Framework** | Spark Declarative Pipelines (SDP, Spark 4.1+) |
| **Pattern** | `@dp.materialized_view` / `@dp.temporary_view` |
| **Auto-Resolution** | Dependencies resolved from `spark.read` / `spark.table` references |

**Materialized views:**

| View | Grain | Use Case |
|---|---|---|
| `revenue_by_territory` | Territory + Month | Regional sales dashboards |
| `daily_sales_summary` | Day | Operational KPIs, trend analysis |
| `product_performance` | Product | Product analytics, inventory planning |

---

## Configuration-as-Code

All pipeline behavior is driven by three YAML files — **no Python code changes needed** to add tables, modify transforms, or adjust SLAs.

### `config/pipelines.yml`

Defines every table in every layer with full transformation and DQ rules:

```yaml
bronze:
  - name: orders
    source_table: "public.orders"
    target_path: "s3a://bronze/tpch/orders"
    connection: postgres_sourcedb
    load_type: incremental         # full | incremental
    watermark_column: modified_at
    primary_key: o_orderkey
    table_type: fact               # fact | dimension | bridge
    scd_type: 1                    # 1 (overwrite) | 2 (history tracking)
    is_active: true
    priority: 10

silver:
  - name: clean_orders
    source: "s3a://bronze/tpch/orders"
    target_path: "s3a://silver/fact_orders"
    primary_key: order_key
    scd_type: 1
    transformations:
      - { source: o_orderkey,    target: order_key,    type: rename }
      - { source: o_totalprice,  target: total_price,  type: cast,  expression: double }
      - { source: o_orderstatus, target: order_status, type: trim }
    data_quality:
      - { name: pk_not_null,    column: order_key,   type: not_null, severity: critical }
      - { name: price_positive, column: total_price, type: range,    expression: "0,", severity: warning }
```

### `config/connections.yml`

Externalized connection strings with environment variable resolution:

```yaml
postgres_sourcedb:
  type: jdbc
  url: "jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${SOURCE_DB}"
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"
  driver: "org.postgresql.Driver"
  options:
    fetchsize: 10000
    batchsize: 5000
```

### `config/settings.yml`

SLA definitions and feature flags:

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

DQ rules are defined inline in `pipelines.yml` and converted to SQL expectations at runtime by the metadata engine.

### Severity to Action Mapping

| Severity | SDP Mode | Behavior |
|---|---|---|
| `critical` / `error` | **FAIL** | Halt the pipeline immediately |
| `warning` | **DROP** | Filter out bad rows, continue processing |
| `info` | **WARN** | Log the issue, keep all rows |

### Supported Rule Types

| Type | Example Expression | Description |
|---|---|---|
| `not_null` | — | Column must not be NULL |
| `unique` | — | Column values must be unique (monitored) |
| `range` | `"0,"` or `"1,100"` | Column must be within range [min, max] |
| `regex` | `"^[A-Z]{2}"` | Column must match regex pattern |
| `custom_sql` | `"col_a > col_b"` | Arbitrary SQL boolean expression |

---

## Query Engine (Trino)

Trino is configured as a **federated SQL engine** over the Iceberg lakehouse, sharing the same JDBC catalog as Spark:

```properties
# trino/etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://host.docker.internal:5433/controldb
```

Both Spark and Trino read/write through the same `lakehouse` catalog — tables created by Spark are instantly queryable via Trino.

```sql
-- Query Silver Iceberg tables via Trino
SELECT order_key, total_price, order_status
FROM lakehouse.silver.clean_orders
WHERE total_price > 1000
ORDER BY total_price DESC
LIMIT 10;
```

---

## Data Lineage (Marquez)

[Marquez](https://marquezproject.ai/) provides **OpenLineage-compatible** data lineage tracking:

- Automatic DAG-level lineage from Airflow
- Table-level lineage across Bronze, Silver, and Gold
- Impact analysis and root cause tracking
- Full REST API for programmatic access

Access the lineage UI at [http://localhost:8085](http://localhost:8085).

---

## Project Structure

```
airflow-agentic-ai/
│
├── config/                          # Configuration-as-Code
│   ├── pipelines.yml                #   Pipeline definitions (Bronze/Silver/Gold)
│   ├── connections.yml              #   Database connection configs
│   └── settings.yml                 #   SLA definitions & feature flags
│
├── dags/                            # Airflow DAG Definitions
│   └── medallion_pipeline.py        #   Main DAG: Config → Bronze → Silver → Gold
│
├── notebooks/                       # Spark Job Scripts
│   ├── _metadata.py                 #   Shared metadata engine (YAML reader, DQ, transforms)
│   ├── spark-pipeline.yml           #   SDP pipeline manifest
│   ├── bronze/
│   │   └── ingestion.py             #   JDBC → Parquet ingestion
│   ├── silver/
│   │   └── transformations.py       #   Parquet → Iceberg with DQ & SCD2
│   └── gold/
│       ├── revenue_analytics.py     #   Revenue materialized views (SDP)
│       ├── customer_analytics.py    #   Customer analytics views (SDP)
│       └── operational_metrics.py   #   Operational metrics views (SDP)
│
├── scripts/                         # Utility Scripts
│   ├── setup_sourcedb.sql           #   TPC-H DDL (8 tables + indexes)
│   ├── generate_tpch_data.py        #   TPC-H data generator (configurable scale)
│   ├── schema_migration_v2.sql      #   DB migration v2
│   ├── schema_migration_v3.sql      #   DB migration v3
│   └── schema_migration_v4.sql      #   DB migration v4
│
├── trino/                           # Trino SQL Engine Configuration
│   └── etc/
│       ├── config.properties        #   Trino server config
│       ├── jvm.config               #   JVM settings
│       ├── node.properties          #   Node identity
│       ├── log.properties           #   Logging
│       └── catalog/
│           └── iceberg.properties   #   Iceberg connector (shared JDBC catalog)
│
├── docker-compose.yml               # Full-stack Docker Compose (9 services)
├── Dockerfile.airflow               # Custom Airflow image (PySpark + Java + AWS SDK)
├── setup.sh                         # JAR dependency downloader
├── .env.example                     # Environment variable template
├── .gitignore                       # Git ignore rules
└── README.md                        # This file
```

---

## Service Endpoints

Once `docker compose up -d` completes, the following UIs are available:

| Service | URL | Credentials |
|---|---|---|
| **Airflow UI** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` |
| **Spark Master UI** | [http://localhost:8082](http://localhost:8082) | — |
| **Trino UI** | [http://localhost:8083](http://localhost:8083) | — |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | See `.env` |
| **Marquez Lineage UI** | [http://localhost:8085](http://localhost:8085) | — |
| **Marquez API** | [http://localhost:5002](http://localhost:5002) | — |
| **MinIO S3 API** | [http://localhost:9000](http://localhost:9000) | See `.env` |

---

## Schema Migrations

The `scripts/` directory contains incremental SQL migrations for the control database (`controldb`):

| File | Description |
|---|---|
| `setup_sourcedb.sql` | TPC-H OLTP schema (8 tables + indexes) |
| `schema_migration_v2.sql` | AI runtime schema (`ai.audit_logs`, `ai.execution_metrics`) |
| `schema_migration_v3.sql` | Extended metadata tables, pipeline registry |
| `schema_migration_v4.sql` | Latest schema additions |

---

## TPC-H Data Generator

A built-in Python script generates realistic [TPC-H](http://www.tpc.org/tpch/) benchmark data directly into PostgreSQL:

```bash
python3 scripts/generate_tpch_data.py --scale 0.1
```

| Flag | Default | Description |
|---|---|---|
| `--scale` | `0.1` | Scale factor (`0.01`=tiny, `0.1`=~800K rows, `1.0`=~8M rows) |
| `--fresh` | `false` | Truncate all tables before loading |

**Generated tables at SF=0.1:**

| Table | Approximate Rows | Type |
|---|---|---|
| `region` | 5 | Dimension |
| `nation` | 25 | Dimension |
| `supplier` | 1,000 | Dimension |
| `customer` | 15,000 | Dimension |
| `part` | 20,000 | Dimension |
| `partsupp` | 80,000 | Bridge |
| `orders` | 150,000 | Fact |
| `lineitem` | ~600,000 | Fact |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add your table to `config/pipelines.yml` — **no code changes needed** for new Bronze/Silver tables
4. Commit your changes (`git commit -m 'feat: add amazing-feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built by the Agentic Data Engineering Team**

*Airflow 3.0 · Spark 4.1.1 · Iceberg 1.10 · MinIO · Trino · Marquez*

</div>
