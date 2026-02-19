# PostgreSQL Database Reference
> Son güncelleme: 2026-02-19 | Server: `localhost:5433` (Mac'te yerel)

---

## Fiziksel Server
Mac'te çalışan tek PostgreSQL instance → `localhost:5433`

---

## Database 1: `controldb`

**Kim bağlanıyor:**
- Airflow (webserver, scheduler, dag-processor) → `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- Spark (Iceberg JDBC catalog) → `ICEBERG_CATALOG_URI`
- FastAPI backend (Data Explorer) → `routers/postgres.py`

### public şeması — Airflow Internal Tables (58 tablo, otomatik yönetilir)

| Grup | Başlıca Tablolar | Neden |
|---|---|---|
| **DAG state** | `dag`, `dag_run`, `dag_version` | Airflow'un DAG registry'si |
| **Task state** | `task_instance`, `task_instance_history`, `xcom` | Her task'ın çalışma durumu ve XCom değerleri |
| **Scheduler** | `job`, `callback_request` | Scheduler heartbeat ve iş kuyruğu |
| **Auth** | `ab_user`, `ab_role`, `ab_permission`, `session` | Airflow kullanıcı ve oturum yönetimi |
| **Audit** | `log` | Her DAG trigger ve UI aksiyonu |
| **Assets** | `asset`, `asset_event`, `asset_dag_run_queue` | Airflow 3.x asset-based scheduling |
| **Iceberg Catalog** | `iceberg_tables`, `iceberg_namespace_properties` | Spark ve Trino'nun Iceberg tablo registry'si |

> ⚠️ Bu tabloları elle değiştirme. Airflow ve Spark otomatik yönetiyor.

### ai şeması — Agent Memory & Observability (7 tablo)

> ⚠️ Bu tablolar şu an boş (0 kayıt). Agentic AI özelliği henüz bu tabloları aktif olarak doldurmaya başlamadı. Altyapı hazır, henüz bağlantı kurulmadı.

| Tablo | Amaç | Aktif? |
|---|---|---|
| `ai.memory` | pgvector semantic hafıza — agent'ın "ne öğrendiği" | ❌ Boş |
| `ai.conversation_history` | Chat session mesajları | ❌ Boş |
| `ai.traces` | LLM call ve tool execution süreleri | ❌ Boş |
| `ai.execution_metrics` | Spark run metrikleri (rows_read, bytes_processed) | ❌ Boş |
| `ai.audit_logs` | Kim ne zaman ne yaptı | ❌ Boş |
| `ai.approval_policies` | HITL: hangi işlem onay gerektirir | ❌ Boş |
| `ai.approval_requests` | Bekleyen onay istekleri | ❌ Boş |

---

## Database 2: `sourcedb`

**Kim bağlanıyor:** Spark Bronze job → JDBC ile okur

**Neden:** TPC-H demo verisi buradadır. Bronze layer bunu okuyup MinIO'ya Parquet olarak yazar.

| Tablo | Tür | Açıklama |
|---|---|---|
| `public.orders` | Fact | Siparişler |
| `public.lineitem` | Fact | Sipariş kalemleri |
| `public.customer` | Dim | Müşteriler |
| `public.part` | Dim | Ürünler |
| `public.supplier` | Dim | Tedarikçiler |
| `public.nation` | Dim | Ülkeler |
| `public.region` | Dim | Bölgeler |
| `public.partsupp` | Bridge | Part-Supplier ilişkisi |

---

## Database 3: `agent_db`

**Kim bağlanıyor:** FastAPI backend (`openclaw-api` container)

**Neden:** Backend'in kendi audit ve memory kayıtları. `controldb`'den bağımsız tutulmuş.

| Tablo | Satır Sayısı | Açıklama |
|---|---|---|
| `public.agent_audit_logs` | 18 (aktif büyüyor) | API çağrı logları |
| `public.memory_store` | 85 (aktif büyüyor) | Backend'in lokal memory'si |

---

## Database 4: `marquez` (Docker-internal)

**Sunucu:** Docker container `marquez-db:5432` — Mac'e port açılmamış

**Kim bağlanıyor:** Marquez container (OpenLineage) — sadece kendi içinden

**Neden:** Airflow'un OpenLineage transport'u her DAG run sonrası lineage event gönderir.  
Bunu değiştirme, Marquez tamamen kendi yönetiyor.

---

## Kaldırılan Şemalar

| Şema | Kaldırma Tarihi | Neden |
|---|---|---|
| `config.*` (11 tablo) | 2026-02-19 | YAML-driven mimariye geçildi. `pipelines.yml` ve `connections.yml` bu işi yapıyor. DB-driven config'e gerek kalmadı. |

---

## Özet Şema

```
localhost:5433
├── controldb
│   ├── public.*        ← Airflow (58 tablo) + Iceberg catalog (2 tablo)
│   └── ai.*            ← Agent observability (7 tablo, şu an boş)
│
├── sourcedb
│   └── public.*        ← TPC-H kaynak verisi (8 tablo, Spark okur)
│
└── agent_db
    └── public.*        ← FastAPI backend logs (2 tablo, aktif)

Docker network içi:
└── marquez-db:5432
    └── marquez.*       ← OpenLineage data (Marquez yönetir)
```
