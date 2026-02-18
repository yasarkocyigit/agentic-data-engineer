# Native Airflow Orchestration — Architecture & Implementation Guide

> **Airflow as a Headless Service.** Users never see the Airflow UI.
> All orchestration features are embedded natively in our product UI.

---

## 1. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        BROWSER (Next.js)                         │
│  ┌─────────┐ ┌───────────┐ ┌──────────┐ ┌───────┐ ┌──────────┐  │
│  │ DAG List│ │ Run History│ │Graph View│ │Grid   │ │Logs      │  │
│  │ + Filters│ │ + Status  │ │(ReactFlow│ │View   │ │Viewer    │  │
│  │ + Search │ │ + Trigger │ │+ dagre)  │ │(Table)│ │(Tail)    │  │
│  └────┬─────┘ └────┬──────┘ └────┬─────┘ └──┬────┘ └────┬─────┘  │
│       │            │            │           │           │        │
│       └────────────┴────────────┴───────────┴───────────┘        │
│                              │                                    │
│                    fetch("/api/orchestrator/...")                  │
└──────────────────────────────┼───────────────────────────────────┘
                               │ HTTPS (JWT from OIDC)
┌──────────────────────────────┼───────────────────────────────────┐
│              NEXT.JS BACKEND (API Routes)                        │
│  ┌───────────┐ ┌─────────┐ ┌───────────┐ ┌────────┐             │
│  │ Auth MW   │→│ RBAC    │→│ Airflow   │→│ Audit  │             │
│  │(JWT verify│ │(role    │ │Client     │ │Logger  │             │
│  │+ user map)│ │checks)  │ │(HTTP+JWT) │ │(DB log)│             │
│  └───────────┘ └─────────┘ └─────┬─────┘ └────────┘             │
│                                  │                               │
│  ┌──────────────┐    ┌───────────┼────────────┐                  │
│  │ Product DB   │    │  Service Account Cache  │                  │
│  │(PostgreSQL)  │    │  (JWT token + refresh)  │                  │
│  │• pipeline_meta    │                         │                  │
│  │• audit_log   │    └───────────┬────────────┘                  │
│  │• favorites   │                │                               │
│  └──────────────┘                │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │ HTTP (Bearer token)
                                   │ Internal network only
┌──────────────────────────────────┼───────────────────────────────┐
│                    AIRFLOW 3.x (Private)                          │
│  ┌─────────────┐  ┌───────────┐  ┌─────────────────┐            │
│  │ API Server  │  │ Scheduler │  │ DAG Processor   │            │
│  │ :8080       │  │           │  │                 │            │
│  │ /api/v2/*   │  │           │  │                 │            │
│  └─────────────┘  └───────────┘  └─────────────────┘            │
│                         │                                        │
│               ┌─────────┴──────────┐                             │
│               │   PostgreSQL (DB)  │                             │
│               └────────────────────┘                             │
└──────────────────────────────────────────────────────────────────┘
```

**Component Responsibilities:**

| Component | Responsibility |
|---|---|
| **Next.js Frontend** | All UI screens — DAG list, graph view, grid view, logs viewer, run history |
| **Next.js API Routes** | Auth enforcement, RBAC, rate limiting, audit logging, Airflow API proxy |
| **Airflow Client** | Singleton that manages JWT token lifecycle + makes typed HTTP requests to Airflow |
| **Product DB** | Pipeline metadata, ownership, tags, environments, favorites, audit log |
| **Airflow** | Headless orchestration — scheduling, task execution, log storage |

---

## 2. API Contract — Orchestrator API

All endpoints are prefixed with `/api/orchestrator`.

### 2.1 DAGs

```
GET  /api/orchestrator/dags
     ?page=1&limit=25&search=medal&tags=lakehouse&paused=false&owner=agentic-ai
     → { dags: DAG[], total: number, page: number, limit: number }

GET  /api/orchestrator/dags/:dagId
     → { dag: DAGDetail }

PATCH /api/orchestrator/dags/:dagId/pause
      Body: { is_paused: boolean }
      → { dag_id, is_paused }
```

**Response: DAG**
```json
{
  "dag_id": "medallion_pipeline",
  "display_name": "medallion_pipeline",
  "description": "Medallion Pipeline: Bronze → Silver",
  "is_paused": true,
  "schedule": "0 0 * * *",
  "schedule_description": "At 00:00",
  "owners": ["agentic-ai"],
  "tags": ["medallion", "lakehouse", "iceberg"],
  "last_run": { "run_id": "manual__...", "state": "failed", "end_date": "..." },
  "next_run": "2026-02-14T00:00:00Z",
  "max_active_runs": 1,
  "file_path": "medallion_pipeline.py",
  "environment": "dev",
  "is_favorite": false
}
```

### 2.2 DAG Runs

```
GET  /api/orchestrator/dags/:dagId/runs
     ?page=1&limit=20&state=failed
     → { runs: DAGRun[], total: number }

POST /api/orchestrator/dags/:dagId/runs
     Body: { conf?: object, note?: string }
     → { run_id, state, queued_at }

GET  /api/orchestrator/dags/:dagId/runs/:runId
     → { run: DAGRunDetail }
```

**Response: DAGRun**
```json
{
  "run_id": "manual__2026-02-08T02:24:00.222544+00:00_3WWWt7ID",
  "state": "failed",
  "run_type": "manual",
  "triggered_by": "cli",
  "queued_at": "2026-02-08T02:24:00.392527Z",
  "start_date": "2026-02-08T02:24:00.977428Z",
  "end_date": "2026-02-08T02:26:25.099757Z",
  "duration_seconds": 144.12,
  "conf": {},
  "note": null
}
```

### 2.3 Tasks (Graph Structure)

```
GET  /api/orchestrator/dags/:dagId/structure
     → { nodes: TaskNode[], edges: Edge[] }
```

**Response: Graph Structure**
```json
{
  "nodes": [
    { "id": "read_config", "label": "read_config", "operator": "PythonOperator", "ui_color": "#ffefeb" },
    { "id": "run_bronze_ingestion", "label": "run_bronze_ingestion", "operator": "SparkSubmitOperator", "ui_color": "#fff" },
    { "id": "run_silver_transforms", "label": "run_silver_transforms", "operator": "SparkSubmitOperator", "ui_color": "#fff" },
    { "id": "log_success", "label": "log_success", "operator": "PythonOperator", "ui_color": "#ffefeb", "trigger_rule": "all_success" },
    { "id": "log_failure", "label": "log_failure", "operator": "PythonOperator", "ui_color": "#ffefeb", "trigger_rule": "one_failed" }
  ],
  "edges": [
    { "source": "read_config", "target": "run_bronze_ingestion" },
    { "source": "run_bronze_ingestion", "target": "run_silver_transforms" },
    { "source": "run_silver_transforms", "target": "log_success" },
    { "source": "run_silver_transforms", "target": "log_failure" }
  ]
}
```

### 2.4 Task Instances (for a Run)

```
GET  /api/orchestrator/dags/:dagId/runs/:runId/tasks
     → { tasks: TaskInstance[] }
```

**Response: TaskInstance**
```json
{
  "task_id": "run_bronze_ingestion",
  "state": "success",
  "start_date": "2026-02-08T02:24:03.159Z",
  "end_date": "2026-02-08T02:24:13.401Z",
  "duration": 10.24,
  "try_number": 1,
  "max_tries": 1,
  "operator": "SparkSubmitOperator",
  "map_index": -1,
  "pool": "default_pool"
}
```

### 2.5 Task Logs

```
GET  /api/orchestrator/dags/:dagId/runs/:runId/tasks/:taskId/logs
     ?try_number=1&map_index=-1&offset=0
     → { content: string, offset: number, is_complete: boolean }
```

### 2.6 Task Actions (Optional)

```
POST /api/orchestrator/dags/:dagId/runs/:runId/tasks/:taskId/clear
     Body: { include_downstream?: boolean }
     → { message: string }
```

### 2.7 Health

```
GET  /api/orchestrator/health
     → { scheduler: HealthStatus, db: HealthStatus, dag_processor: HealthStatus }
```

### 2.8 Favorites & Metadata (Product DB)

```
POST   /api/orchestrator/dags/:dagId/favorite    → { is_favorite: true }
DELETE /api/orchestrator/dags/:dagId/favorite    → { is_favorite: false }

PATCH  /api/orchestrator/dags/:dagId/metadata
       Body: { environment?: string, custom_tags?: string[], owner_team?: string }
       → { updated: true }
```

---

## 3. Mapping Table: Orchestrator → Airflow

| Orchestrator Endpoint | Method | Airflow v2 Endpoint | Notes |
|---|---|---|---|
| `GET /dags` | GET | `/api/v2/dags?limit=N&offset=M` | Map `paused` filter, merge product metadata |
| `GET /dags/:id` | GET | `/api/v2/dags/:id` | Enriched with product DB metadata |
| `PATCH /dags/:id/pause` | PATCH | `/api/v2/dags/:id` body: `{is_paused}` | |
| `GET /dags/:id/runs` | GET | `/api/v2/dags/:id/dagRuns?order_by=-start_date` | |
| `POST /dags/:id/runs` | POST | `/api/v2/dags/:id/dagRuns` body: `{conf}` | Validate `conf` schema |
| `GET /dags/:id/runs/:rid` | GET | `/api/v2/dags/:id/dagRuns/:rid` | |
| `GET /dags/:id/structure` | GET | `/api/v2/dags/:id/tasks` | Transform `downstream_task_ids` → edges |
| `GET /dags/:id/runs/:rid/tasks` | GET | `/api/v2/dags/:id/dagRuns/:rid/taskInstances` | |
| `GET /.../tasks/:tid/logs` | GET | `/api/v2/dags/:id/dagRuns/:rid/taskInstances/:tid/logs` | Stream/paginate |
| `POST /.../tasks/:tid/clear` | POST | `/api/v2/dags/:id/dagRuns/:rid/taskInstances/:tid/clear` | |
| `GET /health` | GET | `/api/v2/monitor/health` | No auth needed (configurable) |

**Airflow v2 vs Airflow 3 Adapter:**
- Airflow 3: `/api/v2/dags`, `dagRuns` field
- Airflow 2: `/api/v1/dags`, same field names
- Strategy: A single env var `AIRFLOW_API_VERSION` (`v1`\|`v2`) controls the base path. Response shapes are 95% compatible.

---

## 4. Data Model (Product DB)

```
┌────────────────────────┐       ┌──────────────────────────┐
│    pipeline_metadata   │       │      audit_log           │
├────────────────────────┤       ├──────────────────────────┤
│ dag_id (PK, VARCHAR)   │       │ id (PK, UUID)            │
│ environment (VARCHAR)  │       │ timestamp (TIMESTAMPTZ)  │
│ owner_team (VARCHAR)   │       │ user_id (VARCHAR)        │
│ custom_tags (JSONB)    │  ┌───→│ dag_id (VARCHAR)         │
│ description (TEXT)     │  │    │ run_id (VARCHAR, NULL)   │
│ created_at (TIMESTAMP) │  │    │ action (VARCHAR)         │
│ updated_at (TIMESTAMP) │──┘    │ details (JSONB)          │
└────────────────────────┘       │ ip_address (VARCHAR)     │
                                 └──────────────────────────┘
┌────────────────────────┐
│   user_favorites       │       ┌──────────────────────────┐
├────────────────────────┤       │   saved_run_configs      │
│ id (PK, UUID)          │       ├──────────────────────────┤
│ user_id (VARCHAR)      │       │ id (PK, UUID)            │
│ dag_id (VARCHAR)       │       │ dag_id (VARCHAR)         │
│ created_at (TIMESTAMP) │       │ name (VARCHAR)           │
│ UNIQUE(user_id,dag_id) │       │ conf (JSONB)             │
└────────────────────────┘       │ created_by (VARCHAR)     │
                                 │ created_at (TIMESTAMP)   │
                                 └──────────────────────────┘
```

**`audit_log.action` values:** `trigger_run`, `pause_dag`, `unpause_dag`, `clear_task`, `view_logs`, `update_metadata`

---

## 5. UI Spec — Screens & Components

### 5.1 Orchestration Dashboard (`/workflows`)

```
┌─────────────────────────────────────────────────────────────┐
│ 🔧 Pipelines                    ● Scheduler ● DB ● Proc    │
│─────────────────────────────────────────────────────────────│
│ [Search...] [Tags ▼] [Status ▼] [Owner ▼]     [⟳ Refresh] │
│─────────────────────────────────────────────────────────────│
│ ★  DAG ID               Schedule   Last Run     Status  ▶  │
│────────────────────────────────────────────────────────────│
│ ★  medallion_pipeline   @daily     2m ago  ● FAILED    ▶ ⏸ │
│ ☆  data_quality_checks  @hourly    15m ago ● SUCCESS   ▶ ⏸ │
│ ☆  dbt_transforms       Manual     1d ago  ● SUCCESS   ▶ ⏸ │
│─────────────────────────────────────────────────────────────│
│                     Page 1 of 3  [< 1 2 3 >]               │
└─────────────────────────────────────────────────────────────┘
```

**Components:**
- `<PipelineHealthBar />` — real-time scheduler/db/processor status dots
- `<FilterToolbar />` — search, tag filter, status filter, owner filter
- `<DAGTable />` — sortable, with favorite toggle, status badge, action buttons
- `<PaginationControls />`

### 5.2 DAG Detail Page (`/workflows/:dagId`)

```
┌─────────────────────────────────────────────────────────────┐
│ ← Back   medallion_pipeline                    [▶ Trigger]  │
│ Medallion Pipeline: Bronze → Silver — SparkSubmitOperator   │
│ ⏱ @daily · 👤 agentic-ai · 🏷 medallion, lakehouse        │
│─────────────────────────────────────────────────────────────│
│  [Runs]  [Graph]  [Details]                                 │
│─────────────────────────────────────────────────────────────│
│                                                             │
│  RUNS LIST (default tab)                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Run ID              State    Start         Duration  │   │
│  │ manual__2026-0...   ● FAIL   Feb 8 02:24   2m 24s   │   │
│  │ scheduled__202...   ● OK     Feb 7 00:00   3m 12s   │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 Run Detail Page (`/workflows/:dagId/runs/:runId`)

Two tabs: **Graph View** and **Grid View**.

**Graph View tab:**
```
┌─────────────────────────────────────────────────────────────┐
│ Run: manual__2026-02-08...  State: ● FAILED  Duration: 2m  │
│─────────────────────────────────────────────────────────────│
│  [Graph]  [Grid]                                            │
│─────────────────────────────────────────────────────────────│
│                                                             │
│   ┌──────────┐    ┌─────────────┐    ┌──────────────┐       │
│   │read_config│──→│run_bronze   │──→│run_silver    │       │
│   │  ✅ 0.8s  │    │  ✅ 10.2s    │    │  ❌ 120.1s   │       │
│   └──────────┘    └─────────────┘    └──────┬───────┘       │
│                                        ┌────┴────┐          │
│                                   ┌────┴───┐ ┌───┴────┐     │
│                                   │log_ok  │ │log_fail│     │
│                                   │  ⏭ skip│ │ ✅ 0.1s│     │
│                                   └────────┘ └────────┘     │
│                                                             │
│  → Click node to open Task Drawer                           │
└─────────────────────────────────────────────────────────────┘
```

**Grid View tab:**
```
┌─────────────────────────────────────────────────────────────┐
│  Task ID               Operator           State   Duration  │
│  read_config           PythonOperator     ✅      0.8s      │
│  run_bronze_ingestion  SparkSubmit        ✅      10.2s     │
│  run_silver_transforms SparkSubmit        ❌      120.1s    │
│  log_success           PythonOperator     ⏭       —         │
│  log_failure           PythonOperator     ✅      0.1s      │
└─────────────────────────────────────────────────────────────┘
```

### 5.4 Task Detail Drawer (Slide-over)

```
┌──────────────────────────────────────┐
│ ✕  run_silver_transforms            │
│────────────────────────────────────│
│ Operator: SparkSubmitOperator       │
│ State:    ❌ failed                  │
│ Duration: 2m 0.1s                   │
│ Try:      1 / 2                     │
│ Pool:     default_pool              │
│ Queue:    default                   │
│────────────────────────────────────│
│ [📋 View Logs] [🔄 Clear & Retry]  │
│────────────────────────────────────│
│ ▼ Logs (auto-tail)                  │
│ ┌────────────────────────────────┐  │
│ │ [2026-02-08 02:24:13] ERROR    │  │
│ │ Spark job failed: OOM at       │  │
│ │ executor 2. Container killed   │  │
│ │ by YARN. Exit code: 137        │  │
│ │ ...                            │  │
│ └────────────────────────────────┘  │
└──────────────────────────────────────┘
```

### 5.5 Logs Viewer (`<LogsViewer />`)

- Monospace font, dark background, ANSI color parsing
- Auto-scroll with "tail" behavior (poll every 2s while task is `running`)
- Manual scroll up pauses auto-scroll, shows "↓ Resume" button
- Line numbers, search within logs (`Ctrl+F`)
- Download raw log file button

---

## 6. Graph Building Logic

### 6.1 Build Nodes & Edges from Airflow API

```typescript
// Fetch: GET /api/v2/dags/{dagId}/tasks
// Each task has: task_id, downstream_task_ids[], operator_name, ui_color, trigger_rule

interface GraphNode {
  id: string;
  label: string;
  operator: string;
  color: string;
  triggerRule: string;
  // Overlaid per-run:
  state?: 'success' | 'failed' | 'running' | 'skipped' | 'upstream_failed' | null;
  duration?: number;
  mapIndex?: number;
}

function buildGraph(tasks: AirflowTask[]): { nodes: GraphNode[], edges: Edge[] } {
  const nodes = tasks.map(t => ({
    id: t.task_id,
    label: t.task_display_name,
    operator: t.operator_name,
    color: t.ui_color,
    triggerRule: t.trigger_rule,
  }));
  
  const edges: Edge[] = [];
  for (const task of tasks) {
    for (const downstream of task.downstream_task_ids) {
      edges.push({ source: task.task_id, target: downstream });
    }
  }
  return { nodes, edges };
}
```

### 6.2 Overlay TaskInstance State

```typescript
// Fetch: GET /api/v2/dags/{dagId}/dagRuns/{runId}/taskInstances
function overlayState(nodes: GraphNode[], taskInstances: TaskInstance[]): GraphNode[] {
  const stateMap = new Map(taskInstances.map(ti => [ti.task_id, ti]));
  return nodes.map(node => {
    const ti = stateMap.get(node.id);
    return { ...node, state: ti?.state, duration: ti?.duration, mapIndex: ti?.map_index };
  });
}
```

### 6.3 Layout with React Flow + dagre

```typescript
import dagre from '@dagrejs/dagre';

function layoutGraph(nodes: GraphNode[], edges: Edge[]) {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'LR', ranksep: 80, nodesep: 40 });
  
  nodes.forEach(n => g.setNode(n.id, { width: 180, height: 60 }));
  edges.forEach(e => g.setEdge(e.source, e.target));
  dagre.layout(g);
  
  return nodes.map(n => {
    const pos = g.node(n.id);
    return { ...n, position: { x: pos.x - 90, y: pos.y - 30 } };
  });
}
```

**Recommended Libraries:**
- `@xyflow/react` (React Flow v12) — node rendering, zoom/pan, minimap
- `@dagrejs/dagre` — automatic DAG layout (LR direction)
- Alternative for complex layouts: `elkjs` (more sophisticated, handles cycles better)

### 6.4 Mapped Tasks (`map_index`)

When `is_mapped: true`:
- TaskInstance returns multiple entries with different `map_index` values (0, 1, 2, ...)
- **UI approach:** Render the parent node with a "stacked" visual (shadow copies). Click reveals a sub-table listing each `map_index` with its state and duration.
- Group by `task_id`, show aggregate state (if any failed → node shows failed, all success → success, mixed → partial).

---

## 7. Monitoring

### 7.1 Health Endpoint

Airflow exposes `GET /api/v2/monitor/health` (no auth required):
```json
{
  "metadatabase": { "status": "healthy" },
  "scheduler": { "status": "healthy", "latest_scheduler_heartbeat": "..." },
  "dag_processor": { "status": "healthy", "latest_dag_processor_heartbeat": "..." },
  "triggerer": { "status": null }
}
```

**UI:** Three colored dots in the toolbar:
- 🟢 Healthy: heartbeat within last 30s
- 🟡 Degraded: heartbeat > 30s ago
- 🔴 Unhealthy: no heartbeat or `status: unhealthy`

Poll every 30 seconds.

### 7.2 Metrics (Prometheus/OpenTelemetry)

Airflow 3 exports Prometheus metrics at `/api/v2/metrics` (configurable). Key metrics:

| Metric | Purpose |
|---|---|
| `airflow_scheduler_heartbeat` | Scheduler liveness |
| `airflow_dag_processing_total_parse_time` | DAG file parsing performance |
| `airflow_pool_open_slots` | Worker pool capacity |
| `airflow_ti_successes` / `airflow_ti_failures` | Task success/failure rates |
| `airflow_dag_run_duration` | Pipeline execution time |

**Recommendation:** Scrape with Prometheus → Grafana dashboard for ops team. For the product UI, use the health endpoint + aggregate run stats from the API.

---

## 8. Security Model

### 8.1 Threat Model

| Threat | Mitigation |
|---|---|
| Direct Airflow API exposure | Airflow binds to internal Docker network only. No external ports. |
| Unauthorized DAG trigger | Backend enforces RBAC before forwarding to Airflow |
| Conf injection (malicious DAG run config) | Backend validates `conf` against allowed schema per DAG |
| Log data exfiltration | Backend filters logs before returning (strip internal IPs, secrets) |
| Token replay | Service account JWT has short TTL (5min), auto-refreshed |
| Lateral movement | Airflow service account has minimal permissions (`dag:read`, `dag:execute`) |

### 8.2 RBAC Rules

| Role | View DAGs | Trigger Runs | Pause/Unpause | View Logs | Retry/Clear | Admin |
|---|---|---|---|---|---|---|
| `viewer` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `operator` | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| `admin` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

**Implementation:**
```typescript
const RBAC: Record<string, string[]> = {
  viewer:   ['dags:read'],
  operator: ['dags:read', 'dags:trigger', 'dags:pause', 'logs:read'],
  admin:    ['dags:read', 'dags:trigger', 'dags:pause', 'logs:read', 'tasks:clear', 'metadata:write'],
};

function authorize(userRole: string, requiredPermission: string): boolean {
  return RBAC[userRole]?.includes(requiredPermission) ?? false;
}
```

### 8.3 Rate Limiting

| Endpoint Group | Rate Limit |
|---|---|
| Read operations (GET) | 100 req/min per user |
| Write operations (POST/PATCH) | 10 req/min per user |
| Log streaming | 30 req/min per user |
| Health check | 60 req/min (no auth) |

**Implementation:** Use `Map<userId, { count, windowStart }>` in-memory for dev. In production, use Redis-backed sliding window.

---

## 9. Performance & UX

### 9.1 Pagination

| Resource | Default Page Size | Max | Sort Default |
|---|---|---|---|
| DAGs list | 25 | 100 | `dag_id ASC` |
| DAG Runs | 20 | 50 | `start_date DESC` |
| Task Instances | 50 (all for one run) | 200 | execution order |

**Strategy:** Offset-based pagination for simplicity. Cursor-based for future scale.

### 9.2 Caching

| Data | Cache TTL | Invalidation |
|---|---|---|
| DAG structure (nodes/edges) | 5 min | On DAG deploy (version change) |
| DAG list (metadata) | 30 sec | On poll refresh |
| Health status | 30 sec | Fixed interval |
| Task Instances (completed run) | ∞ (immutable) | Never — completed runs don't change |
| Task Instances (running) | No cache | Always fetch fresh |

**Implementation:**
```typescript
const structureCache = new Map<string, { data: Graph, version: string, expires: number }>();

async function getDAGStructure(dagId: string) {
  const cached = structureCache.get(dagId);
  const dag = await airflowClient.getDag(dagId);
  
  if (cached && cached.version === dag.bundle_version && cached.expires > Date.now()) {
    return cached.data;
  }
  
  const tasks = await airflowClient.getDAGTasks(dagId);
  const graph = buildGraph(tasks);
  structureCache.set(dagId, { data: graph, version: dag.bundle_version, expires: Date.now() + 300_000 });
  return graph;
}
```

### 9.3 Polling Strategy

| View | What to Poll | Interval | Backoff |
|---|---|---|---|
| DAG List | DAG metadata + last run state | 15s | None — fixed |
| Run Detail (running) | Task instances | 2s | 2s → 5s → 10s when idle |
| Run Detail (completed) | Nothing | — | Stop polling |
| Logs (running task) | Log content with offset | 2s | 2s → 5s when no new content |
| Logs (completed task) | Nothing | — | Fetch once, cache |
| Health | Health endpoint | 30s | None |

**Live update hook:**
```typescript
function usePolling<T>(fetcher: () => Promise<T>, intervalMs: number, enabled: boolean) {
  const [data, setData] = useState<T | null>(null);
  
  useEffect(() => {
    if (!enabled) return;
    let timeout: NodeJS.Timeout;
    const poll = async () => {
      const result = await fetcher();
      setData(result);
      timeout = setTimeout(poll, intervalMs);
    };
    poll();
    return () => clearTimeout(timeout);
  }, [fetcher, intervalMs, enabled]);
  
  return data;
}
```

---

## 10. Starter Code Scaffold

### 10.1 File Structure

```
web-ui/src/
├── app/
│   ├── api/
│   │   └── orchestrator/
│   │       ├── dags/
│   │       │   └── route.ts                    # GET /dags
│   │       ├── dags/[dagId]/
│   │       │   ├── route.ts                    # GET /dags/:id, PATCH pause
│   │       │   ├── structure/route.ts           # GET structure (graph)
│   │       │   ├── runs/route.ts                # GET runs, POST trigger
│   │       │   └── runs/[runId]/
│   │       │       ├── route.ts                 # GET run detail
│   │       │       └── tasks/route.ts           # GET task instances
│   │       │       └── tasks/[taskId]/
│   │       │           └── logs/route.ts        # GET logs
│   │       └── health/route.ts                  # GET health
│   └── workflows/
│       ├── page.tsx                             # DAG List (Dashboard)
│       ├── [dagId]/
│       │   ├── page.tsx                         # DAG Detail + Runs
│       │   └── runs/[runId]/page.tsx            # Run Detail (Graph/Grid)
├── components/
│   └── orchestrator/
│       ├── DAGTable.tsx
│       ├── RunsList.tsx
│       ├── GraphView.tsx                        # React Flow + dagre
│       ├── GridView.tsx                         # Task table
│       ├── TaskDrawer.tsx                       # Slide-over panel
│       ├── LogsViewer.tsx                       # Tail-like log viewer
│       ├── HealthIndicator.tsx                  # Status dots
│       └── FilterToolbar.tsx
├── lib/
│   └── airflow/
│       ├── client.ts                            # AirflowClient class
│       ├── types.ts                             # TypeScript interfaces
│       ├── cache.ts                             # Server-side cache
│       └── auth.ts                              # JWT token manager
```

### 10.2 Airflow Client (`lib/airflow/client.ts`)

```typescript
import { AirflowTokenManager } from './auth';
import { DAG, DAGRun, TaskInstance, AirflowTask, HealthStatus } from './types';

const AIRFLOW_BASE = process.env.AIRFLOW_API_URL || 'http://airflow_webserver:8080';
const API_PREFIX = process.env.AIRFLOW_API_VERSION === 'v1' ? '/api/v1' : '/api/v2';

const tokenManager = new AirflowTokenManager();

async function airflowFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const token = await tokenManager.getToken();
  const url = `${AIRFLOW_BASE}${API_PREFIX}${path}`;
  
  const res = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });
  
  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(`Airflow API Error: ${res.status} - ${JSON.stringify(error)}`);
  }
  
  return res.json();
}

export const airflowClient = {
  // DAGs
  listDAGs: (params: { limit?: number; offset?: number; paused?: boolean }) =>
    airflowFetch<{ dags: DAG[]; total_entries: number }>(
      `/dags?limit=${params.limit || 25}&offset=${params.offset || 0}` +
      (params.paused !== undefined ? `&paused=${params.paused}` : '')
    ),
  
  getDAG: (dagId: string) => airflowFetch<DAG>(`/dags/${dagId}`),
  
  patchDAG: (dagId: string, body: { is_paused: boolean }) =>
    airflowFetch<DAG>(`/dags/${dagId}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),
  
  // Runs
  listRuns: (dagId: string, params?: { limit?: number; offset?: number }) =>
    airflowFetch<{ dag_runs: DAGRun[]; total_entries: number }>(
      `/dags/${dagId}/dagRuns?order_by=-start_date&limit=${params?.limit || 20}&offset=${params?.offset || 0}`
    ),
  
  triggerRun: (dagId: string, conf?: object) =>
    airflowFetch<DAGRun>(`/dags/${dagId}/dagRuns`, {
      method: 'POST',
      body: JSON.stringify({ conf: conf || {} }),
    }),
  
  getRun: (dagId: string, runId: string) =>
    airflowFetch<DAGRun>(`/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}`),
  
  // Tasks
  getDAGTasks: (dagId: string) =>
    airflowFetch<{ tasks: AirflowTask[] }>(`/dags/${dagId}/tasks`),
  
  getTaskInstances: (dagId: string, runId: string) =>
    airflowFetch<{ task_instances: TaskInstance[] }>(
      `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}/taskInstances`
    ),
  
  // Logs
  getTaskLogs: (dagId: string, runId: string, taskId: string, tryNumber: number) =>
    airflowFetch<string>(
      `/dags/${dagId}/dagRuns/${encodeURIComponent(runId)}/taskInstances/${taskId}/logs/${tryNumber}`,
    ),
  
  // Health
  getHealth: () => airflowFetch<HealthStatus>('/monitor/health'),
};
```

### 10.3 Token Manager (`lib/airflow/auth.ts`)

```typescript
const AIRFLOW_BASE = process.env.AIRFLOW_API_URL || 'http://airflow_webserver:8080';
const SA_USERNAME = process.env.AIRFLOW_SA_USERNAME || 'admin';
const SA_PASSWORD = process.env.AIRFLOW_SA_PASSWORD || 'admin';

export class AirflowTokenManager {
  private token: string | null = null;
  private expiresAt: number = 0;

  async getToken(): Promise<string> {
    // Refresh if expired or within 60s of expiry
    if (!this.token || Date.now() >= this.expiresAt - 60_000) {
      await this.refresh();
    }
    return this.token!;
  }

  private async refresh(): Promise<void> {
    const res = await fetch(`${AIRFLOW_BASE}/auth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: SA_USERNAME, password: SA_PASSWORD }),
    });

    if (!res.ok) {
      throw new Error(`Failed to get Airflow token: ${res.status}`);
    }

    const { access_token } = await res.json();
    this.token = access_token;

    // Parse JWT expiry (payload is base64-encoded JSON)
    const payload = JSON.parse(Buffer.from(access_token.split('.')[1], 'base64').toString());
    this.expiresAt = payload.exp * 1000; // Convert to ms
  }
}
```

### 10.4 Environment Variables

```env
# .env.local (Next.js)
AIRFLOW_API_URL=http://localhost:8081          # Internal Airflow API
AIRFLOW_API_VERSION=v2                         # v1 for Airflow 2.x
AIRFLOW_SA_USERNAME=admin                      # Service account
AIRFLOW_SA_PASSWORD=admin                      # Service account password
PRODUCT_DB_URL=postgresql://postgres:changeme@localhost:5433/controldb
```

### 10.5 Docker Compose Addition

Already exists in current `docker-compose.yml`. The Airflow services are configured and running. The only change needed is ensuring the Airflow API server is **not** exposed to external networks in production:

```yaml
# Production: Remove port mapping, use internal network only
airflow-webserver:
  # ports:             # ← REMOVE in production
  #   - "8081:8080"    # ← No external access
  networks:
    - agentic-network  # Internal only — backend accesses via service name
```

---

## Implementation Phases

| Phase | Scope | Effort |
|---|---|---|
| **Phase 1** | Airflow Client + token auth + health endpoint + DAG list page with real data | 1 day |
| **Phase 2** | DAG detail page + runs list + trigger/pause actions | 1 day |
| **Phase 3** | Graph View (React Flow + dagre) + task state overlay | 1-2 days |
| **Phase 4** | Grid View + Task Drawer + Logs Viewer | 1 day |
| **Phase 5** | Product DB tables + favorites + metadata + audit log | 1 day |
| **Phase 6** | RBAC middleware + rate limiting + security hardening | 0.5 day |
| **Phase 7** | Polling + caching + performance optimization | 0.5 day |
