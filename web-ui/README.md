# OpenClaw Workspace UI

This is the Next.js application for the Unified Management Interface.

## Setup
1.  Install dependencies:
    ```bash
    npm install
    ```
2.  Run development server:
    ```bash
    npm run dev
    ```

## Configuration
-   **Trino:** The app assumes Trino is running at `http://localhost:8080` (default) or set `TRINO_URL` env var.
-   **Airflow:** Currently integrated via mock data/iframe placeholders.

## Features
-   **Dashboard:** High-level metrics.
-   **Data Explorer:** Run SQL queries against Trino/Iceberg.
-   **Agent Console:** Visual interface for ClawdBot (simulated).
