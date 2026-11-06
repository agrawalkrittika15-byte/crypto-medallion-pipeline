# Crypto Mini-Pipeline

A mini data-engineering project implementing a **Medallion Architecture** (Bronze → Silver → Gold) in ClickHouse, fed by three independent ingestion pipelines:

| Source | Type | Tool | Frequency |
|---|---|---|---|
| CoinGecko API | Real-time / streaming | Apache NiFi | Every 30s |
| CSV historical snapshot | Batch | Apache Airflow | Daily |
| Baserow (dummy coin metadata) | Batch | Apache Airflow | Daily |

Transformations from Bronze → Silver → Gold are handled by **dbt** (`dbt-clickhouse` adapter).

## Architecture

```
CoinGecko API ──► NiFi ───────────────┐
CSV file ───────► Airflow ────────────┼──► ClickHouse (bronze_*) ──► dbt (silver_*) ──► dbt (gold_*)
Baserow ────────► Airflow ────────────┘
```

- **Bronze**: raw ingested data, one table per source (`bronze_ticks`, `bronze_crypto_snapshot`, `bronze_baserow_coins`)
- **Silver**: cleaned/joined data (`silver_ticks`, `silver_crypto`, `silver_baserow_coins`)
- **Gold**: business-level aggregates (`gold_price_summary`, `gold_crypto_summary`, `gold_baserow_summary`)

## Services

| Service | Port | Purpose |
|---|---|---|
| ClickHouse | 8123 (HTTP), 9000 (native) | Data warehouse |
| Baserow | 8081 | No-code database, source of dummy coin metadata |
| NiFi | 8443 (HTTPS) | Real-time ingestion flow |
| Airflow | 8090 | Batch orchestration |

## Setup

1. Copy `.env.example` to `.env` and fill in your own values (see "Environment variables" below). **Do not commit `.env`.**
2. Start the stack:
   ```
   docker compose up -d
   ```
3. Wait for all services to report healthy: `docker ps`
4. Open Airflow at `http://localhost:8090`, unpause and trigger `baserow_batch_pipeline` and `crypto_batch_pipeline`.
5. Open NiFi at `https://localhost:8443/nifi`, start all processors to begin real-time ingestion.
6. Run dbt to build the Silver/Gold layers:
   ```
   cd crypto_dbt
   dbt run
   dbt test
   ```

## Environment variables

All secrets are stored in a local `.env` file, which is listed in `.gitignore` and never committed. Copy `.env.example` to `.env` and set these values:

| Variable | Used by |
|---|---|
| `CLICKHOUSE_PASSWORD` | ClickHouse, Airflow DAGs |
| `BASEROW_TOKEN` | Airflow (`baserow_batch_dag.py`) |
| `NIFI_ADMIN_PASSWORD` | NiFi single-user login (minimum 12 characters) |

The Airflow DAGs read these values from environment variables, so no credentials are stored in code.

## Project structure

```
crypto-medallion-pipeline/
├── docker-compose.yml
├── .env.example         (template, placeholders only)
├── flow-backup.xml.gz   (NiFi flow backup)
├── airflow-dags/
│   ├── baserow_batch_dag.py
│   └── crypto_batch_dag.py
├── crypto_dbt/
│   ├── models/
│   │   ├── staging/     (bronze → cleaned views)
│   │   ├── marts/       (silver + gold tables)
│   │   └── sources.yml
│   └── dbt_project.yml
└── nifi-data/           (NiFi runtime data, not versioned)
```

## Known limitations

- `bronze_baserow_coins` and `bronze_crypto_snapshot` are truncate-and-reload on each run rather than append-only, so historical bronze data is not retained for these two sources.
- NiFi's real-time flow currently ingests price only; 24h volume is available from the CoinGecko API but not wired through end-to-end.
