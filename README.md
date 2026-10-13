# Smart Retail Analytics Pipeline

A production-grade retail analytics pipeline that ingests real-time sales transactions, processes them through a Medallion architecture on Delta Lake, and uses an AI anomaly detection model to flag unusual sales patterns — sudden drops, unexpected spikes, potential fraud — before they reach the reporting layer. Built with PySpark, Delta Lake, Kafka, Airflow orchestration, and deployed via Docker. Includes a text-to-SQL agent for natural-language querying over the final reporting tables.

![Architecture diagram](docs/architecture_diagram.svg)

## Dataset

[Online Retail II](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) (UCI / Kaggle) — ~1.07M UK-based online retail transactions, Dec 2009 – Dec 2011.

## Architecture

**Batch pipeline (Medallion architecture):**
1. **Bronze** — raw ingestion via Spark (`spark.read.csv`). Excel→CSV conversion is a one-time preprocessing step outside the pipeline (Spark has no native Excel reader).
2. **Silver** — type casting, deduplication, `is_cancelled` flag (invoice starts with "C"), `total_amount` derived column.
3. **Anomaly detection** — aggregates Silver to daily × product grain, scores with Isolation Forest (scikit-learn), ~5% contamination.
4. **Gold** — three reporting tables: daily product sales (with anomaly flags + description + top country), monthly country sales rollup, top-100 anomalies leaderboard.

**Streaming pipeline (parallel):**
- **Producer** replays a historical month as a paced event stream — real inter-arrival gaps compressed by a configurable factor, capped to avoid long stalls — onto a Kafka topic.
- **Consumer** (Spark Structured Streaming, 10s micro-batches) writes to Delta using `MERGE` (not `INSERT`), making re-processed/duplicate messages idempotent.

**Orchestration:** Apache Airflow (LocalExecutor + Postgres), running in a custom Docker image with Java 17, PySpark 3.5.1, and Delta 3.2.0 baked in — same pipeline code runs unmodified on Windows (local dev) and Linux (container), via a `PROJECT_ROOT` environment variable and `os.path.join`-based paths.

**RAG agent:** a text-to-SQL natural-language interface over the Gold tables — registers Delta tables as Spark SQL views, an LLM generates SQL grounded in the real schema, the query runs for real, and a second LLM call phrases the answer from actual results (not a hallucination). Guarded to only execute `SELECT` statements.

## Key design decisions

- **Pandas used only once**, outside the pipeline (Excel→CSV), never inside a pipeline stage — real pipelines don't hand Spark proprietary source formats directly.
- **Anomaly detection runs on aggregated data, not raw transactions** — fraud/trend signals (sudden spikes, drops) are properties of aggregates, not single rows.
- **Gold's core table stays at the grain the model was scored on** (date × product) rather than re-aggregating to a finer grain (date × product × country) — splitting one anomaly score across a finer grain than it was computed at would misrepresent what was measured. A "top country" field is attached as descriptive context instead.
- **Streaming writes use Delta `MERGE`**, making the consumer idempotent against duplicate/replayed messages — the production-correct way to handle at-least-once delivery, rather than relying on the producer to never double-send.
- **Centralized, OS-agnostic configuration** (`pipeline/config.py`, `PROJECT_ROOT` env var) lets the same code run locally on Windows and inside the Linux-based Airflow container without modification.
- **Text-to-SQL chosen over embedding-based RAG** for the query agent — the Gold tables are structured/numeric/aggregatable, which text-to-SQL handles natively; vector retrieval is a poor fit for exact filters and aggregations over structured data.

## Known limitations / next steps

- `stockcode == "M"` (manual/adjustment entries, not real products) shows up in top anomalies — a known data quality quirk in this dataset, worth filtering out in a v2.
- Streaming producer/consumer run natively (not containerized) — a deliberate scope decision to limit local resource usage; Dockerizing them is a natural extension.
- Local LLM (Ollama) was evaluated for the RAG agent but caused a hardware-related shutdown on a laptop with a degraded battery under sudden CPU load; switched to an API-based model (OpenAI `gpt-4o-mini`) instead.

## Tech stack

Python, PySpark, Delta Lake, Kafka, Apache Airflow, Docker, scikit-learn, pytest + chispa, OpenAI API

## Setup

1. Download the [Online Retail II dataset](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci), place the `.xlsx` in `data/`.
2. `pip install -r requirements.txt`
3. Run `pipeline/utils/excel_to_csv.py` once to generate the CSV.
4. Run pipeline stages in order: `bronze_ingestion.py` → `silver_processing.py` → `anomaly_detection.py` → `gold_processing.py`.
5. For streaming: `docker compose up -d` (Kafka), then run `consumer.py` and `producer.py` in separate terminals.
6. For orchestration: `docker compose -f docker-compose-airflow.yml up -d`, visit `localhost:8080`.
7. For the RAG agent: set `OPENAI_API_KEY` environment variable, run `pipeline/rag_agent/text_to_sql_agent.py`.
8. Run tests: `pytest tests/ -v`

## Resume summary

```
Smart Retail Analytics Pipeline | Python, PySpark, Delta Lake, Kafka, Airflow, Docker, scikit-learn

- Built end-to-end streaming + batch data pipeline processing 1M+ retail transactions
  through Medallion Architecture (Bronze/Silver/Gold) using PySpark and Delta Lake
- Integrated Isolation Forest anomaly detection at the Silver-Gold transition layer,
  flagging ~5% of daily product-sales groups as statistically anomalous
- Simulated real-time event streaming using Kafka, replaying historical transactions
  as paced live events; built an idempotent Spark Structured Streaming consumer using
  Delta MERGE semantics to safely handle at-least-once delivery
- Containerized the pipeline (custom Airflow image with PySpark/Delta/Java) and
  orchestrated batch workflows via Airflow DAGs with dependency management
- Added unit test coverage (pytest, chispa) for transformation/cleaning logic
- Built a text-to-SQL natural-language query agent over Gold-layer analytics tables
```