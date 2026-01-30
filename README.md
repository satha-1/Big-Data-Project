## FinTech Fraud Detection Pipeline (Kafka + Spark Streaming + Postgres + Airflow) — Windows (Docker Desktop + WSL2)

End-to-end mini-project implementing a Lambda-style pipeline:

- **Streaming ingestion**: Python producer → Kafka topic `transactions_raw`
- **Real-time fraud detection**: Spark Structured Streaming (event-time + watermark)
- **Secure fraud sink**: Postgres table `fraud_alerts`
- **Validated warehouse**: Parquet files under `./data/warehouse/validated` (bind-mounted)
- **Airflow ETL every 6 hours**: reconciliation + fraud-by-category CSV reports to `./reports` (bind-mounted)

### Architecture

- **Kafka**: `transactions_raw` receives transaction JSON events
- **Spark Structured Streaming** reads from Kafka and applies fraud rules:
  - **Rule A (HIGH_SPEND)**: `amount > 5000`
  - **Rule B (IMPOSSIBLE_TRAVEL)**: same `user_id` transacts from **2 different countries within 10 minutes**, based on **event-time** with **15 minute watermark**
- **Outputs**
  - **Fraud events** → Postgres `fraud_alerts` (immediate)
  - **Non-fraud (validated)** → Parquet warehouse in `./data/warehouse/validated` partitioned by `date` and `hour`
  - **Ingress metrics** (raw totals) → Postgres `ingress_metrics` aggregated in 6-hour event-time buckets
- **Airflow** (LocalExecutor) runs every 6 hours:
  - **Reconciliation report**: ingress total vs validated total for last completed 6-hour bucket
  - **Fraud attempts by merchant category** (last 24 hours)

### Prerequisites (Windows)

- **Docker Desktop** installed
- **WSL2 enabled**
- Docker Desktop settings:
  - **Use the WSL 2 based engine**
  - Ensure your distro integration is enabled (Settings → Resources → WSL Integration)
- Ensure your project folder is on a drive Docker can access (this repo uses bind mounts `./data` and `./reports`)

### Quickstart (single command boot)

1) Create your env file:

- Copy `.env.example` → `.env`

2) Start everything:

```bash
docker compose up -d --build
```

### Service URLs

- **Airflow UI**: `http://localhost:8080`
  - user/pass from `.env` (defaults: `admin` / `admin`)
- **Spark Master UI**: `http://localhost:8081`

### How to view logs

```bash
docker compose logs -f producer
docker compose logs -f spark-master
docker compose logs -f airflow-scheduler
```

### Verify the pipeline

#### 1) Producer is generating events

```bash
docker compose logs -f producer
```

You should see periodic counts like `Produced=... high_spend_injected=... impossible_travel_pairs=...`.

#### 2) (Optional) Watch Kafka topic

```bash
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic transactions_raw --from-beginning --max-messages 5"
```

#### 3) Check Postgres fraud sink

```bash
docker exec -it postgres psql -U frauduser -d frauddb -c "SELECT fraud_type, COUNT(*) FROM fraud_alerts GROUP BY fraud_type ORDER BY 2 DESC;"
docker exec -it postgres psql -U frauduser -d frauddb -c "SELECT * FROM fraud_alerts ORDER BY detected_at DESC LIMIT 5;"
```

You should see both `HIGH_SPEND` and `IMPOSSIBLE_TRAVEL`.

#### 4) Check validated Parquet warehouse on host

- On your host, look under:
  - `./data/warehouse/validated/date=YYYY-MM-DD/hour=H/part-*.parquet`

#### 5) Trigger Airflow DAG and check reports

1) Open Airflow UI (`http://localhost:8080`)
2) Enable and **trigger** DAG: `fraud_etl_every_6_hours`
3) Reports will appear on host under `./reports/`:
   - `reconciliation_<window_start>_<window_end>.csv`
   - `fraud_by_category_<date>.csv`

### Troubleshooting

- **Kafka not ready / producer retries**: This is expected briefly on first boot. Wait ~30–60 seconds and watch `docker compose logs -f producer`.
- **Spark job can't read Kafka**:
  - Ensure Kafka is healthy: `docker compose ps`
  - Check `spark-master` logs for connector download issues (it uses `--packages` at submit time).
  - If your network blocks Maven downloads, pre-pull dependencies or allow Maven Central access.
- **Windows bind mount issues**:
  - Keep the repo under a path Docker Desktop can access.
  - If you see permission errors, move the repo into your WSL2 filesystem and run compose from there.

### Technical Notes

- **Spark Image**: Uses official `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu` image (includes Java 17 and Python 3) to avoid Debian trixie openjdk-11 package availability issues. This image is pre-configured with Spark 3.5.8 and all required dependencies.

### Repo layout

```
docker-compose.yml
.env.example
sql/init.sql
src/producer/producer.py
src/streaming/spark_fraud_job.py
src/airflow/dags/fraud_etl_dag.py
data/          # Parquet warehouse + Spark checkpoints (bind-mounted)
reports/       # Airflow outputs (bind-mounted)
```

