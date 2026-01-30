import csv
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pendulum
import psycopg2
import pyarrow.dataset as ds
from airflow import DAG
from airflow.operators.python import PythonOperator


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _floor_to_6h(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    floored_hour = (dt.hour // 6) * 6
    return dt.replace(hour=floored_hour)


def _window_last_completed(now: datetime) -> tuple[datetime, datetime]:
    end = _floor_to_6h(now)
    start = end - timedelta(hours=6)
    return start, end


def _pg_conn():
    return psycopg2.connect(
        host=_env("POSTGRES_HOST", "postgres"),
        dbname=_env("POSTGRES_DB", "frauddb"),
        user=_env("POSTGRES_USER", "frauduser"),
        password=_env("POSTGRES_PASSWORD", "fraudpass"),
        port=5432,
    )


def generate_reconciliation_report(**_context) -> None:
    data_dir = _env("DATA_DIR", "/opt/airflow/data")
    reports_dir = _env("REPORTS_DIR", "/opt/airflow/reports")
    os.makedirs(reports_dir, exist_ok=True)

    now = datetime.now(timezone.utc)
    window_start, window_end = _window_last_completed(now)

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT total_amount::float, txn_count
                FROM ingress_metrics
                WHERE window_start = %s AND window_end = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (window_start, window_end),
            )
            row = cur.fetchone()

    total_ingress_amount = float(row[0]) if row else 0.0

    validated_base = os.path.join(data_dir, "warehouse", "validated")
    validated_amount = 0.0
    if os.path.exists(validated_base):
        dataset = ds.dataset(validated_base, format="parquet", partitioning="hive")
        filt = (ds.field("event_time") >= pd.Timestamp(window_start)) & (
            ds.field("event_time") < pd.Timestamp(window_end)
        )
        table = dataset.to_table(filter=filt, columns=["amount"])
        if table.num_rows > 0:
            validated_amount = float(table.column("amount").to_pandas().sum())

    diff = total_ingress_amount - validated_amount

    ws = window_start.strftime("%Y%m%dT%H%M%SZ")
    we = window_end.strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(reports_dir, f"reconciliation_{ws}_{we}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "window_start",
                "window_end",
                "total_ingress_amount",
                "validated_amount",
                "difference",
            ]
        )
        w.writerow(
            [
                window_start.isoformat().replace("+00:00", "Z"),
                window_end.isoformat().replace("+00:00", "Z"),
                round(total_ingress_amount, 2),
                round(validated_amount, 2),
                round(diff, 2),
            ]
        )


def generate_fraud_by_category_report(**_context) -> None:
    reports_dir = _env("REPORTS_DIR", "/opt/airflow/reports")
    os.makedirs(reports_dir, exist_ok=True)

    since = datetime.now(timezone.utc) - timedelta(hours=24)

    with _pg_conn() as conn:
        df = pd.read_sql(
            """
            SELECT merchant_category,
                   COUNT(*)::bigint AS fraud_count,
                   COALESCE(SUM(amount), 0)::float AS fraud_amount
            FROM fraud_alerts
            WHERE event_time >= %s
            GROUP BY merchant_category
            ORDER BY fraud_count DESC, fraud_amount DESC
            """,
            conn,
            params=(since,),
        )

    out_path = os.path.join(
        reports_dir, f"fraud_by_category_{datetime.now(timezone.utc).date().isoformat()}.csv"
    )
    df.to_csv(out_path, index=False)


with DAG(
    dag_id="fraud_etl_every_6_hours",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 */6 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "fraud", "reconciliation"],
) as dag:
    t1 = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
    )

    t2 = PythonOperator(
        task_id="generate_fraud_by_category_report",
        python_callable=generate_fraud_by_category_report,
    )

    t1 >> t2

