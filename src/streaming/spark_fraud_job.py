import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    coalesce,
    current_timestamp,
    date_format,
    from_json,
    hour,
    lit,
    sum as fsum,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql import functions as F


def get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def main() -> None:
    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    raw_topic = get_env("RAW_TOPIC", "transactions_raw")

    pg_host = get_env("POSTGRES_HOST", "postgres")
    pg_db = get_env("POSTGRES_DB", "frauddb")
    pg_user = get_env("POSTGRES_USER", "frauduser")
    pg_pass = get_env("POSTGRES_PASSWORD", "fraudpass")

    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"
    jdbc_props = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder.appName("fintech-fraud-detection")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    schema = StructType(
        [
            StructField("txn_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("event_time", StringType(), False),  # ISO8601 string
            StructField("merchant_category", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("country", StringType(), False),
            StructField("city", StringType(), False),
        ]
    )

    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", raw_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_kafka.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("t"))
        .select("t.*")
        .withColumn(
            "event_time_ts",
            coalesce(
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"),
                to_timestamp(col("event_time")),
            ),
        )
        .dropna(subset=["event_time_ts"])
        .drop("event_time")
        .withColumnRenamed("event_time_ts", "event_time")
        .select(
            "txn_id",
            "user_id",
            "event_time",  # TimestampType
            "merchant_category",
            "amount",
            "country",
            "city",
        )
    )

    # Base watermark + txn_id dedupe (streaming state is fine here)
    events = parsed.withWatermark("event_time", "15 minutes").dropDuplicates(["txn_id"])

    # Rule A: HIGH_SPEND
    is_high_spend = col("amount") > lit(5000.0)

    # Rule B: IMPOSSIBLE_TRAVEL (10-min window; >1 distinct country)
    # Extract window start/end as separate columns to avoid event-time column conflict
    events_with_win = (
        events.withColumn("win10m", window(col("event_time"), "10 minutes"))
        .withColumn("win10m_start", col("win10m.start"))
        .withColumn("win10m_end", col("win10m.end"))
        .drop("win10m")  # Drop the window struct to avoid event-time conflict
    )

    # Add watermark using event_time to support append mode downstream
    # The aggregation groups by window boundaries but uses event_time for watermark
    suspect_windows = (
        events_with_win.withWatermark("event_time", "15 minutes")
        .groupBy(col("user_id"), col("win10m_start"), col("win10m_end"))
        .agg(approx_count_distinct(col("country"), rsd=0.02).alias("distinct_countries"))
        .filter(col("distinct_countries") > lit(1))
        .select(
            col("user_id").alias("sw_user_id"),
            col("win10m_start").alias("sw_win10m_start"),
            col("win10m_end").alias("sw_win10m_end"),
        )
    )

    flagged = (
        events_with_win.join(
            suspect_windows,
            (events_with_win.user_id == suspect_windows.sw_user_id)
            & (events_with_win.win10m_start == suspect_windows.sw_win10m_start)
            & (events_with_win.win10m_end == suspect_windows.sw_win10m_end),
            how="left",
        )
        .withColumn("is_impossible_travel", col("sw_user_id").isNotNull())
        .drop("sw_user_id", "sw_win10m_start", "sw_win10m_end")
    )

    fraud_high_spend = (
        flagged.filter(is_high_spend)
        .drop("win10m_start", "win10m_end")
        .withColumn("fraud_type", lit("HIGH_SPEND"))
        .withColumn("detected_at", current_timestamp())
    )

    fraud_impossible_travel = (
        flagged.filter(col("is_impossible_travel"))
        .drop("win10m_start", "win10m_end")
        .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL"))
        .withColumn("detected_at", current_timestamp())
    )

    # No streaming dropDuplicates here; we'll dedupe inside foreachBatch.
    fraud_events = fraud_high_spend.unionByName(fraud_impossible_travel)

    valid_events = (
        flagged.filter(~is_high_spend & ~col("is_impossible_travel"))
        .drop("win10m_start", "win10m_end")
    )

    validated_out = (
        valid_events.withColumn("date", date_format(col("event_time"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("event_time")))
    )

    def write_fraud_to_postgres(batch_df, batch_id: int) -> None:  # noqa: ANN001
        # Batch dedupe (no streaming state)
        deduped = batch_df.dropDuplicates(["txn_id", "fraud_type"])

        (
            deduped.select(
                "txn_id",
                "user_id",
                "event_time",
                "merchant_category",
                col("amount").cast("double").alias("amount"),
                "country",
                "city",
                "fraud_type",
                "detected_at",
            )
            .write.mode("append")
            .jdbc(url=jdbc_url, table="fraud_alerts", properties=jdbc_props)
        )

    def write_ingress_metrics(batch_df, batch_id: int) -> None:  # noqa: ANN001
        (
            batch_df.select(
                col("window_start"),
                col("window_end"),
                col("total_amount"),
                col("txn_count"),
                current_timestamp().alias("updated_at"),
            )
            .write.mode("append")
            .jdbc(url=jdbc_url, table="ingress_metrics", properties=jdbc_props)
        )

    fraud_query = (
        fraud_events.writeStream.outputMode("append")
        .foreachBatch(write_fraud_to_postgres)
        .option("checkpointLocation", "/data/checkpoints/fraud_alerts")
        .start()
    )

    validated_query = (
        validated_out.writeStream.format("parquet")
        .outputMode("append")
        .option("path", "/data/warehouse/validated")
        .option("checkpointLocation", "/data/checkpoints/validated")
        .partitionBy("date", "hour")
        .start()
    )

    ingress_metrics = (
        events.groupBy(window(col("event_time"), "6 hours").alias("win6h"))
        .agg(
            fsum(col("amount")).alias("total_amount"),
            F.count("*").alias("txn_count"),
        )
        .select(
            col("win6h.start").alias("window_start"),
            col("win6h.end").alias("window_end"),
            col("total_amount"),
            col("txn_count"),
        )
    )

    ingress_query = (
        ingress_metrics.writeStream.outputMode("append")
        .foreachBatch(write_ingress_metrics)
        .option("checkpointLocation", "/data/checkpoints/ingress_metrics")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
