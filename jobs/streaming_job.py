import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def build_schema() -> StructType:
    return StructType([
        StructField("Delivery_person_ID",      StringType(), True),
        StructField("Delivery_person_Ratings", StringType(), True),
        StructField("Weather_conditions",      StringType(), True),
        StructField("Road_traffic_density",    StringType(), True),
        StructField("Type_of_vehicle",         StringType(), True),
        StructField("Type_of_order",           StringType(), True),
        StructField("Festival",                StringType(), True),
        StructField("City",                    StringType(), True),
        StructField("Time_taken (min)",        StringType(), True),
        StructField("event_timestamp",         StringType(), True),
    ])


def write_dashboard_batch(batch_df, batch_id: int) -> None:
    dashboard_dir = Path(os.getenv("DASHBOARD_DIR", "/opt/zomato/dashboard_data"))
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    rows  = [r.asDict(recursive=True) for r in batch_df.collect()]
    total = sum(int(r.get("count", 0)) for r in rows)

    # R2: global avg delivery time across all weather groups
    avg_times = [float(r.get("avg_delivery_min", 0)) for r in rows
                 if r.get("avg_delivery_min") is not None]
    global_avg = round(sum(avg_times) / len(avg_times), 2) if avg_times else None

    # R1: orders per minute estimate (trigger = 10s → multiply by 6)
    orders_per_min = round(total * 6, 1)

    payload = {
        "batch_id":                  batch_id,
        "updated_at":                datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rows":                      rows,
        "orders_per_minute":         orders_per_min,       # R1
        "current_avg_delivery_min":  global_avg,           # R2
        "total_orders_this_batch":   total,
    }

    # Atomic write
    tmp  = dashboard_dir / "latest_snapshot.json.tmp"
    snap = dashboard_dir / "latest_snapshot.json"
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(snap)

    # Append history
    history_entry = {
        "batch_id":         batch_id,
        "updated_at":       payload["updated_at"],
        "total_orders":     total,
        "orders_per_min":   orders_per_min,
        "avg_delivery_min": global_avg,
        "weather_totals": {
            r["Weather_conditions"]: int(r["count"])
            for r in rows if r.get("Weather_conditions")
        },
    }
    with (dashboard_dir / "history.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(history_entry) + "\n")

    print(f"\n--- Batch {batch_id} | Orders: {total} "
          f"| ~{orders_per_min}/min | avg {global_avg} min ---")
    batch_df.show(truncate=False)


def main() -> None:
    bootstrap  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic      = os.getenv("KAFKA_TOPIC", "zomato-orders")
    checkpoint = os.getenv("CHECKPOINT_DIR", "/opt/zomato/checkpoints/streaming")

    spark = (
        SparkSession.builder
        .appName("ZomatoStreamingJob")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS raw_json")
           .select(F.from_json("raw_json", build_schema()).alias("e"))
           .select("e.*")
    )
    cleaned = (
        parsed
        .withColumn(
            "delivery_time_min",
            F.regexp_replace(F.col("Time_taken (min)"), r"\(min\)\s*", "")
             .cast(DoubleType())
        )
        .withColumn("Weather_conditions", F.trim(F.col("Weather_conditions")))
        .filter(F.col("delivery_time_min").isNotNull())
    )

    aggregated = (
        cleaned.groupBy("Weather_conditions")
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_min"),
        )
        .orderBy("Weather_conditions")
    )

    query = (
        aggregated.writeStream
        .outputMode("complete")
        .foreachBatch(write_dashboard_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()