import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count,
    min as spark_min, max as spark_max,
    round as spark_round, regexp_replace, trim,
    percentile_approx, stddev
)
from pyspark.sql.types import DoubleType

HDFS_INPUT  = "hdfs://namenode:9000/user/zomato/raw/zomato_dataset_cleaned.csv"
HDFS_OUTPUT = "hdfs://namenode:9000/user/zomato/batch_results"

spark = (
    SparkSession.builder
    .appName("ZomatoBatchAnalysis")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("ZOMATO DELIVERY — BATCH ANALYSIS")
print("=" * 60)

# ── Load ──────────────────────────────────────────────────────────
df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
df = df.toDF(*[c.strip() for c in df.columns])
df = df.withColumn(
    "delivery_time_min",
    regexp_replace(col("Time_taken (min)"), r"\(min\)\s*", "").cast(DoubleType())
)
for c in ["Weather_conditions", "Road_traffic_density",
          "Type_of_vehicle", "Type_of_order", "City", "Festival"]:
    if c in df.columns:
        df = df.withColumn(c, trim(col(c)))

df = df.filter(col("delivery_time_min").isNotNull() & (col("delivery_time_min") > 0))
print(f"\n✅ Valid records: {df.count():,}")

# ── 1. Multi-Factor Summary (Key Factors) ────────────────────────
print("\n📊 [1] Key Factors Impacting Avg Delivery Time")
for factor in ["Weather_conditions", "Road_traffic_density",
               "Type_of_vehicle", "City", "Festival"]:
    if factor not in df.columns:
        continue
    result = df.groupBy(factor).agg(
        spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
        count("*").alias("orders")
    ).orderBy("avg_min", ascending=False)
    print(f"  -- {factor} --")
    result.show(truncate=False)
    result.write.mode("overwrite").csv(
        f"{HDFS_OUTPUT}/b1_{factor.lower()}", header=True)

# ── 2. Rider Performance ─────────────────────────────────────────
print("\n📊 [2] Rider Performance — Highest & Lowest")
riders = df.groupBy("Delivery_person_ID").agg(
    spark_round(avg(col("Delivery_person_Ratings").cast(DoubleType())), 2).alias("avg_rating"),
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
).filter(col("orders") >= 5)

print("  Top 10 riders (highest rating):")
riders.orderBy("avg_rating", ascending=False).limit(10).show()
riders.orderBy("avg_rating", ascending=False).limit(10) \
      .write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b2_top_riders", header=True)

print("  Bottom 10 riders (lowest rating):")
riders.orderBy("avg_rating", ascending=True).limit(10).show()
riders.orderBy("avg_rating", ascending=True).limit(10) \
      .write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b2_bottom_riders", header=True)

# ── 3. By Order Type & Vehicle ───────────────────────────────────
print("\n📊 [3] Delivery Time by Type_of_order")
ord_type = df.groupBy("Type_of_order").agg(
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
).orderBy("avg_min", ascending=False)
ord_type.show()
ord_type.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b3_by_order", header=True)

print("\n📊 [3] Delivery Time by Type_of_vehicle")
veh = df.groupBy("Type_of_vehicle").agg(
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
).orderBy("avg_min")
veh.show()
veh.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b3_by_vehicle", header=True)

# ── 4. By Traffic Density ────────────────────────────────────────
print("\n📊 [4] Avg Delivery Delay by Road Traffic Density")
traffic = df.groupBy("Road_traffic_density").agg(
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
).orderBy("avg_min", ascending=False)
traffic.show()
traffic.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b4_by_traffic", header=True)

# ── 5. Festival Impact ───────────────────────────────────────────
print("\n📊 [5] Festival vs Normal Day Performance")
fest = df.groupBy("Festival").agg(
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
)
fest.show()
fest.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b5_festival", header=True)

# ── 6. Weather x Traffic Combination ────────────────────────────
print("\n📊 [6] Weather x Traffic Worst Combinations")
combo = df.groupBy("Weather_conditions", "Road_traffic_density").agg(
    spark_round(avg("delivery_time_min"), 2).alias("avg_min"),
    count("*").alias("orders")
).orderBy("avg_min", ascending=False)
print("  Top 10 worst combinations:")
combo.limit(10).show(truncate=False)
combo.write.mode("overwrite").csv(
    f"{HDFS_OUTPUT}/b6_weather_traffic_combo", header=True)

# ── 7. Distribution & Bottleneck Analysis ────────────────────────
print("\n📊 [7] Delivery Time Distribution & Bottleneck")
stats = df.agg(
    spark_round(avg("delivery_time_min"), 2).alias("mean"),
    spark_round(stddev("delivery_time_min"), 2).alias("std_dev"),
    spark_min("delivery_time_min").alias("min"),
    spark_max("delivery_time_min").alias("max"),
    percentile_approx("delivery_time_min", 0.25).alias("p25"),
    percentile_approx("delivery_time_min", 0.50).alias("p50_median"),
    percentile_approx("delivery_time_min", 0.75).alias("p75"),
    percentile_approx("delivery_time_min", 0.90).alias("p90"),
    percentile_approx("delivery_time_min", 0.95).alias("p95"),
    count("*").alias("total_orders")
)
stats.show(truncate=False)
stats.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b7_distribution", header=True)

# Bottleneck: orders in top-10% delivery time
p90_val = stats.collect()[0]["p90"]
bottleneck = df.filter(col("delivery_time_min") >= p90_val).groupBy(
    "Weather_conditions", "Road_traffic_density", "Type_of_vehicle"
).agg(count("*").alias("count")).orderBy("count", ascending=False)
print(f"  Bottleneck orders (>= p90 = {p90_val} min):")
bottleneck.limit(10).show(truncate=False)
bottleneck.write.mode("overwrite").csv(f"{HDFS_OUTPUT}/b7_bottleneck", header=True)

print(f"\n✅ Results saved to {HDFS_OUTPUT}")
spark.stop()