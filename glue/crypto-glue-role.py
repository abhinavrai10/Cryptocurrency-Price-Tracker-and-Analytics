import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# ----------------------------
# Glue Job Setup
# ----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------
# Schema for Kafka messages
# ----------------------------
schema = StructType([
    StructField("coin", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("timestamp", TimestampType(), True)
])

# ----------------------------
# Read stream from Kafka
# ----------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.23.60:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON into columns
parsed_df = kafka_df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add watermark for late data handling
watermarked_df = parsed_df.withWatermark("timestamp", "1 minute")

# Add partition columns for S3
watermarked_df = watermarked_df \
    .withColumn("year", F.year("timestamp")) \
    .withColumn("month", F.month("timestamp")) \
    .withColumn("day", F.dayofmonth("timestamp")) \
    .withColumn("hour", F.hour("timestamp"))

# ----------------------------
# Write RAW data to S3
# ----------------------------
raw_query = watermarked_df.writeStream \
    .format("parquet") \
    .option("path", "s3://coingecko-crypto-bucket/raw/") \
    .option("checkpointLocation", "s3://coingecko-crypto-bucket/checkpoints/raw/") \
    .partitionBy("year", "month", "day", "hour") \
    .outputMode("append") \
    .start()

# ----------------------------
# Function to enrich and write in foreachBatch
# ----------------------------
def enrich_and_write(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    unbounded_window = Window.partitionBy("coin").orderBy("timestamp")

    enriched_df = batch_df.withColumn(
        "price_change_1min",
        (F.col("price_usd") - F.lag("price_usd", 2).over(unbounded_window)) / F.lag("price_usd", 2).over(unbounded_window) * 100
    ).withColumn(
        "price_change_5min",
        (F.col("price_usd") - F.lag("price_usd", 10).over(unbounded_window)) / F.lag("price_usd", 10).over(unbounded_window) * 100
    ).withColumn(
        "sma_5", F.avg("price_usd").over(unbounded_window.rowsBetween(-4, 0))
    ).withColumn(
        "sma_10", F.avg("price_usd").over(unbounded_window.rowsBetween(-9, 0))
    ).withColumn(
        "volatility_5min", F.stddev("price_usd").over(unbounded_window.rowsBetween(-4, 0))
    )

    ema_window = unbounded_window.rowsBetween(-11, 0)
    enriched_df = enriched_df.withColumn(
        "price_list", F.collect_list("price_usd").over(ema_window)
    ).withColumn(
        "price_list", F.reverse("price_list")
    )

    def compute_ema(prices):
        try:
            if not prices:
                return 0.0
            alpha = 2 / (len(prices) + 1)
            ema = prices[0]
            for p in prices[1:]:
                ema = alpha * p + (1 - alpha) * ema
            return float(ema)
        except Exception as e:
            return 0.0

    ema_udf = F.udf(compute_ema, DoubleType())
    enriched_df = enriched_df.withColumn("ema_12", ema_udf("price_list")).drop("price_list")

    enriched_df.write.mode("append").partitionBy("year", "month", "day", "hour").parquet(
        "s3://coingecko-crypto-bucket/enriched/"
    )

# ----------------------------
# Write ENRICHED data via foreachBatch
# ----------------------------
enriched_query = watermarked_df.writeStream \
    .foreachBatch(enrich_and_write) \
    .option("checkpointLocation", "s3://coingecko-crypto-bucket/checkpoints/enriched/") \
    .start()

# ----------------------------
# Await termination
# ----------------------------
raw_query.awaitTermination()
enriched_query.awaitTermination()

job.commit()