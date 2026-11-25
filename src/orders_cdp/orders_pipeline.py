"""Orders product: Raw, Processed and Serve layers."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
RAW = f"{CATALOG}.{spark.conf.get('raw_schema')}"
PROCESSED = f"{CATALOG}.{spark.conf.get('processed_schema')}"
SERVE = f"{CATALOG}.{spark.conf.get('serve_schema')}"
LANDING_PATH = spark.conf.get("landing_path")

ORDER_SCHEMA = (
    "o_orderkey LONG, o_custkey LONG, o_orderstatus STRING, o_totalprice DOUBLE, "
    "o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, "
    "o_shippriority INT, o_comment STRING"
)


@dp.table(
    name=f"{RAW}.order_management_orders",
    comment="Raw: order files as delivered, with load metadata",
)
def orders_raw():
    """Load each landed JSON line untouched, plus load metadata and privacy fields."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("pathGlobFilter", "*.json")
        .load(LANDING_PATH)
        .select(
            F.current_timestamp().alias("ingestion_timestamp"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("_metadata.file_name").alias("file_name"),
            F.lit("tpch_orders").alias("source_id"),
            F.get_json_object("value", "$.o_custkey").cast("long").alias("customer_id"),
            F.to_date(F.get_json_object("value", "$.o_orderdate")).alias("readout_date"),
            F.col("value").alias("content"),
        )
    )


@dp.materialized_view(
    name=f"{PROCESSED}.order_management_orders_latest",
    comment="Processed: one row per order",
)
def orders_latest():
    """Parse the JSON and keep only the most recently loaded copy of each order."""
    latest_first = Window.partitionBy("order_key").orderBy(F.col("ingestion_timestamp").desc())
    return (
        spark.read.table(f"{RAW}.order_management_orders")
        .withColumn("data", F.from_json("content", ORDER_SCHEMA))
        .withColumn("order_key", F.col("data.o_orderkey"))
        .withColumn("row_number", F.row_number().over(latest_first))
        .filter("row_number = 1")
        .drop("row_number")
    )


@dp.materialized_view(
    name=f"{SERVE}.order_management_orders",
    comment="Serve: flattened, typed orders",
)
@dp.expect_or_fail("has_order_key", "order_key IS NOT NULL")
@dp.expect_or_drop("positive_price", "total_price > 0")
def orders_serve():
    """Flatten processed orders into typed, readable, source-aligned columns."""
    return spark.read.table(f"{PROCESSED}.order_management_orders_latest").select(
        "ingestion_timestamp",
        "file_path",
        "file_name",
        "source_id",
        "customer_id",
        "readout_date",
        "order_key",
        F.col("data.o_orderstatus").alias("order_status"),
        F.col("data.o_totalprice").alias("total_price"),
        F.col("data.o_orderdate").alias("order_date"),
        F.col("data.o_orderpriority").alias("order_priority"),
        F.col("data.o_clerk").alias("clerk"),
        F.col("data.o_shippriority").alias("ship_priority"),
        F.col("data.o_comment").alias("comment"),
    )
