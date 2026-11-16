from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Bronze: raw orders, loaded incrementally")
def bronze_orders():
    return (
        spark.readStream.table("samples.tpch.orders")
        .withColumn("ingested_at", F.current_timestamp())
    )


@dp.materialized_view(comment="Bronze: raw customers")
def bronze_customers():
    return spark.read.table("samples.tpch.customer")


@dp.table(comment="Silver: validated orders")
@dp.expect("has_comment", "o_comment IS NOT NULL")
@dp.expect_or_drop("positive_price", "o_totalprice > 0")
@dp.expect_or_fail("has_order_key", "o_orderkey IS NOT NULL")
def silver_orders():
    min_date = spark.conf.get("min_order_date")
    return (
        spark.readStream.table("bronze_orders")
        .filter(F.col("o_orderdate") >= min_date)
    )

@dp.materialized_view(comment="Silver: customers with a known segment")
@dp.expect_or_drop("known_segment", "c_mktsegment IS NOT NULL")
def silver_customers():
    return spark.read.table("bronze_customers")

