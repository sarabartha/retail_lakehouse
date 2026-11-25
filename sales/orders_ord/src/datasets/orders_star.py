"""Orders star schema Data Set."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDP_SERVE = spark.conf.get("cdp_serve")


@dp.materialized_view(comment="Fact: one row per order")
def fact_orders():
    """Select order facts from the orders product."""
    return spark.read.table(f"{CDP_SERVE}.order_management_orders").select(
        "order_key",
        F.col("customer_id").alias("customer_key"),
        F.col("order_date").alias("date_key"),
        "order_priority",
        "total_price",
    )


@dp.materialized_view(comment="Dimension: customers")
def dim_customer():
    """Describe each customer with readable column names."""
    return spark.read.table("samples.tpch.customer").select(
        F.col("c_custkey").alias("customer_key"),
        F.col("c_name").alias("customer_name"),
        F.col("c_mktsegment").alias("market_segment"),
        F.col("c_nationkey").alias("nation_key"),
    )


@dp.materialized_view(comment="Dimension: one row per calendar day")
def dim_date():
    """Generate every day from 1992 to 1998 with its parts."""
    days = F.sequence(F.lit("1992-01-01").cast("date"), F.lit("1998-12-31").cast("date"))
    return (
        spark.range(1)
        .select(F.explode(days).alias("date_key"))
        .withColumns(
            {
                "year": F.year("date_key"),
                "month": F.month("date_key"),
                "month_name": F.date_format("date_key", "MMMM"),
                "day_of_week": F.dayofweek("date_key"),
            }
        )
    )
