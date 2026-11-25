"""Gold layer of the retail pipeline."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Gold: monthly orders and revenue per market segment")
def gold_revenue_by_segment():
    """Count orders, urgent orders and revenue per segment and month.

    Returns:
        One row per market segment and month.
    """
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")
    is_urgent = F.when(F.col("o_orderpriority") == "1-URGENT", 1).otherwise(0)
    return (
        orders.join(customers, orders.o_custkey == customers.c_custkey)
        .groupBy(
            F.col("c_mktsegment").alias("market_segment"),
            F.date_trunc("MONTH", "o_orderdate").alias("order_month"),
        )
        .agg(
            F.count("*").alias("orders"),
            F.sum(is_urgent).alias("urgent_orders"),
            F.round(F.sum("o_totalprice"), 2).alias("revenue"),
        )
    )
