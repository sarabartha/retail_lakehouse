# Databricks notebook source
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("month", "1998-01")
path = dbutils.widgets.get("landing_path")
month = dbutils.widgets.get("month")

batch = (
    spark.read.table("samples.tpch.orders")
    .filter(f"date_format(o_orderdate, 'yyyy-MM') = '{month}'")
)
batch.write.mode("overwrite").option("header", True).csv(f"{path}/{month}")
print(f"Landed {batch.count()} orders for {month}")