# Databricks notebook source
dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

table = f"{catalog}.{schema}.gold_revenue_by_segment"
gold = spark.read.table(table)

rows = gold.count()
segments = gold.select("market_segment").distinct().count()
negative = gold.filter("revenue <= 0").count()

assert rows > 0, f"{table} is empty"
assert segments == 5, f"Expected 5 market segments, found {segments}"
assert negative == 0, f"{negative} rows have revenue <= 0"
print(f"OK: {rows} rows, {segments} segments")