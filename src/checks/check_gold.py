# Databricks notebook source

from gold_rules import find_gold_problems

dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")


gold = spark.read.table(f"{catalog}.{schema}.gold_revenue_by_segment")
problems = find_gold_problems(gold)
assert not problems, "Gold checks failed: " + "; ".join(problems)
print("OK: all gold checks passed")