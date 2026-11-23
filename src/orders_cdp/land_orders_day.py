# Databricks notebook source
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("day", "1998-01-01")
landing_path = dbutils.widgets.get("landing_path")
day = dbutils.widgets.get("day")

orders = spark.read.table("samples.tpch.orders").filter(f"o_orderdate = '{day}'")
orders.write.mode("overwrite").json(f"{landing_path}/{day}")
print(f"Landed {orders.count()} orders for {day}")
