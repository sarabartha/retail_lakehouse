CREATE OR REFRESH MATERIALIZED VIEW fact_orders AS
SELECT order_key, customer_id AS customer_key, order_date AS date_key,
       order_priority, total_price
FROM ${cdp_serve}.order_management_orders;

CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
SELECT c_custkey AS customer_key, c_name AS customer_name,
       c_mktsegment AS market_segment, c_nationkey AS nation_key
FROM samples.tpch.customer;

CREATE OR REFRESH MATERIALIZED VIEW dim_date AS
SELECT d AS date_key, year(d) AS year, month(d) AS month,
       date_format(d, 'MMMM') AS month_name, dayofweek(d) AS day_of_week
FROM (SELECT explode(sequence(DATE'1992-01-01', DATE'1998-12-31', INTERVAL 1 DAY)) AS d);