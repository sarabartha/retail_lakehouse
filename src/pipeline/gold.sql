CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_segment
COMMENT "Gold: monthly orders and revenue per market segment"
AS SELECT
  c.c_mktsegment AS market_segment,
  date_trunc('MONTH', o.o_orderdate) AS order_month,
  COUNT(*) AS orders,
  ROUND(SUM(o.o_totalprice), 2) AS revenue,
  SUM(CASE WHEN o.o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END) AS urgent_orders
  FROM silver_orders o
JOIN silver_customers c ON o.o_custkey = c.c_custkey
GROUP BY ALL;