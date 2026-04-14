-- Denormalized source view: fact_sales + all dimension tables
-- Required because metric views do NOT support JOINs
-- This is the foundation for the comprehensive metric view

CREATE OR REPLACE VIEW vjoseph_pbi_demo.pbi_retail_demo.vw_sales_enriched AS
SELECT
  f.order_id, f.order_date, f.order_status, f.quantity, f.revenue, f.cost, f.profit,
  p.product_name, p.category AS product_category, p.product_type, p.list_price,
  r.rep_name, r.territory, r.title AS rep_title,
  c.customer_name, c.segment AS customer_segment, c.region, c.city, c.state,
  d.month_name, d.quarter, d.year, d.day_of_week
FROM vjoseph_pbi_demo.pbi_retail_demo.fact_sales f
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_products p ON f.product_id = p.product_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_sales_reps r ON f.rep_id = r.rep_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_customers c ON f.customer_id = c.customer_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_dates d ON f.order_date = d.date_key;
