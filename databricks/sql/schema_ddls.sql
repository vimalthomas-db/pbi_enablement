-- ============================================
-- dim_customers
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.dim_customers (
  customer_id INT,
  customer_name STRING,
  segment STRING,
  industry STRING,
  region STRING,
  state STRING,
  city STRING,
  signup_date STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- dim_dates
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.dim_dates (
  date_key DATE,
  year INT,
  quarter INT,
  month_num INT,
  month_name STRING,
  month_short STRING,
  day_of_month INT,
  day_of_week INT,
  day_name STRING,
  week_of_year INT,
  quarter_label STRING,
  month_year STRING,
  is_weekend BOOLEAN,
  first_of_month DATE,
  last_of_month DATE)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- dim_products
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.dim_products (
  product_id INT,
  product_name STRING,
  product_type STRING,
  category STRING,
  list_price DECIMAL(7,2),
  cost DECIMAL(7,2))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- dim_sales_reps
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.dim_sales_reps (
  rep_id INT,
  rep_name STRING,
  territory STRING,
  title STRING,
  hire_date STRING,
  annual_quota DECIMAL(8,2))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- fact_sales
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.fact_sales (
  order_id INT,
  customer_id INT,
  product_id INT,
  rep_id INT,
  order_date DATE,
  quantity INT,
  unit_price DECIMAL(7,2),
  discount_pct DECIMAL(2,2),
  revenue DECIMAL(22,2),
  cost DECIMAL(19,2),
  profit DECIMAL(23,2),
  order_status STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- fact_support_tickets_raw
-- ============================================
CREATE TABLE vjoseph_pbi_demo.pbi_retail_demo.fact_support_tickets_raw (
  ticket_id INT,
  customer_id INT,
  created_at STRING,
  resolved_at STRING,
  priority STRING,
  status STRING,
  issue_description STRING,
  resolution_hours DECIMAL(4,2),
  csat_score INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')


-- ============================================
-- vw_sales_enriched
-- ============================================
CREATE VIEW pbi_retail_demo.vw_sales_enriched (
  order_id,
  order_date,
  order_status,
  quantity,
  revenue,
  cost,
  profit,
  product_name,
  product_category,
  product_type,
  list_price,
  rep_name,
  territory,
  rep_title,
  customer_name,
  customer_segment,
  region,
  city,
  state,
  month_name,
  quarter,
  year,
  day_of_week)
WITH SCHEMA COMPENSATION
AS SELECT 
  f.order_id, f.order_date, f.order_status, f.quantity, f.revenue, f.cost, f.profit,
  p.product_name, p.category as product_category, p.product_type, p.list_price,
  r.rep_name, r.territory, r.title as rep_title,
  c.customer_name, c.segment as customer_segment, c.region, c.city, c.state,
  d.month_name, d.quarter, d.year, d.day_of_week
FROM vjoseph_pbi_demo.pbi_retail_demo.fact_sales f
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_products p ON f.product_id = p.product_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_sales_reps r ON f.rep_id = r.rep_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_customers c ON f.customer_id = c.customer_id
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_dates d ON f.order_date = d.date_key


-- ============================================
-- vw_support_detail
-- ============================================
CREATE VIEW pbi_retail_demo.vw_support_detail (
  ticket_id,
  created_at,
  priority,
  status,
  issue_description,
  resolution_hours,
  csat_score,
  customer_name)
WITH SCHEMA COMPENSATION
AS SELECT 
  t.ticket_id, t.created_at, t.priority, t.status, t.issue_description,
  t.resolution_hours, t.csat_score,
  c.customer_name
FROM vjoseph_pbi_demo.pbi_retail_demo.fact_support_tickets_raw t
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_customers c ON t.customer_id = c.customer_id


-- ============================================
-- METRIC VIEW: sales_metrics
-- (DESCRIBE output — SHOW CREATE TABLE not supported for metric views)
-- ============================================
--   order_date                     date
--   customer_id                    int
--   product_id                     int
--   rep_id                         int
--   order_status                   string
--   Total Revenue                  decimal(32,2) measure
--   Total Profit                   decimal(33,2) measure
--   Total Cost                     decimal(29,2) measure
--   Order Count                    bigint measure
--   Avg Order Value                decimal(38,8) measure
--   Profit Margin                  decimal(38,6) measure
--   Revenue Completed              decimal(32,2) measure

-- ============================================
-- METRIC VIEW: sales_metrics_full
-- (DESCRIBE output — SHOW CREATE TABLE not supported for metric views)
-- ============================================
--   order_date                     date
--   order_status                   string
--   product_name                   string
--   product_category               string
--   rep_name                       string
--   territory                      string
--   customer_name                  string
--   customer_segment               string
--   region                         string
--   city                           string
--   state                          string
--   month_name                     string
--   quarter                        int
--   year                           int
--   day_of_week                    int
--   Total Revenue                  decimal(32,2) measure
--   Total Profit                   decimal(33,2) measure
--   Total Cost                     decimal(29,2) measure
--   Order Count                    bigint measure
--   Avg Order Value                decimal(38,8) measure
--   Profit Margin                  decimal(38,6) measure
--   Revenue Completed              decimal(32,2) measure
--   Total Quantity                 bigint measure

-- ============================================
-- METRIC VIEW: support_metrics
-- (DESCRIBE output — SHOW CREATE TABLE not supported for metric views)
-- ============================================
--   created_at                     string
--   customer_id                    int
--   priority                       string
--   status                         string
--   Ticket Count                   bigint measure
--   Avg Resolution Hours           decimal(8,6) measure
--   Avg CSAT                       double measure
--   Open Tickets                   bigint measure
