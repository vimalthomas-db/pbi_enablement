-- PBI Enablement Demo: Retail Analytics Star Schema
-- Creates a catalog.schema with fact + dimension tables for Power BI reporting

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS serverless_1d81nj_catalog.pbi_retail_demo
COMMENT 'Power BI enablement demo - retail analytics star schema';

USE serverless_1d81nj_catalog.pbi_retail_demo;

-- 2. Dimension: Customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT * FROM VALUES
  (1, 'Acme Corp', 'Enterprise', 'Manufacturing', 'West', 'CA', 'San Francisco', '2022-01-15'),
  (2, 'TechFlow Inc', 'Enterprise', 'Technology', 'West', 'WA', 'Seattle', '2021-06-20'),
  (3, 'GreenLeaf LLC', 'Mid-Market', 'Retail', 'East', 'NY', 'New York', '2022-03-10'),
  (4, 'DataDriven Co', 'Enterprise', 'Financial Services', 'East', 'MA', 'Boston', '2021-11-05'),
  (5, 'CloudFirst Ltd', 'SMB', 'Technology', 'Central', 'TX', 'Austin', '2023-01-20'),
  (6, 'RetailMax', 'Enterprise', 'Retail', 'East', 'FL', 'Miami', '2021-08-12'),
  (7, 'HealthPlus', 'Mid-Market', 'Healthcare', 'Central', 'IL', 'Chicago', '2022-07-01'),
  (8, 'FinEdge Partners', 'Enterprise', 'Financial Services', 'East', 'NY', 'New York', '2021-03-15'),
  (9, 'AutoNova', 'Mid-Market', 'Manufacturing', 'Central', 'MI', 'Detroit', '2022-09-25'),
  (10, 'MediaPulse', 'SMB', 'Media', 'West', 'CA', 'Los Angeles', '2023-04-10'),
  (11, 'EduBright', 'SMB', 'Education', 'East', 'MA', 'Cambridge', '2023-06-15'),
  (12, 'LogiTrans', 'Mid-Market', 'Logistics', 'Central', 'OH', 'Columbus', '2022-05-18'),
  (13, 'PharmaCure', 'Enterprise', 'Healthcare', 'East', 'NJ', 'Princeton', '2021-09-30'),
  (14, 'AgriGrow', 'SMB', 'Agriculture', 'Central', 'IA', 'Des Moines', '2023-02-28'),
  (15, 'SolarPeak', 'Mid-Market', 'Energy', 'West', 'AZ', 'Phoenix', '2022-11-10')
AS t(customer_id, customer_name, segment, industry, region, state, city, signup_date);

-- 3. Dimension: Products
CREATE OR REPLACE TABLE dim_products AS
SELECT * FROM VALUES
  (101, 'Platform License', 'Licenses', 'Core', 15000.00, 12000.00),
  (102, 'Premium Support', 'Services', 'Core', 5000.00, 2000.00),
  (103, 'Data Integration Suite', 'Software', 'Analytics', 8500.00, 4500.00),
  (104, 'ML Toolkit', 'Software', 'AI/ML', 12000.00, 6000.00),
  (105, 'Dashboard Builder', 'Software', 'Analytics', 3500.00, 1500.00),
  (106, 'Security Add-on', 'Software', 'Security', 4000.00, 1800.00),
  (107, 'API Gateway', 'Software', 'Infrastructure', 6000.00, 3000.00),
  (108, 'Training Package', 'Services', 'Enablement', 2500.00, 800.00),
  (109, 'Consulting Hours', 'Services', 'Professional', 200.00, 80.00),
  (110, 'Storage Tier - 1TB', 'Infrastructure', 'Storage', 500.00, 150.00)
AS t(product_id, product_name, product_type, category, list_price, cost);

-- 4. Dimension: Sales Reps
CREATE OR REPLACE TABLE dim_sales_reps AS
SELECT * FROM VALUES
  (201, 'Sarah Johnson', 'West', 'Senior AE', '2020-03-15', 500000.00),
  (202, 'Mike Chen', 'East', 'Senior AE', '2019-08-20', 600000.00),
  (203, 'Lisa Park', 'Central', 'AE', '2021-06-01', 400000.00),
  (204, 'James Wilson', 'West', 'AE', '2022-01-10', 350000.00),
  (205, 'Anna Rodriguez', 'East', 'Senior AE', '2020-11-15', 550000.00)
AS t(rep_id, rep_name, territory, title, hire_date, annual_quota);

-- 5. Dimension: Dates (2023-2025)
CREATE OR REPLACE TABLE dim_dates AS
WITH date_series AS (
  SELECT explode(sequence(DATE '2023-01-01', DATE '2025-12-31', INTERVAL 1 DAY)) AS date_key
)
SELECT
  date_key,
  YEAR(date_key) AS year,
  QUARTER(date_key) AS quarter,
  MONTH(date_key) AS month_num,
  DATE_FORMAT(date_key, 'MMMM') AS month_name,
  DATE_FORMAT(date_key, 'MMM') AS month_short,
  DAY(date_key) AS day_of_month,
  DAYOFWEEK(date_key) AS day_of_week,
  DATE_FORMAT(date_key, 'EEEE') AS day_name,
  WEEKOFYEAR(date_key) AS week_of_year,
  CONCAT('Q', QUARTER(date_key), ' ', YEAR(date_key)) AS quarter_label,
  CONCAT(DATE_FORMAT(date_key, 'MMM'), ' ', YEAR(date_key)) AS month_year,
  CASE WHEN DAYOFWEEK(date_key) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  TRUNC(date_key, 'MM') AS first_of_month,
  LAST_DAY(date_key) AS last_of_month
FROM date_series;

-- 6. Fact: Sales Orders (~400 rows, realistic distribution)
CREATE OR REPLACE TABLE fact_sales AS
WITH base_orders AS (
  SELECT explode(sequence(1, 400)) AS order_seq
),
generated AS (
  SELECT
    order_seq AS order_id,
    -- Weighted customer distribution (enterprise customers order more)
    CASE
      WHEN order_seq % 30 < 4 THEN 1  WHEN order_seq % 30 < 8 THEN 2
      WHEN order_seq % 30 < 11 THEN 3 WHEN order_seq % 30 < 15 THEN 4
      WHEN order_seq % 30 < 17 THEN 5 WHEN order_seq % 30 < 20 THEN 6
      WHEN order_seq % 30 < 22 THEN 7 WHEN order_seq % 30 < 25 THEN 8
      WHEN order_seq % 30 < 27 THEN 9 WHEN order_seq % 30 < 28 THEN 10
      WHEN order_seq % 30 < 29 THEN 11 ELSE (order_seq % 5) + 11
    END AS customer_id,
    -- Product distribution
    CASE
      WHEN order_seq % 20 < 4 THEN 101 WHEN order_seq % 20 < 7 THEN 102
      WHEN order_seq % 20 < 10 THEN 103 WHEN order_seq % 20 < 12 THEN 104
      WHEN order_seq % 20 < 14 THEN 105 WHEN order_seq % 20 < 16 THEN 106
      WHEN order_seq % 20 < 17 THEN 107 WHEN order_seq % 20 < 18 THEN 108
      WHEN order_seq % 20 < 19 THEN 109 ELSE 110
    END AS product_id,
    -- Date spread across 2023-2025, with growth trend
    DATE_ADD('2023-01-01',
      CASE
        WHEN order_seq <= 100 THEN (order_seq * 3.5)::INT          -- 2023: ~100 orders
        WHEN order_seq <= 250 THEN 365 + ((order_seq - 100) * 2.3)::INT  -- 2024: ~150 orders
        ELSE 730 + ((order_seq - 250) * 2.0)::INT                  -- 2025: ~150 orders
      END
    ) AS order_date,
    -- Quantity
    CASE
      WHEN order_seq % 7 = 0 THEN 5 WHEN order_seq % 5 = 0 THEN 3
      WHEN order_seq % 3 = 0 THEN 2 ELSE 1
    END AS quantity,
    -- Discount (some orders get discounts)
    CASE
      WHEN order_seq % 10 = 0 THEN 0.20  WHEN order_seq % 7 = 0 THEN 0.15
      WHEN order_seq % 4 = 0 THEN 0.10   WHEN order_seq % 3 = 0 THEN 0.05
      ELSE 0.0
    END AS discount_pct,
    -- Status
    CASE
      WHEN order_seq % 25 = 0 THEN 'Cancelled'
      WHEN order_seq % 15 = 0 THEN 'Pending'
      WHEN order_seq % 40 = 0 THEN 'Refunded'
      ELSE 'Completed'
    END AS order_status
  FROM base_orders
)
SELECT
  g.order_id,
  g.customer_id,
  g.product_id,
  -- Assign rep based on customer region
  CASE c.region
    WHEN 'West' THEN (CASE WHEN g.order_seq % 2 = 0 THEN 201 ELSE 204 END)
    WHEN 'East' THEN (CASE WHEN g.order_seq % 2 = 0 THEN 202 ELSE 205 END)
    ELSE 203
  END AS rep_id,
  g.order_date,
  g.quantity,
  p.list_price AS unit_price,
  g.discount_pct,
  ROUND(g.quantity * p.list_price * (1 - g.discount_pct), 2) AS revenue,
  ROUND(g.quantity * p.cost, 2) AS cost,
  ROUND(g.quantity * p.list_price * (1 - g.discount_pct) - g.quantity * p.cost, 2) AS profit,
  g.order_status
FROM generated g
JOIN dim_customers c ON g.customer_id = c.customer_id
JOIN dim_products p ON g.product_id = p.product_id
WHERE g.order_date <= CURRENT_DATE();

-- 7. Fact: Support Tickets (intentionally messy for Power Query cleanup)
CREATE OR REPLACE TABLE fact_support_tickets_raw AS
SELECT * FROM VALUES
  (1, 1, '2024-01-15 09:30:00', '2024-01-15 14:20:00', 'High', 'Resolved', 'Platform - Login issues', 4.8, 8),
  (2, 2, '2024-01-20 11:00:00', '2024-01-22 16:00:00', 'medium', 'Resolved', 'integration -- sync failure', 53.0, 7),
  (3, 3, '2024-02-05 08:15:00', '2024-02-05 10:00:00', 'Low', 'Resolved', 'Dashboard: display bug', 1.75, 9),
  (4, 4, '2024-02-10 14:00:00', '2024-02-14 09:30:00', 'HIGH', 'Resolved', 'ML Toolkit - model deploy fail', 91.5, 6),
  (5, 1, '2024-03-01 10:00:00', NULL, 'Critical', 'Open', 'Platform outage -- production', NULL, NULL),
  (6, 6, '2024-03-15 16:30:00', '2024-03-16 11:00:00', 'high', 'Resolved', 'Security: access denied errors', 18.5, 7),
  (7, 8, '2024-04-02 09:00:00', '2024-04-03 15:00:00', 'Medium', 'Resolved', 'API Gateway - rate limiting', 30.0, 8),
  (8, 2, '2024-04-20 13:45:00', '2024-04-20 17:30:00', 'low', 'Resolved', 'UI: minor formatting', 3.75, 9),
  (9, 5, '2024-05-10 08:00:00', '2024-05-12 12:00:00', 'Medium', 'Resolved', 'Storage: data not loading', 52.0, 6),
  (10, 7, '2024-05-25 11:30:00', NULL, 'High', 'In Progress', 'Dashboard Builder - export broken', NULL, NULL),
  (11, 13, '2024-06-05 09:15:00', '2024-06-06 10:00:00', 'HIGH', 'Resolved', 'Compliance: audit log gap', 24.75, 8),
  (12, 9, '2024-06-20 14:00:00', '2024-06-21 09:00:00', 'medium', 'Resolved', 'Integration - connector timeout', 19.0, 7),
  (13, 4, '2024-07-08 10:30:00', '2024-07-10 16:00:00', 'High', 'Resolved', 'ML Toolkit: training stuck', 53.5, 5),
  (14, 12, '2024-07-22 08:45:00', '2024-07-22 12:00:00', 'Low', 'Resolved', 'Docs -- broken link', 3.25, 10),
  (15, 6, '2024-08-05 15:00:00', NULL, 'Critical', 'Escalated', 'Platform -- data corruption risk', NULL, NULL),
  (16, 1, '2024-08-15 09:00:00', '2024-08-16 14:00:00', 'medium', 'Resolved', 'Storage: slow query perf', 29.0, 7),
  (17, 3, '2024-09-01 11:00:00', '2024-09-01 15:30:00', 'Low', 'Resolved', 'Dashboard: chart render lag', 4.5, 8),
  (18, 8, '2024-09-18 13:00:00', '2024-09-20 10:00:00', 'HIGH', 'Resolved', 'API Gateway -- auth failure', 45.0, 6),
  (19, 10, '2024-10-05 10:00:00', '2024-10-05 16:00:00', 'Medium', 'Resolved', 'Training pkg: content outdated', 6.0, 8),
  (20, 15, '2024-10-20 08:30:00', NULL, 'High', 'Open', 'Integration - pipeline break', NULL, NULL)
AS t(ticket_id, customer_id, created_at, resolved_at, priority, status, issue_description, resolution_hours, csat_score);

-- 8. Verify
SELECT 'dim_customers' AS tbl, COUNT(*) AS cnt FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_sales_reps', COUNT(*) FROM dim_sales_reps
UNION ALL SELECT 'dim_dates', COUNT(*) FROM dim_dates
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL SELECT 'fact_support_tickets_raw', COUNT(*) FROM fact_support_tickets_raw;
