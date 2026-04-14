-- Comprehensive sales metric view with ALL dimension attributes
-- Source: vw_sales_enriched (denormalized fact + dims)
-- This is the production metric view used by AI/BI Dashboards and PBI

CREATE OR REPLACE VIEW vjoseph_pbi_demo.pbi_retail_demo.sales_metrics_full
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Comprehensive Sales KPIs with all dimensions"
source: vjoseph_pbi_demo.pbi_retail_demo.vw_sales_enriched
dimensions:
  - name: order_date
    expr: order_date
  - name: order_status
    expr: order_status
  - name: product_name
    expr: product_name
  - name: product_category
    expr: product_category
  - name: rep_name
    expr: rep_name
  - name: territory
    expr: territory
  - name: customer_name
    expr: customer_name
  - name: customer_segment
    expr: customer_segment
  - name: region
    expr: region
  - name: city
    expr: city
  - name: state
    expr: state
  - name: month_name
    expr: month_name
  - name: quarter
    expr: quarter
  - name: year
    expr: year
  - name: day_of_week
    expr: day_of_week
measures:
  - name: Total Revenue
    expr: SUM(revenue)
  - name: Total Profit
    expr: SUM(profit)
  - name: Total Cost
    expr: SUM(cost)
  - name: Order Count
    expr: COUNT(1)
  - name: Avg Order Value
    expr: SUM(revenue) / NULLIF(COUNT(1), 0)
  - name: Profit Margin
    expr: SUM(profit) / NULLIF(SUM(revenue), 0)
  - name: Revenue Completed
    expr: SUM(revenue) FILTER (WHERE order_status = 'Completed')
  - name: Total Quantity
    expr: SUM(quantity)
$$;
