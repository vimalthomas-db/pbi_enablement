-- Original sales metric view (FK-based dimensions, no denormalization)
-- Source: fact_sales directly
-- Workspace: adb-984752964297111.11.azuredatabricks.net
-- Catalog: vjoseph_pbi_demo | Schema: pbi_retail_demo

CREATE OR REPLACE VIEW vjoseph_pbi_demo.pbi_retail_demo.sales_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Sales KPIs for Power BI retail demo"
source: vjoseph_pbi_demo.pbi_retail_demo.fact_sales
dimensions:
  - name: order_date
    expr: order_date
  - name: customer_id
    expr: customer_id
  - name: product_id
    expr: product_id
  - name: rep_id
    expr: rep_id
  - name: order_status
    expr: order_status
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
$$;
