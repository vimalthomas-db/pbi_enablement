-- Support detail view: raw tickets + customer names
-- Used for row-level detail tables in dashboards (not aggregated metrics)

CREATE OR REPLACE VIEW vjoseph_pbi_demo.pbi_retail_demo.vw_support_detail AS
SELECT
  t.ticket_id, t.created_at, t.priority, t.status, t.issue_description,
  t.resolution_hours, t.csat_score,
  c.customer_name
FROM vjoseph_pbi_demo.pbi_retail_demo.fact_support_tickets_raw t
JOIN vjoseph_pbi_demo.pbi_retail_demo.dim_customers c ON t.customer_id = c.customer_id;
