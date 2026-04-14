-- Support ticket metric view
-- Source: fact_support_tickets_raw directly (no denormalization needed — fewer dims)

CREATE OR REPLACE VIEW vjoseph_pbi_demo.pbi_retail_demo.support_metrics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Support ticket KPIs"
source: vjoseph_pbi_demo.pbi_retail_demo.fact_support_tickets_raw
dimensions:
  - name: created_at
    expr: created_at
  - name: customer_id
    expr: customer_id
  - name: priority
    expr: priority
  - name: status
    expr: status
measures:
  - name: Ticket Count
    expr: COUNT(1)
  - name: Avg Resolution Hours
    expr: AVG(resolution_hours)
  - name: Avg CSAT
    expr: AVG(csat_score)
  - name: Open Tickets
    expr: COUNT(1) FILTER (WHERE status <> 'Resolved')
$$;
