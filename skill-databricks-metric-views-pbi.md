# Databricks Metric Views & Power BI Integration Skill

A comprehensive guide for creating Databricks Metric Views, consuming them from Power BI Desktop, and building AI/BI Dashboards — based on hands-on testing and official documentation as of April 2026.

---

## 1. What Are Metric Views

Metric Views are Unity Catalog objects that define **governed business metrics** (measures + dimensions) in YAML. They provide a single source of truth consumed by Databricks SQL, AI/BI Dashboards, Genie, and — with BI Compatibility Mode — Power BI.

```sql
CREATE OR REPLACE VIEW <catalog>.<schema>.<name>
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Description of this metric view"
source: <catalog>.<schema>.<source_table_or_view>
dimensions:
  - name: <dimension_name>
    expr: <column_expression>
measures:
  - name: <measure_name>
    expr: <aggregate_expression>
$$
```

### Key Rules
- **Source must be a single table or view.** JOINs inside or against a metric view are NOT supported (`METRIC_VIEW_JOIN_NOT_SUPPORTED`).
- **Measures require `MEASURE()` function** to query. `SELECT *` or bare column references to measure columns fail with `METRIC_VIEW_MISSING_MEASURE_FUNCTION`.
- **Dimensions** behave like regular columns — filter, group, sort as normal.
- **YAML delimiter**: Use `$$` (double dollar). Single `$` fails via the Statement Execution API (`PARSE_SYNTAX_ERROR`). The `$yaml$` tag-style delimiter also works.
- **Requires** SQL Warehouse with Databricks Runtime 17.3+.

---

## 2. The Denormalized Source Pattern

Since metric views cannot JOIN, you must **pre-join** fact and dimension tables into a single source view before building the metric view.

### Step 1: Create denormalized source view

```sql
CREATE OR REPLACE VIEW <catalog>.<schema>.vw_sales_enriched AS
SELECT
  f.order_date, f.order_status, f.quantity, f.revenue, f.cost, f.profit,
  p.product_name, p.category AS product_category,
  r.rep_name, r.territory,
  c.customer_name, c.segment AS customer_segment,
  d.month_name, d.quarter, d.year
FROM <schema>.fact_sales f
JOIN <schema>.dim_products p ON f.product_id = p.product_id
JOIN <schema>.dim_sales_reps r ON f.rep_id = r.rep_id
JOIN <schema>.dim_customers c ON f.customer_id = c.customer_id
JOIN <schema>.dim_dates d ON f.order_date = d.date_key
```

### Step 2: Build metric view on the denormalized source

```sql
CREATE OR REPLACE VIEW <catalog>.<schema>.sales_metrics_full
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: <catalog>.<schema>.vw_sales_enriched
dimensions:
  - name: order_date
    expr: order_date
  - name: product_category
    expr: product_category
  - name: rep_name
    expr: rep_name
  - name: customer_segment
    expr: customer_segment
  -- include ALL dimension attributes you'll ever need to slice by
measures:
  - name: Total Revenue
    expr: SUM(revenue)
  - name: Profit Margin
    expr: SUM(profit) / NULLIF(SUM(revenue), 0)
  - name: Revenue Completed
    expr: SUM(revenue) FILTER (WHERE order_status = 'Completed')
$$
```

### Why This Pattern Matters
- Every BI consumer (PBI, AI/BI, Genie) needs dimension attributes like product names, rep names, segments
- Without the denormalized source, you'd only have foreign key IDs as dimensions
- The metric view is the governance layer; the denormalized view is the data layer

---

## 3. Querying Metric Views (Databricks SQL)

### Basic MEASURE() syntax

```sql
-- Grand totals (no GROUP BY)
SELECT MEASURE(`Total Revenue`) as revenue
FROM <schema>.sales_metrics_full

-- Grouped by dimension
SELECT product_category,
  MEASURE(`Total Revenue`) as revenue,
  MEASURE(`Profit Margin`) as margin
FROM <schema>.sales_metrics_full
GROUP BY product_category

-- With filters
SELECT rep_name,
  MEASURE(`Total Revenue`) as revenue
FROM <schema>.sales_metrics_full
WHERE year = 2024
GROUP BY rep_name
ORDER BY revenue DESC

-- Date truncation
SELECT DATE_TRUNC('month', order_date) as month,
  MEASURE(`Total Revenue`) as revenue
FROM <schema>.sales_metrics_full
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month
```

### What Fails

```sql
-- FAILS: SELECT * (measures can't be selected without MEASURE())
SELECT * FROM sales_metrics_full

-- FAILS: Bare column reference to measure
SELECT `Total Revenue` FROM sales_metrics_full

-- FAILS: Standard aggregation on measure column
SELECT SUM(`Total Revenue`) FROM sales_metrics_full

-- FAILS: JOIN with another table
SELECT s.*, d.dim_col
FROM sales_metrics_full s
JOIN other_table d ON s.id = d.id
```

---

## 4. Power BI Integration Options

### Option 0: BI Compatibility Mode (Beta, Recommended When Available)

**Status:** Beta as of April 2026. Requires PBI Desktop v2.151.1052.0+ (Feb 2026).

**How it works:** Databricks intercepts PBI-generated SQL and rewrites standard aggregations (SUM, COUNT, etc.) to `MEASURE()` calls. PBI sees the metric view as a normal table.

**Setup:**
1. Get Data → Databricks
2. Enter Server Hostname and HTTP Path
3. **Advanced Options → Metric View BI Compatibility Mode → Enabled**
4. **Data Connectivity mode → DirectQuery** (Import is NOT supported)
5. Navigate to metric view in Navigator, Load

**Critical rules:**
- Always leave aggregation as **SUM** for measure columns (all agg types rewrite to same MEASURE())
- AVG shows `1.0` (because SUM/COUNT return same value, and PBI computes AVG = SUM/COUNT)
- Measures go in **Values** wells ONLY — never drag to axis, legend, slicer, or GROUP BY
- Grand totals are **wrong for non-additive measures** (ratios, percentages, DISTINCT counts)
- **No joins** between metric view and other PBI tables
- COUNT(Distinct), StdDev, Variance, Median → errors or wrong results
- Quantitative range slicers on measures don't work (range collapses to single point)

**Best practices (from Microsoft docs):**
- Create DAX "wrapper measures": `Total Sales = SUM('MetricView'[total_revenue])`
- Hide original measure columns, expose only wrappers
- Set numeric dimension columns to "Don't Aggregate"
- Organize dimensions into folders, measures into a "Measures" folder

**Source:** https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi-metric-views

---

### Option 1: Wrapper SQL Views + DirectQuery (No Beta dependency)

Create regular SQL views that call MEASURE() for you. PBI sees them as normal tables.

```sql
CREATE OR REPLACE VIEW <schema>.vw_sales_for_pbi AS
SELECT
  order_date, order_status, product_category, rep_name,
  MEASURE(`Total Revenue`) as total_revenue,
  MEASURE(`Total Profit`) as total_profit,
  MEASURE(`Order Count`) as order_count
FROM <schema>.sales_metrics_full
GROUP BY order_date, order_status, product_category, rep_name
```

PBI connects to `vw_sales_for_pbi` in DirectQuery mode. No special settings needed.

| Pro | Con |
|-----|-----|
| Works today, no Beta needed | Must decide GROUP BY granularity upfront |
| PBI treats it as a regular view | Adding dimensions = alter the view |
| DirectQuery supported | Two objects to maintain per metric view |

---

### Option 2: Custom SQL in PBI (DirectQuery)

In PBI: Get Data → Databricks → Advanced Options → SQL statement:

```sql
SELECT order_date, product_category,
  MEASURE(`Total Revenue`) as total_revenue,
  MEASURE(`Profit Margin`) as profit_margin
FROM <schema>.sales_metrics_full
GROUP BY order_date, product_category
```

| Pro | Con |
|-----|-----|
| No extra Databricks objects | PBI can't fold additional aggregations |
| Full control | SQL embedded in PBI file, not governed |
| DirectQuery works | Each PBI author writes own SQL |

---

### Option 3: Materialized Table + Import Mode

Scheduled job materializes metric view results into a Delta table. PBI imports it.

```sql
-- Databricks job runs on schedule
CREATE OR REPLACE TABLE <schema>.sales_summary AS
SELECT order_date, product_category, rep_name,
  MEASURE(`Total Revenue`) as total_revenue,
  MEASURE(`Total Profit`) as total_profit,
  MEASURE(`Order Count`) as order_count
FROM <schema>.sales_metrics_full
GROUP BY order_date, product_category, rep_name
```

| Pro | Con |
|-----|-----|
| Import mode = fast PBI experience | Data is stale (as of last refresh) |
| Full DAX capability | Extra storage + scheduled job |
| Any PBI version works | Not real-time |

---

### Option 4: Tabular Editor Semantic Bridge (Third-party)

Tabular Editor 3's Semantic Bridge imports Databricks metric definitions into PBI's semantic model. Metric definitions stay in Databricks; PBI gets a proper semantic model.

| Pro | Con |
|-----|-----|
| Single source of truth in Databricks | Third-party license required |
| PBI gets proper semantic model | Currently MVP stage |
| No wrapper views needed | Adds tooling dependency |

**Source:** https://tabulareditor.com/blog/bridge-analytics-in-databricks-and-power-bi-via-tabular-editor

---

### Option 5: AI/BI Dashboards (Databricks Native, Zero Friction)

Skip PBI. Use Databricks AI/BI Dashboards that natively understand MEASURE().

| Pro | Con |
|-----|-----|
| No translation layer | Not PBI (org may mandate PBI) |
| Genie integration for NL queries | Fewer viz types than PBI |
| Governed end-to-end in UC | Different sharing/publishing model |

---

### Decision Framework

```
Need Power BI?
├── Yes
│   ├── BI Compat Mode available? → Option 0 (simplest)
│   ├── Need real-time, no Beta? → Option 1 (wrapper views + DirectQuery)
│   ├── Need full DAX power? → Option 3 (materialized + Import)
│   ├── Have Tabular Editor? → Option 4 (Semantic Bridge)
│   └── One-off analysis? → Option 2 (custom SQL)
└── No / flexible
    └── Option 5: AI/BI Dashboard (native)
```

---

## 5. AI/BI Dashboard Creation via API

### API Endpoint

```
POST   /api/2.0/lakeview/dashboards              — Create
PATCH  /api/2.0/lakeview/dashboards/{id}          — Update (requires etag)
POST   /api/2.0/lakeview/dashboards/{id}/published — Publish
GET    /api/2.0/lakeview/dashboards/{id}          — Get (includes serialized_dashboard + etag)
```

### Dashboard JSON Structure (serialized_dashboard)

```json
{
  "datasets": [
    {
      "name": "ds_unique_id",
      "displayName": "Human Readable Name",
      "queryLines": ["SELECT ...\n", "FROM ...\n", "GROUP BY ..."]
    }
  ],
  "pages": [
    {
      "name": "pg_unique_id",
      "displayName": "Page Title",
      "layout": [ /* array of widget definitions */ ],
      "pageType": "PAGE_TYPE_CANVAS"
    }
  ]
}
```

### Widget Types and Encodings

**Counter (KPI card):**
```json
{
  "widget": {
    "name": "w_id",
    "queries": [{"name": "main_query", "query": {
      "datasetName": "ds_id",
      "fields": [{"name": "field_alias", "expression": "SUM(`column`)"}],
      "disaggregated": false
    }}],
    "spec": {
      "version": 3,
      "widgetType": "counter",
      "encodings": {
        "value": {"fieldName": "field_alias", "displayName": "Display Name"}
      }
    }
  },
  "position": {"x": 0, "y": 0, "width": 1, "height": 2}
}
```

**Bar chart:**
```json
{
  "spec": {
    "version": 3,
    "widgetType": "bar",
    "encodings": {
      "x": {"fieldName": "category_col", "scale": {"type": "categorical"}, "displayName": "Label"},
      "y": {"fieldName": "value_col", "scale": {"type": "quantitative"}, "displayName": "Value"}
    }
  }
}
```

**Line chart:**
```json
{
  "spec": {
    "version": 3,
    "widgetType": "line",
    "encodings": {
      "x": {"fieldName": "date_col", "scale": {"type": "temporal"}, "displayName": "Date"},
      "y": {"fieldName": "value_col", "scale": {"type": "quantitative"}, "displayName": "Value"}
    }
  }
}
```

**Table:**
```json
{
  "spec": {
    "version": 3,
    "widgetType": "table",
    "encodings": {
      "columns": [
        {"fieldName": "col1", "displayName": "Column 1"},
        {"fieldName": "col2", "displayName": "Column 2"}
      ]
    }
  }
}
```

**Filter (multi-select):**
```json
{
  "spec": {
    "version": 2,
    "widgetType": "filter-multi-select",
    "encodings": {
      "fields": [{"fieldName": "col", "displayName": "Label", "queryName": "filter_query"}]
    }
  }
}
```

**Text/title:**
```json
{
  "widget": {
    "name": "w_id",
    "textbox_spec": "# Markdown Title\nSubtitle text"
  }
}
```

### Position Grid
- Grid is 6 columns wide
- `x`: 0-5 (column), `y`: row position, `width`: 1-6, `height`: rows to span
- Typical KPI card: `width: 1, height: 2`
- Typical chart: `width: 3, height: 5`
- Full-width element: `width: 6`

### Critical: How Datasets Work with Metric Views

**The dashboard engine wraps your dataset query as a subquery.** Widget field expressions run on the dataset's result columns, NOT on the original table. Therefore:

**WRONG — dimensions only in dataset, MEASURE() in widget:**
```
Dataset: SELECT order_date, order_status FROM sales_metrics
Widget:  MEASURE(`Total Revenue`)  →  ERROR: column not found
```

**CORRECT — MEASURE() in dataset, SUM() in widget:**
```
Dataset: SELECT order_date, MEASURE(`Total Revenue`) as total_revenue
         FROM sales_metrics GROUP BY order_date
Widget:  SUM(`total_revenue`)  →  Works!
```

For **non-additive measures** (ratios like Profit Margin), bring through component parts and compute in the widget:
```
Dataset: ... MEASURE(`Total Profit`) as profit, MEASURE(`Total Revenue`) as revenue ...
Widget:  SUM(`profit`) / SUM(`revenue`)  →  Correct ratio
```

Do NOT pre-compute the ratio in the dataset and then SUM it in the widget — that re-sums ratios, which is mathematically wrong.

### Full Create + Publish Python Example

```python
import json, urllib.request, ssl, certifi

host = "<workspace-host>"
warehouse_id = "<warehouse-id>"
token = "<token>"
ctx = ssl.create_default_context(cafile=certifi.where())

dashboard = {
    "datasets": [{
        "name": "ds_sales",
        "displayName": "Sales",
        "queryLines": [
            "SELECT order_date, product_category,\n",
            "  MEASURE(`Total Revenue`) as revenue,\n",
            "  MEASURE(`Total Profit`) as profit\n",
            "FROM <catalog>.<schema>.sales_metrics_full\n",
            "GROUP BY order_date, product_category"
        ]
    }],
    "pages": [{
        "name": "pg1",
        "displayName": "Overview",
        "layout": [
            # ... widget definitions ...
        ],
        "pageType": "PAGE_TYPE_CANVAS"
    }]
}

# Create
payload = json.dumps({
    "display_name": "Dashboard Name",
    "warehouse_id": warehouse_id,
    "serialized_dashboard": json.dumps(dashboard)
}).encode()

req = urllib.request.Request(
    f"https://{host}/api/2.0/lakeview/dashboards",
    data=payload,
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
)
resp = urllib.request.urlopen(req, context=ctx)
result = json.loads(resp.read())
dashboard_id = result["dashboard_id"]

# Publish
pub_payload = json.dumps({
    "warehouse_id": warehouse_id,
    "embed_credentials": True
}).encode()
req2 = urllib.request.Request(
    f"https://{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    data=pub_payload,
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
)
urllib.request.urlopen(req2, context=ctx)
print(f"https://{host}/dashboardsv3/{dashboard_id}")
```

### Update an Existing Dashboard

```python
# 1. GET current dashboard to obtain etag
req = urllib.request.Request(
    f"https://{host}/api/2.0/lakeview/dashboards/{dashboard_id}",
    headers={"Authorization": f"Bearer {token}"}
)
etag = json.loads(urllib.request.urlopen(req, ctx).read())["etag"]

# 2. PATCH with new serialized_dashboard + etag
payload = json.dumps({
    "display_name": "Updated Name",
    "warehouse_id": warehouse_id,
    "serialized_dashboard": json.dumps(new_dashboard),
    "etag": etag
}).encode()
req = urllib.request.Request(
    f"https://{host}/api/2.0/lakeview/dashboards/{dashboard_id}",
    data=payload,
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    method="PATCH"
)
urllib.request.urlopen(req, ctx)

# 3. Re-publish
```

### CLI Alternative

```bash
# List dashboards
databricks lakeview list --profile <profile> -o json

# Get dashboard (includes serialized_dashboard)
databricks lakeview get <dashboard_id> --profile <profile> -o json

# Create dashboard
databricks lakeview create \
  --display-name "My Dashboard" \
  --warehouse-id "<id>" \
  --serialized-dashboard '{"pages":[...],"datasets":[...]}' \
  --profile <profile>

# Publish
databricks lakeview publish <dashboard_id> \
  --warehouse-id "<id>" \
  --profile <profile>
```

---

## 6. Gotchas & Lessons Learned

### Metric View Creation
| Issue | Symptom | Fix |
|-------|---------|-----|
| Single `$` delimiter via API | `PARSE_SYNTAX_ERROR at $` | Use `$$` or `$yaml$` as delimiter |
| `CREATE METRIC VIEW` syntax | `PARSE_SYNTAX_ERROR` | Correct: `CREATE VIEW ... WITH METRICS LANGUAGE YAML AS` |
| Source table has no dim attributes | Only FK IDs available as dimensions | Create denormalized source view first |
| Requires Runtime 17.3+ | Feature not recognized | Ensure warehouse is on supported runtime |

### Querying
| Issue | Symptom | Fix |
|-------|---------|-----|
| `SELECT *` from metric view | `METRIC_VIEW_MISSING_MEASURE_FUNCTION` | Select dimensions explicitly; use `MEASURE()` for measures |
| `SUM(measure_col)` | `METRIC_VIEW_MISSING_MEASURE_FUNCTION` | Use `MEASURE(\`measure_col\`)` instead |
| JOIN with metric view | `METRIC_VIEW_JOIN_NOT_SUPPORTED` | Pre-join in the source view; metric view sources one table/view |
| Non-additive grand totals | Wrong values when re-aggregated | Bring component parts, compute ratio at final aggregation |

### PBI with BI Compatibility Mode
| Issue | Symptom | Fix |
|-------|---------|-----|
| Import mode selected | Measures don't work | Must use DirectQuery |
| Changed aggregation to AVG | Shows `1.0` | Always leave as SUM for measures |
| Measure dragged to axis/slicer | Query error | Measures go in Values wells only |
| Grand total wrong for ratio | Sum of ratios ≠ ratio of sums | Known limitation; document for users |
| PBI version too old | BI Compat option missing | Requires v2.151.1052.0+ (Feb 2026) |

### AI/BI Dashboard
| Issue | Symptom | Fix |
|-------|---------|-----|
| MEASURE() in widget expression | `UNRESOLVED_COLUMN` | Put MEASURE() in **dataset** query; widget uses SUM() on result columns |
| Pre-computed ratio in dataset | Wrong values when widget re-aggregates | Bring numerator + denominator separately; compute ratio in widget |
| Dataset too narrow | Widget can't group by needed dimension | Include ALL dimensions in dataset's GROUP BY and SELECT |

---

## 7. End-to-End Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Unity Catalog                         │
│                                                       │
│  dim_products ──┐                                     │
│  dim_reps ──────┤                                     │
│  dim_customers ─┼──→ vw_sales_enriched ──→ sales_metrics_full
│  dim_dates ─────┤     (denormalized)       (metric view)
│  fact_sales ────┘                            │        │
│                                              │        │
│  fact_support ─────────────────→ support_metrics      │
│                                  (metric view)        │
└──────────────────────────────────┬────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
              ┌─────▼──────┐ ┌────▼─────┐ ┌──────▼──────┐
              │ AI/BI Dash │ │  Genie   │ │  Power BI   │
              │ MEASURE()  │ │ natural  │ │ BI Compat   │
              │ in dataset │ │ language │ │ or wrapper  │
              │ SUM() in   │ │ queries  │ │ views + DQ  │
              │ widgets    │ │          │ │             │
              └────────────┘ └──────────┘ └─────────────┘
```

---

## 8. Reference Links

- [Metric Views Documentation](https://docs.databricks.com/aws/en/business-semantics/metric-views/)
- [Query Metric Views in Power BI](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi-metric-views)
- [Lakeview Dashboard API](https://docs.databricks.com/api/workspace/lakeview/create)
- [Dashboard CRUD Tutorial](https://docs.databricks.com/aws/en/dashboards/tutorials/dashboard-crud-api)
- [Tabular Editor Semantic Bridge](https://tabulareditor.com/blog/bridge-analytics-in-databricks-and-power-bi-via-tabular-editor)
- [Advancing Analytics: Metric Views + PBI Challenges](https://www.advancinganalytics.co.uk/blog/the-metrics-are-in-why-your-databricks-metrics-views-and-power-bi-arent-seeing-eye-to-eye)
