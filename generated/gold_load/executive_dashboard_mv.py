# Generated by LakehousePlumber
# Pipeline: gold_load
# FlowGroup: executive_dashboard_mv
# Generated: 2025-07-16T10:03:28.453892

from pyspark.sql import DataFrame
import dlt

# Pipeline Configuration
PIPELINE_ID = "gold_load"
FLOWGROUP_ID = "executive_dashboard_mv"

# ============================================================================
# SOURCE VIEWS
# ============================================================================

@dlt.view()
def v_monthly_sales_sql():
    """SQL source: monthly_sales_sql"""
    df = spark.sql("""SELECT
  year,
  month,
  monthly_revenue,
  monthly_orders,
  monthly_customers,
  avg_monthly_order_value,
  LAG(monthly_revenue, 1) OVER (ORDER BY year, month) as prev_month_revenue,
  LAG(monthly_orders, 1) OVER (ORDER BY year, month) as prev_month_orders
FROM acmi_edw_dev.edw_gold.sales_summary_monthly_mv
""")

    return df

@dlt.view()
def v_regional_performance_sql():
    """SQL source: regional_performance_sql"""
    df = spark.sql("""SELECT
  month,
  COUNT(DISTINCT region_name) as active_regions,
  SUM(region_revenue) as total_regional_revenue,
  SUM(region_orders) as total_regional_orders,
  SUM(region_customers) as total_regional_customers,
  AVG(region_revenue) as avg_regional_revenue
FROM acmi_edw_dev.edw_gold.revenue_by_region_mv
GROUP BY month
""")

    return df

@dlt.view()
def v_customer_metrics_sql():
    """SQL source: customer_metrics_sql"""
    df = spark.sql("""SELECT
  COUNT(*) as total_customers,
  AVG(lifetime_value) as avg_customer_lifetime_value,
  AVG(total_orders) as avg_orders_per_customer,
  AVG(customer_tenure_days) as avg_customer_tenure_days,
  SUM(CASE WHEN total_orders = 1 THEN 1 ELSE 0 END) as one_time_customers,
  SUM(CASE WHEN total_orders > 5 THEN 1 ELSE 0 END) as loyal_customers
FROM acmi_edw_dev.edw_gold.customer_lifetime_value_mv
""")

    return df

@dlt.view()
def v_top_products_sql():
    """SQL source: top_products_sql"""
    df = spark.sql("""SELECT
  month,
  COUNT(DISTINCT part_id) as active_products,
  SUM(total_revenue) as product_revenue,
  AVG(avg_unit_price) as avg_product_price,
  SUM(total_quantity_sold) as total_units_sold
FROM acmi_edw_dev.edw_gold.product_performance_mv
GROUP BY month
""")

    return df

@dlt.view()
def v_executive_dashboard_mv_sql():
    """SQL source: executive_dashboard_mv_sql"""
    df = spark.sql("""SELECT
  ms.year,
  ms.month,
  CONCAT(ms.year, '-', LPAD(ms.month, 2, '0')) as year_month,

  -- Revenue Metrics
  ms.monthly_revenue,
  ms.prev_month_revenue,
  CASE
    WHEN ms.prev_month_revenue > 0 THEN
      ROUND((ms.monthly_revenue - ms.prev_month_revenue) / ms.prev_month_revenue * 100, 2)
    ELSE NULL
  END as revenue_growth_pct,

  -- Order Metrics
  ms.monthly_orders,
  ms.prev_month_orders,
  CASE
    WHEN ms.prev_month_orders > 0 THEN
      ROUND((ms.monthly_orders - ms.prev_month_orders) / ms.prev_month_orders * 100, 2)
    ELSE NULL
  END as orders_growth_pct,

  -- Customer Metrics
  ms.monthly_customers,
  ms.avg_monthly_order_value,
  cm.avg_customer_lifetime_value,
  cm.avg_orders_per_customer,
  cm.avg_customer_tenure_days,
  ROUND(cm.one_time_customers / cm.total_customers * 100, 2) as one_time_customer_pct,
  ROUND(cm.loyal_customers / cm.total_customers * 100, 2) as loyal_customer_pct,

  -- Regional Performance
  rp.active_regions,
  rp.total_regional_revenue,
  rp.avg_regional_revenue,

  -- Product Performance
  tp.active_products,
  tp.product_revenue,
  tp.avg_product_price,
  tp.total_units_sold,

  -- KPIs
  ROUND(ms.monthly_revenue / ms.monthly_customers, 2) as revenue_per_customer,
  ROUND(ms.monthly_revenue / ms.monthly_orders, 2) as revenue_per_order,
  ROUND(tp.total_units_sold / ms.monthly_orders, 2) as units_per_order,

  -- Current timestamp for freshness
  CURRENT_TIMESTAMP() as dashboard_updated_at

FROM v_monthly_sales_sql ms
LEFT JOIN v_regional_performance_sql rp ON ms.year = YEAR(rp.month) AND ms.month = MONTH(rp.month)
LEFT JOIN v_top_products_sql tp ON ms.year = YEAR(tp.month) AND ms.month = MONTH(tp.month)
CROSS JOIN v_customer_metrics_sql cm
WHERE ms.year >= YEAR(add_months(CURRENT_DATE(), -24))
ORDER BY ms.year, ms.month
""")

    return df


# ============================================================================
# TARGET TABLES
# ============================================================================

@dlt.table(
    name="acmi_edw_dev.edw_gold.executive_dashboard_mv",
    comment="Materialized view: executive_dashboard_mv",
    table_properties={})
def executive_dashboard_mv():
    """Write to acmi_edw_dev.edw_gold.executive_dashboard_mv from multiple sources"""
    # Materialized views use batch processing
    df = spark.read.table("v_executive_dashboard_mv_sql")

    return df
