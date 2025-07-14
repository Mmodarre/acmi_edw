# ACMI EDW - Lakehouse Plumber Sample Project

A **LakehousePlumber** Delta Live Tables (DLT) pipeline project for processing TPC-H benchmark data using a medallion architecture.

## Overview

This project implements a complete data lakehouse solution for the TPC-H benchmark dataset using [**LakehousePlumber (lhp)**](https://github.com/Mmodarre/Lakehouse_Plumber) to generate Delta Live Tables pipelines. The project follows a medallion architecture pattern with **raw ingestion**, **bronze**, **silver**, and **gold** layers.

The solution processes 8 core TPC-H tables:
- `customer` - Customer dimension data
- `lineitem` - Order line items fact data  
- `nation` - Nation dimension data
- `orders` - Orders fact data
- `part` - Part dimension data
- `partsupp` - Part supplier dimension data
- `region` - Region dimension data
- `supplier` - Supplier dimension data

## Todo

- [ ] Add Applend flow with multiple sources writing to the same table (for now using append_flow for single source to each table)
- [ ] Add once=True for nation and region tables (for now using Auto_cdc_flow)
- [ ] Add presets demonstration (currently presets are not used)
- [ ] Add SQL file for SQL queries (for now using inline SQL queries in the pipeline)
- [ ] Add Python transformation for demonstration.

## Architecture

### Medallion Architecture Layers

1. **Raw Ingestion Layer** (`01_raw_ingestion/`)
   - Ingests data from multiple formats (CSV, JSON, Parquet)
   - Supports streaming ingestion with schema hints
   - Adds operational metadata (file paths, timestamps, record hashes)

2. **Bronze Layer** (`02_bronze/`)
   - Cleanses and standardizes raw data
   - Applies data quality expectations
   - Implements SCD Type 2 for dimension tables

3. **Silver Layer** (`03_silver/`)
   - Creates business-ready, conformed data
   - Implements Change Data Capture (CDC) flows
   - Maintains historical data with SCD Type 2

4. **Gold Layer** (`04_gold/`)
   - Business aggregations and reporting tables
   - Optimized for analytical queries

### Data Quality

The project implements comprehensive data quality checks:
- **Fail Actions**: Critical validations that stop pipeline execution
- **Warn Actions**: Non-critical validations that log warnings
- **Expectations**: Defined in `expectations/` directory as JSON files

Example quality checks for customer data:
- Valid customer keys (NOT NULL, > 0)
- Valid customer names (NOT NULL, length > 0)
- Valid nation keys 
- Phone format validation
- Account balance range checks
- Market segment validation

## Project Structure

```
acmi_edw/
├── pipelines/              # Pipeline configurations organized by layer
│   ├── 01_raw_ingestion/   # Raw data ingestion configs
│   │   ├── csv_ingestions/     # CSV file ingestion
│   │   ├── json_ingestion/     # JSON file ingestion  
│   │   └── parquet_ingestions/ # Parquet file ingestion
│   ├── 02_bronze/          # Bronze layer transformations
│   │   ├── dim/               # Dimension tables
│   │   └── fct/               # Fact tables
│   ├── 03_silver/          # Silver layer transformations
│   │   ├── dim/               # Dimension tables
│   │   └── fct/               # Fact tables
│   └── 04_gold/            # Gold layer aggregations
├── generated/              # Generated DLT pipeline code
├── schemas/                # Table schema definitions (YAML)
├── expectations/           # Data quality expectations (JSON)
├── templates/              # Reusable pipeline templates
├── substitutions/          # Environment-specific configurations
├── presets/                # Reusable configuration presets
└── lhp.yaml               # Main project configuration
```

## Pipeline Organization

This project demonstrates a **layer-based pipeline organization** strategy, where each medallion layer constitutes a separate DLT pipeline:

### Current Organization Structure

| Layer | Pipeline Name | Description |
|-------|---------------|-------------|
| Raw Ingestion | `raw_ingestions` | All table ingestion flows |
| Bronze | `bronze_load` | All bronze transformations |
| Silver | `silver_load` | All silver CDC flows |
| Gold | `gold_aggregations` | All gold aggregations |

### Pipeline Organization Strategies

LakehousePlumber supports multiple pipeline organization approaches:

#### 1. **Layer-Based Organization** (Current Approach)
```yaml
# All raw ingestion flows
pipeline: raw_ingestions
flowgroup: customer_ingestion

# All bronze transformations  
pipeline: bronze_load
flowgroup: customer_bronze_dim
```

**Benefits:**
- Clear separation of concerns by data layer
- Easier to manage layer-specific configurations
- Simplified deployment and monitoring per layer
- Better resource management per layer

**Considerations:**
- Single pipeline failure affects all tables in that layer
- Potential resource contention for large datasets

#### 2. **Table-Based Organization**
```yaml
# Customer-specific pipeline
pipeline: customer_pipeline
flowgroup: customer_ingestion

# Orders-specific pipeline
pipeline: orders_pipeline  
flowgroup: orders_ingestion
```

**Benefits:**
- Independent processing and failure isolation per table
- Table-specific resource allocation
- Parallel processing of different tables
- Easier troubleshooting and debugging

**Considerations:**
- More pipelines to manage and monitor
- Potential resource overhead with many small pipelines

#### 3. **Domain-Based Organization**
```yaml
# Customer domain (customer, nation, region)
pipeline: customer_domain
flowgroup: customer_processing

# Sales domain (orders, lineitem)
pipeline: sales_domain
flowgroup: orders_processing

# Product domain (part, partsupp, supplier)
pipeline: product_domain
flowgroup: part_processing
```

**Benefits:**
- Logical grouping of related business entities
- Domain-specific resource allocation
- Better alignment with business ownership
- Balanced pipeline sizes

#### 4. **Hybrid Organization**
```yaml
# Critical tables get their own pipelines
pipeline: lineitem_pipeline  # Largest fact table
pipeline: orders_pipeline    # High-volume fact table

# Smaller dimensions share pipelines
pipeline: dimensions_pipeline # customer, nation, region, etc.
```

### Choosing the Right Organization

Consider these factors when selecting a pipeline organization strategy:

| Factor | Layer-Based | Table-Based | Domain-Based | Hybrid |
|--------|-------------|-------------|--------------|--------|
| **Simplicity** | ✅ High | ❌ Low | ⚠️ Medium | ⚠️ Medium |
| **Failure Isolation** | ❌ Low | ✅ High | ⚠️ Medium | ✅ High |
| **Resource Optimization** | ⚠️ Medium | ✅ High | ✅ High | ✅ High |
| **Monitoring Complexity** | ✅ Low | ❌ High | ⚠️ Medium | ⚠️ Medium |
| **Development Complexity** | ✅ Low | ⚠️ Medium | ⚠️ Medium | ❌ High |

### Migration Between Organizations

To migrate from the current layer-based to a different organization:

1. **Update pipeline names** in YAML configurations
2. **Reorganize directory structure** (optional)
3. **Update dependencies** between pipelines
4. **Regenerate DLT code** with new organization
5. **Update deployment scripts** and monitoring

Example migration to table-based organization:
```bash
# Update pipeline configurations
sed -i 's/pipeline: raw_ingestions/pipeline: customer_pipeline/' pipelines/*/customer_*.yaml

# Regenerate code
lhp generate --env dev

# Deploy new pipeline structure
```

### Best Practices for Pipeline Organization

1. **Start Simple**: Begin with layer-based organization for new projects
2. **Monitor Performance**: Watch for bottlenecks and resource contention
3. **Consider Data Volume**: Large fact tables may benefit from dedicated pipelines
4. **Plan for Growth**: Design organization that scales with data volume
5. **Document Dependencies**: Clear documentation of inter-pipeline dependencies
6. **Test Thoroughly**: Validate all data flows after organization changes

## Gold Layer Table Structures

Based on the silver layer tables, here are suggested gold layer table structures organized by business domains:

### 📊 **Sales & Revenue Analytics**

#### `sales_summary_daily`
```sql
-- Daily sales performance metrics
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_price) as total_revenue,
    AVG(total_price) as avg_order_value,
    SUM(CASE WHEN order_status = 'F' THEN 1 ELSE 0 END) as fulfilled_orders,
    SUM(CASE WHEN order_status = 'O' THEN 1 ELSE 0 END) as open_orders
FROM orders_silver_fct o
JOIN customer_silver_dim c ON o.customer_id = c.customer_id
GROUP BY order_date
```

#### `sales_summary_monthly`
```sql
-- Monthly sales aggregations with year-over-year growth
SELECT 
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    SUM(total_price) as monthly_revenue,
    COUNT(DISTINCT order_id) as monthly_orders,
    COUNT(DISTINCT customer_id) as monthly_customers,
    AVG(total_price) as avg_monthly_order_value
FROM orders_silver_fct
GROUP BY YEAR(order_date), MONTH(order_date)
```

#### `revenue_by_region`
```sql
-- Revenue analysis by geographic region
SELECT 
    r.region_name,
    n.nation_name,
    DATE_TRUNC('month', o.order_date) as month,
    SUM(o.total_price) as region_revenue,
    COUNT(DISTINCT o.order_id) as region_orders,
    COUNT(DISTINCT c.customer_id) as region_customers
FROM orders_silver_fct o
JOIN customer_silver_dim c ON o.customer_id = c.customer_id
JOIN nation_silver_dim n ON c.nation_key = n.nation_key
JOIN region_silver_dim r ON n.region_key = r.region_key
GROUP BY r.region_name, n.nation_name, DATE_TRUNC('month', o.order_date)
```

### 🛒 **Customer Analytics**

#### `customer_lifetime_value`
```sql
-- Customer lifetime value and behavior metrics
SELECT 
    c.customer_id,
    c.name as customer_name,
    c.market_segment,
    n.nation_name,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_price) as lifetime_value,
    AVG(o.total_price) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) as customer_tenure_days
FROM customer_silver_dim c
JOIN orders_silver_fct o ON c.customer_id = o.customer_id
JOIN nation_silver_dim n ON c.nation_key = n.nation_key
GROUP BY c.customer_id, c.name, c.market_segment, n.nation_name
```

#### `customer_segmentation`
```sql
-- RFM (Recency, Frequency, Monetary) customer segmentation
SELECT 
    customer_id,
    customer_name,
    market_segment,
    recency_score,
    frequency_score,
    monetary_score,
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 3 AND frequency_score >= 2 THEN 'Potential Loyalists'
        WHEN recency_score >= 4 AND frequency_score <= 1 THEN 'New Customers'
        WHEN recency_score <= 2 AND frequency_score >= 2 THEN 'At Risk'
        WHEN recency_score <= 1 AND frequency_score <= 1 THEN 'Lost Customers'
        ELSE 'Others'
    END as customer_segment
FROM customer_lifetime_value
```

### 📦 **Product & Inventory Analytics**

#### `product_performance`
```sql
-- Product sales performance and popularity metrics
SELECT 
    p.part_key,
    p.name as product_name,
    p.manufacturer,
    p.brand,
    p.type,
    p.size,
    COUNT(DISTINCT l.order_key) as orders_count,
    SUM(l.quantity) as total_quantity_sold,
    SUM(l.extended_price) as total_revenue,
    AVG(l.extended_price / l.quantity) as avg_unit_price,
    SUM(l.extended_price * l.discount) as total_discount_given,
    AVG(l.discount) as avg_discount_rate
FROM part_silver_dim p
JOIN lineitem_silver_fct l ON p.part_key = l.part_key
GROUP BY p.part_key, p.name, p.manufacturer, p.brand, p.type, p.size
```

#### `supplier_performance`
```sql
-- Supplier performance metrics
SELECT 
    s.supplier_key,
    s.name as supplier_name,
    s.nation_name,
    COUNT(DISTINCT l.order_key) as orders_fulfilled,
    SUM(l.quantity) as total_quantity_supplied,
    SUM(l.extended_price) as total_revenue_generated,
    AVG(DATEDIFF(l.receipt_date, l.commit_date)) as avg_delivery_days,
    SUM(CASE WHEN l.receipt_date > l.commit_date THEN 1 ELSE 0 END) as late_deliveries,
    COUNT(*) as total_line_items
FROM supplier_silver_dim s
JOIN lineitem_silver_fct l ON s.supplier_key = l.supplier_key
GROUP BY s.supplier_key, s.name, s.nation_name
```

### 📈 **Time Series Analytics**

#### `daily_business_metrics`
```sql
-- Daily business KPIs dashboard
SELECT 
    DATE(order_date) as business_date,
    COUNT(DISTINCT order_id) as daily_orders,
    SUM(total_price) as daily_revenue,
    COUNT(DISTINCT customer_id) as daily_unique_customers,
    AVG(total_price) as daily_avg_order_value,
    SUM(total_price) / COUNT(DISTINCT customer_id) as revenue_per_customer,
    LAG(SUM(total_price), 1) OVER (ORDER BY DATE(order_date)) as prev_day_revenue,
    (SUM(total_price) - LAG(SUM(total_price), 1) OVER (ORDER BY DATE(order_date))) / 
    LAG(SUM(total_price), 1) OVER (ORDER BY DATE(order_date)) * 100 as revenue_growth_pct
FROM orders_silver_fct
GROUP BY DATE(order_date)
```

#### `seasonal_trends`
```sql
-- Seasonal and cyclical trend analysis
SELECT 
    YEAR(order_date) as year,
    QUARTER(order_date) as quarter,
    MONTH(order_date) as month,
    DAYOFWEEK(order_date) as day_of_week,
    SUM(total_price) as period_revenue,
    COUNT(DISTINCT order_id) as period_orders,
    AVG(total_price) as avg_order_value,
    SUM(total_price) / SUM(SUM(total_price)) OVER (PARTITION BY YEAR(order_date)) * 100 as revenue_contribution_pct
FROM orders_silver_fct
GROUP BY YEAR(order_date), QUARTER(order_date), MONTH(order_date), DAYOFWEEK(order_date)
```

### 💰 **Financial Analytics**

#### `profitability_analysis`
```sql
-- Profit margins and cost analysis
SELECT 
    p.manufacturer,
    p.brand,
    p.type,
    SUM(l.extended_price) as gross_revenue,
    SUM(l.extended_price * l.discount) as total_discounts,
    SUM(l.extended_price * (1 - l.discount)) as net_revenue,
    SUM(l.extended_price * l.tax) as total_taxes,
    SUM(ps.supply_cost * l.quantity) as total_supply_cost,
    SUM(l.extended_price * (1 - l.discount) - ps.supply_cost * l.quantity) as gross_profit,
    AVG((l.extended_price * (1 - l.discount) - ps.supply_cost * l.quantity) / l.extended_price) as profit_margin_pct
FROM lineitem_silver_fct l
JOIN part_silver_dim p ON l.part_key = p.part_key
JOIN partsupp_silver_dim ps ON l.part_key = ps.part_key AND l.supplier_key = ps.supplier_key
GROUP BY p.manufacturer, p.brand, p.type
```

#### `cash_flow_analysis`
```sql
-- Cash flow and payment analysis
SELECT 
    DATE_TRUNC('month', order_date) as month,
    SUM(CASE WHEN order_status = 'F' THEN total_price ELSE 0 END) as cash_received,
    SUM(CASE WHEN order_status = 'O' THEN total_price ELSE 0 END) as pending_receivables,
    SUM(CASE WHEN order_status = 'P' THEN total_price ELSE 0 END) as processing_orders,
    SUM(total_price) as total_bookings,
    COUNT(CASE WHEN order_status = 'F' THEN order_id END) as fulfilled_orders,
    COUNT(CASE WHEN order_status = 'O' THEN order_id END) as open_orders
FROM orders_silver_fct
GROUP BY DATE_TRUNC('month', order_date)
```

### 🔍 **Operational Analytics**

#### `order_fulfillment_metrics`
```sql
-- Order processing and fulfillment performance
SELECT 
    order_priority,
    shipping_priority,
    AVG(DATEDIFF(ship_date, order_date)) as avg_processing_days,
    AVG(DATEDIFF(receipt_date, ship_date)) as avg_shipping_days,
    AVG(DATEDIFF(receipt_date, order_date)) as avg_total_fulfillment_days,
    COUNT(*) as total_line_items,
    SUM(CASE WHEN return_flag = 'R' THEN 1 ELSE 0 END) as returned_items,
    SUM(CASE WHEN return_flag = 'R' THEN 1 ELSE 0 END) / COUNT(*) * 100 as return_rate_pct
FROM lineitem_silver_fct l
JOIN orders_silver_fct o ON l.order_key = o.order_key
GROUP BY order_priority, shipping_priority
```

#### `inventory_turnover`
```sql
-- Inventory turnover and demand forecasting
SELECT 
    p.part_key,
    p.name as product_name,
    p.manufacturer,
    ps.available_quantity,
    SUM(l.quantity) as total_sold,
    ps.available_quantity / NULLIF(SUM(l.quantity), 0) as inventory_turnover_ratio,
    COUNT(DISTINCT DATE_TRUNC('month', l.ship_date)) as months_active,
    SUM(l.quantity) / COUNT(DISTINCT DATE_TRUNC('month', l.ship_date)) as avg_monthly_demand
FROM part_silver_dim p
JOIN partsupp_silver_dim ps ON p.part_key = ps.part_key
JOIN lineitem_silver_fct l ON p.part_key = l.part_key
GROUP BY p.part_key, p.name, p.manufacturer, ps.available_quantity
```

### 🎯 **Executive Dashboard**

#### `executive_summary`
```sql
-- High-level executive KPIs
SELECT 
    'Total Revenue' as metric,
    SUM(total_price) as current_value,
    LAG(SUM(total_price), 1) OVER (ORDER BY DATE_TRUNC('month', order_date)) as previous_value
FROM orders_silver_fct
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY DATE_TRUNC('month', order_date)

UNION ALL

SELECT 
    'Total Orders' as metric,
    COUNT(DISTINCT order_id) as current_value,
    LAG(COUNT(DISTINCT order_id), 1) OVER (ORDER BY DATE_TRUNC('month', order_date)) as previous_value
FROM orders_silver_fct
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY DATE_TRUNC('month', order_date)
```

### 📋 **Implementation Guidelines**

1. **Incremental Updates**: Use Delta Lake's merge capabilities for efficient updates
2. **Partitioning**: Partition by date columns for time-based queries
3. **Indexing**: Create appropriate Z-order indexes for frequent query patterns
4. **Materialized Views**: Consider materialized views for frequently accessed aggregations
5. **Refresh Strategy**: Implement appropriate refresh schedules based on business needs
6. **Data Retention**: Define retention policies for historical data

### 💡 **Gold Layer Best Practices**

- **Business-Centric**: Design tables around business questions and KPIs
- **Denormalized**: Pre-join frequently used tables for query performance
- **Aggregated**: Pre-calculate common aggregations and metrics
- **Partitioned**: Use appropriate partitioning for time-series data
- **Documented**: Include clear descriptions and business definitions
- **Monitored**: Track usage patterns and query performance

## Data Sources and Formats

| Table     | Format  | Description |
|-----------|---------|-------------|
| customer  | CSV     | Customer dimension with demographic data |
| lineitem  | CSV     | Order line items (largest fact table) |
| nation    | CSV     | Nation dimension |
| orders    | CSV     | Orders fact table |
| part      | JSON    | Part dimension |
| partsupp  | JSON    | Part supplier relationships |
| region    | Parquet | Region dimension |
| supplier  | Parquet | Supplier dimension |

## Environments

The project supports multiple environments:

- **dev** - Development environment (`acmi_edw_dev` catalog)
- **tst** - Test environment  
- **prod** - Production environment

Environment-specific configurations are defined in `substitutions/` directory.

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- LakehousePlumber CLI installed
- Access to TPC-H dataset files
- Appropriate secrets configured in Databricks

### Setup

1. **Configure your environment**:
   ```bash
   # Edit substitutions/dev.yaml for your environment
   # Update catalog, schema, and volume paths
   ```

2. **Validate your configuration**:
   ```bash
   lhp validate --env dev
   ```

3. **Generate DLT pipeline code**:
   ```bash
   lhp generate --env dev
   ```

4. **Deploy to Databricks**:
   - Upload generated Python files to Databricks
   - Create DLT pipelines using the generated code
   - Configure compute and storage settings

### Running Pipelines

1. **Raw Ingestion**: Start with `01_raw_ingestion` pipelines
2. **Bronze Layer**: Run `02_bronze` transformations
3. **Silver Layer**: Execute `03_silver` CDC flows
4. **Gold Layer**: Create business aggregations

## Key Features

### Templates
- **CSV Ingestion Template**: Standardized CSV processing with schema hints
- **JSON Ingestion Template**: JSON file processing with schema evolution
- **Parquet Ingestion Template**: Efficient parquet file ingestion

### Operational Metadata
All tables include operational metadata:
- `_source_file_path`: Origin file path
- `_source_file_size`: File size in bytes
- `_source_file_modification_time`: File modification timestamp
- `_record_hash`: Record hash for change detection
- `_processing_timestamp`: Pipeline processing time

### Data Quality
- JSON-based expectation definitions
- Configurable failure actions (fail vs warn)
- Comprehensive validation rules per table

### Change Data Capture
- Automatic CDC flows for dimension tables
- SCD Type 2 implementation
- Delta table change data feed enabled

## Commands

- `lhp validate --env <environment>` - Validate pipeline configurations
- `lhp generate --env <environment>` - Generate DLT pipeline code
- `lhp list-presets` - List available presets
- `lhp list-templates` - List available templates
- `lhp show <flowgroup> --env <environment>` - Show resolved configuration

## Configuration

### Main Configuration (`lhp.yaml`)
- Project metadata and settings
- Operational metadata column definitions
- Global configuration standards

### Environment Configuration (`substitutions/`)
- Environment-specific variables
- Catalog and schema mappings
- Secret scope configurations

### Schema Definitions (`schemas/`)
- YAML-based table schema definitions
- Column types, nullability, and comments
- Used for schema hints in autoloader

## Author

**Mehdi Modarressi**  
Project Created: July 2025

---

For more information about LakehousePlumber, visit the [LakehousePlumber GitHub repository](https://github.com/Mmodarre/Lakehouse_Plumber).
