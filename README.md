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
