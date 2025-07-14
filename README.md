# acmi_edw

A **LakehousePlumber** Delta Live Tables (DLT) pipeline project for processing TPC-H benchmark data using a medallion architecture.

## Overview

This project implements a complete data lakehouse solution for the TPC-H benchmark dataset using **LakehousePlumber (lhp)** to generate Delta Live Tables pipelines. The project follows a medallion architecture pattern with **raw ingestion**, **bronze**, **silver**, and **gold** layers.

The solution processes 8 core TPC-H tables:
- `customer` - Customer dimension data
- `lineitem` - Order line items fact data  
- `nation` - Nation dimension data
- `orders` - Orders fact data
- `part` - Part dimension data
- `partsupp` - Part supplier dimension data
- `region` - Region dimension data
- `supplier` - Supplier dimension data

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
Project Created: July 11, 2025

---

For more information about LakehousePlumber, visit: https://github.com/yourusername/lakehouse-plumber
