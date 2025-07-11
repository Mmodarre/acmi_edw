# Generated by LakehousePlumber
# Pipeline: raw_ingestions
# FlowGroup: customer_ingestion
# Generated: 2025-07-12T11:22:28.079507

from pyspark.sql import functions as F
from pyspark.sql.functions import hash
import dlt

# Pipeline Configuration
PIPELINE_ID = "customer_ingestion"
PIPELINE_GROUP = "raw_ingestions"

# ============================================================================
# SOURCE VIEWS
# ============================================================================

# Schema hints for customer table
customer_schema_hints = """
    c_custkey BIGINT,
    c_name STRING,
    c_address STRING,
    c_nationkey BIGINT,
    c_phone STRING,
    c_acctbal DECIMAL(18,2),
    c_mktsegment STRING,
    c_comment STRING
""".strip().replace(
    "\n", " "
)


@dlt.view()
def v_customer_raw():
    """Load customer CSV files from landing volume"""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", "|")
        .option("cloudFiles.maxFilesPerTrigger", 10)
        .option("cloudFiles.inferColumnTypes", False)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaHints", customer_schema_hints)
        .load("/Volumes/acmi_edw_dev/edw_raw/landing_volume/customer/*.csv")
    )

    # Add operational metadata columns
    df = df.withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
    df = df.withColumn("_source_file_path", F.col("_metadata.file_path"))
    df = df.withColumn("_record_hash", F.xxhash64(*[F.col(c) for c in df.columns]))
    df = df.withColumn("_source_file_size", F.col("_metadata.file_size"))

    return df


# ============================================================================
# TARGET TABLES
# ============================================================================

# Create the streaming table
dlt.create_streaming_table(
    name="acmi_edw_dev.edw_raw.customer",
    comment="Streaming table: customer",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
    },
)


# Define append flow(s)
@dlt.append_flow(
    target="acmi_edw_dev.edw_raw.customer",
    name="f_customer_bronze",
    comment="Append flow to acmi_edw_dev.edw_raw.customer",
)
def f_customer_bronze():
    """Append flow to acmi_edw_dev.edw_raw.customer"""
    # Streaming flow
    df = spark.readStream.table("v_customer_raw")

    return df
