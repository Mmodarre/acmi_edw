# Generated by LakehousePlumber
# Pipeline: raw_ingestions
# FlowGroup: orders_ingestion
# Generated: 2025-07-12T11:22:28.111197

from pyspark.sql import functions as F
from pyspark.sql.functions import hash
import dlt

# Pipeline Configuration
PIPELINE_ID = "orders_ingestion"
PIPELINE_GROUP = "raw_ingestions"

# ============================================================================
# SOURCE VIEWS
# ============================================================================

# Schema hints for orders table
orders_schema_hints = """
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus STRING,
    o_totalprice DECIMAL(18,2),
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING
""".strip().replace(
    "\n", " "
)


@dlt.view()
def v_orders_raw():
    """Load orders CSV files from landing volume"""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", "|")
        .option("cloudFiles.maxFilesPerTrigger", 10)
        .option("cloudFiles.inferColumnTypes", False)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaHints", orders_schema_hints)
        .load("/Volumes/acmi_edw_dev/edw_raw/landing_volume/orders/*.csv")
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
    name="acmi_edw_dev.edw_raw.orders",
    comment="Streaming table: orders",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
    },
)


# Define append flow(s)
@dlt.append_flow(
    target="acmi_edw_dev.edw_raw.orders",
    name="f_orders_bronze",
    comment="Append flow to acmi_edw_dev.edw_raw.orders",
)
def f_orders_bronze():
    """Append flow to acmi_edw_dev.edw_raw.orders"""
    # Streaming flow
    df = spark.readStream.table("v_orders_raw")

    return df
