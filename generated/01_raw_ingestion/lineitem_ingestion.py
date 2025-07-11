# Generated by LakehousePlumber
# Pipeline: raw_ingestions
# FlowGroup: lineitem_ingestion
# Generated: 2025-07-12T11:22:28.047999

from pyspark.sql import functions as F
from pyspark.sql.functions import hash
import dlt

# Pipeline Configuration
PIPELINE_ID = "lineitem_ingestion"
PIPELINE_GROUP = "raw_ingestions"

# ============================================================================
# SOURCE VIEWS
# ============================================================================

# Schema hints for lineitem table
lineitem_schema_hints = """
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber INT,
    l_quantity DECIMAL(18,2),
    l_extendedprice DECIMAL(18,2),
    l_discount DECIMAL(18,2),
    l_tax DECIMAL(18,2),
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
""".strip().replace(
    "\n", " "
)


@dlt.view()
def v_lineitem_raw():
    """Load lineitem CSV files from landing volume"""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("delimiter", "|")
        .option("cloudFiles.maxFilesPerTrigger", 10)
        .option("cloudFiles.inferColumnTypes", False)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaHints", lineitem_schema_hints)
        .load("/Volumes/acmi_edw_dev/edw_raw/landing_volume/lineitem/*.csv")
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
    name="acmi_edw_dev.edw_raw.lineitem",
    comment="Streaming table: lineitem",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
    },
)


# Define append flow(s)
@dlt.append_flow(
    target="acmi_edw_dev.edw_raw.lineitem",
    name="f_lineitem_bronze",
    comment="Append flow to acmi_edw_dev.edw_raw.lineitem",
)
def f_lineitem_bronze():
    """Append flow to acmi_edw_dev.edw_raw.lineitem"""
    # Streaming flow
    df = spark.readStream.table("v_lineitem_raw")

    return df
