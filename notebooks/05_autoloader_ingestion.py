# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 05 · Auto Loader Ingestion
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Use Databricks Auto Loader (`cloudFiles`) to incrementally ingest new order files dropped into a Volume folder into `bronze_orders_stream`, instead of re-reading the whole directory on every run.

# COMMAND ----------

# DBTITLE 1,Resolve bundle sys.path
import sys, os

try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _bundle_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
    if _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)
except Exception:
    pass

# COMMAND ----------

# DBTITLE 1,Project configuration
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from notebooks.config import get_config

cfg = get_config()

CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]
RAW_VOLUME_PATH = cfg["raw_volume"]
CHECKPOINT_VOLUME_PATH = cfg["checkpoint_volume"]

INCOMING_ORDERS_DIR = cfg["incoming_orders_dir"]
AUTOLOADER_SCHEMA_TRACKING = f"{CHECKPOINT_VOLUME_PATH}/schema_tracking/bronze_orders_stream"
AUTOLOADER_CHECKPOINT = f"{CHECKPOINT_VOLUME_PATH}/bronze_orders_stream"
BRONZE_ORDERS_STREAM = f"{CATALOG}.{SCHEMA}.bronze_orders_stream"

orders_stream_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), False),
])

from notebooks.logging_helper import get_logger

logger = get_logger("05_autoloader_ingestion")

# COMMAND ----------

# DBTITLE 1,Inspect incoming files
files = dbutils.fs.ls(INCOMING_ORDERS_DIR)
logger.info("Incoming orders directory: %s (files=%s)", INCOMING_ORDERS_DIR, len(files))
for f in files:
    logger.info(" - %s", f.path)
display(files)

# COMMAND ----------

# DBTITLE 1,Start Auto Loader stream
orders_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", AUTOLOADER_SCHEMA_TRACKING)
    .option("header", True)
    .schema(orders_stream_schema)
    .load(INCOMING_ORDERS_DIR)
)

bronze_orders_stream_query = (
    orders_stream_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", AUTOLOADER_CHECKPOINT)
    .option("mergeSchema", "false")
    .trigger(availableNow=True)
    .toTable(BRONZE_ORDERS_STREAM)
)

bronze_orders_stream_query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Validate stream output
display(spark.table(BRONZE_ORDERS_STREAM).orderBy("order_id"))
print(f"Rows in stream bronze table: {spark.table(BRONZE_ORDERS_STREAM).count()}")

# COMMAND ----------

# DBTITLE 1,Incremental test instructions
# MAGIC %md
# MAGIC ## Incremental test
# MAGIC
# MAGIC 1. Go back to `00_data_generation` and re-run the helper cell that writes a new incremental file.
# MAGIC 2. Re-run the Auto Loader cells in this notebook.
# MAGIC 3. Confirm that only the newly dropped file adds rows to `bronze_orders_stream`, proving incremental ingestion behavior.
