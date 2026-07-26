# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 06 · Streaming Silver/Gold
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Stream from `bronze_orders_stream`, join against the static `bronze_customers`/`bronze_products` dimension tables, and upsert (via `foreachBatch` + `MERGE`) into `silver_enriched_orders_stream`, keyed on `order_id`.

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
from pyspark.sql import functions as F
from notebooks.config import get_config

cfg = get_config()

CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]
CHECKPOINT_VOLUME_PATH = cfg["checkpoint_volume"]

BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS_STREAM = f"{CATALOG}.{SCHEMA}.bronze_orders_stream"
SILVER_ENRICHED_ORDERS_STREAM = f"{CATALOG}.{SCHEMA}.silver_enriched_orders_stream"
STREAMING_CHECKPOINT = f"{CHECKPOINT_VOLUME_PATH}/silver_enriched_orders_stream"

from notebooks.logging_helper import get_logger

logger = get_logger("06_streaming_silver_gold")

# COMMAND ----------

# DBTITLE 1,Load dimensions and source stream
customers_dim_df = spark.table(BRONZE_CUSTOMERS).select("customer_id", "customer_name", "region", "signup_date")
products_dim_df = spark.table(BRONZE_PRODUCTS).select("product_id", "product_name", "category", "unit_price")
orders_stream_df = spark.readStream.table(BRONZE_ORDERS_STREAM)

stream_enriched_df = (
    orders_stream_df.alias("o")
    .join(customers_dim_df.alias("c"), on="customer_id", how="left")
    .join(products_dim_df.alias("p"), on="product_id", how="left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("c.customer_name"),
        F.col("c.region"),
        F.col("c.signup_date"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.unit_price"),
        F.col("o.quantity"),
        F.col("o.order_date"),
        F.col("o.status"),
        (F.col("o.status") == F.lit("CANCELLED")).alias("is_cancelled"),
        (F.col("o.quantity") * F.col("p.unit_price")).alias("total_amount"),
        F.current_timestamp().alias("stream_processed_ts"),
    )
)

# COMMAND ----------

# DBTITLE 1,Define foreachBatch merge
def upsert_silver_stream(batch_df, batch_id):
    # Unique per-batch view name avoids the Spark Connect stale temp-view
    # anti-pattern (SCPAP003) from reusing a static name across micro-batches.
    view_name = f"silver_stream_batch_{batch_id}"
    try:
        record_count = batch_df.count()
    except Exception:
        record_count = None
    logger.info("Processing micro-batch %s (estimated rows=%s)", batch_id, record_count)
    batch_df.createOrReplaceTempView(view_name)
    batch_df.sparkSession.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_ENRICHED_ORDERS_STREAM} (
            order_id INT,
            customer_id INT,
            customer_name STRING,
            region STRING,
            signup_date DATE,
            product_id INT,
            product_name STRING,
            category STRING,
            unit_price DOUBLE,
            quantity INT,
            order_date DATE,
            status STRING,
            is_cancelled BOOLEAN,
            total_amount DOUBLE,
            stream_processed_ts TIMESTAMP
        ) USING DELTA
    """)

    try:
        batch_df.sparkSession.sql(f"""
            MERGE INTO {SILVER_ENRICHED_ORDERS_STREAM} AS target
            USING {view_name} AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info("Micro-batch %s merged into %s", batch_id, SILVER_ENRICHED_ORDERS_STREAM)
    except Exception as e:
        logger.exception("Failed to merge micro-batch %s: %s", batch_id, e)
        raise
    batch_df.sparkSession.catalog.dropTempView(view_name)

# COMMAND ----------

# DBTITLE 1,Run streaming upsert
silver_stream_query = (
    stream_enriched_df
    .writeStream
    .foreachBatch(upsert_silver_stream)
    .option("checkpointLocation", STREAMING_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

silver_stream_query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Validate output and idempotency
silver_stream_df = spark.table(SILVER_ENRICHED_ORDERS_STREAM)

display(silver_stream_df.orderBy("order_id"))

display(
    silver_stream_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Rows in silver stream table: {silver_stream_df.count()}")

# COMMAND ----------

# DBTITLE 1,Re-run note
# MAGIC %md
# MAGIC ## Idempotency check
# MAGIC
# MAGIC After `05_autoloader_ingestion` processes a new incremental file, re-run this notebook once more.
# MAGIC
# MAGIC Expected result:
# MAGIC * Existing `order_id` values are updated, not duplicated.
# MAGIC * New `order_id` values are inserted.
# MAGIC * The duplicate check cell should return zero rows.
