# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 02 · Bronze Ingestion
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Read the raw CSVs with explicit schemas (no `inferSchema`), add ingestion metadata columns, and persist them as managed Delta tables: `bronze_customers`, `bronze_products`, `bronze_orders`.

# COMMAND ----------

# DBTITLE 1,Resolve bundle sys.path
import sys, os

# When running as a Databricks job task the bundle sync root is not on sys.path.
# Resolve it dynamically so `from notebooks.config import ...` works for any
# deployment target (dev / staging / prod) as well as interactive runs.
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _bundle_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
    if _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)
except Exception:
    pass  # Local / pytest run — conftest.py handles sys.path there

# COMMAND ----------

# DBTITLE 1,Project configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from notebooks.config import get_config

cfg = get_config()

CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]
RAW_VOLUME_PATH = cfg["raw_volume"]

CUSTOMERS_PATH = cfg["customers_path"]
PRODUCTS_PATH = cfg["products_path"]
ORDERS_PATH = cfg["orders_path"]

BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS = f"{CATALOG}.{SCHEMA}.bronze_orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

from notebooks.logging_helper import get_logger

logger = get_logger("02_bronze_ingestion")

# COMMAND ----------

# DBTITLE 1,Define schemas
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("signup_date", DateType(), False),
])

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_date", DateType(), False),
    StructField("status", StringType(), False),
])

# COMMAND ----------

# DBTITLE 1,Read raw CSVs
customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv(CUSTOMERS_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv(PRODUCTS_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(ORDERS_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

logger.info("Read source CSVs: customers=%s products=%s orders=%s", customers_df.count(), products_df.count(), orders_df.count())

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta tables
customers_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_CUSTOMERS)
products_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_PRODUCTS)
orders_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_ORDERS)
logger.info("Created Bronze tables: %s, %s, %s", BRONZE_CUSTOMERS, BRONZE_PRODUCTS, BRONZE_ORDERS)

# COMMAND ----------

# DBTITLE 1,Validate counts and schemas
validation_rows = [
    ("bronze_customers", spark.table(BRONZE_CUSTOMERS).count(), customers_df.count()),
    ("bronze_products", spark.table(BRONZE_PRODUCTS).count(), products_df.count()),
    ("bronze_orders", spark.table(BRONZE_ORDERS).count(), orders_df.count()),
]

display(spark.createDataFrame(validation_rows, ["table_name", "table_row_count", "source_row_count"]))

spark.table(BRONZE_CUSTOMERS).printSchema()
spark.table(BRONZE_PRODUCTS).printSchema()
spark.table(BRONZE_ORDERS).printSchema()

# COMMAND ----------

# DBTITLE 1,Preview Bronze tables
display(spark.table(BRONZE_CUSTOMERS))
display(spark.table(BRONZE_PRODUCTS))
display(spark.table(BRONZE_ORDERS))
