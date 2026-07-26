# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 03 · Silver Enrichment
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Clean the Bronze data (nulls, duplicates, types), join orders with customers and products to build `silver_enriched_orders`, and demonstrate Delta Lake CRUD (`INSERT`, `UPDATE`, `DELETE`, `MERGE`).
# MAGIC
# MAGIC **Business decision:** `CANCELLED` orders are kept in the Silver table (flagged via `is_cancelled`) so history is preserved, but are filtered out of the Gold-layer revenue aggregations in `04_gold_aggregation`.

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
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from notebooks.config import get_config

cfg = get_config()

CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]

BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_ENRICHED_ORDERS = f"{CATALOG}.{SCHEMA}.silver_enriched_orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Load Bronze tables
customers_bronze_df = spark.table(BRONZE_CUSTOMERS)
products_bronze_df = spark.table(BRONZE_PRODUCTS)
orders_bronze_df = spark.table(BRONZE_ORDERS)

print(customers_bronze_df.count(), products_bronze_df.count(), orders_bronze_df.count())

# COMMAND ----------

# DBTITLE 1,Clean and deduplicate Bronze data
customers_clean_df = (
    customers_bronze_df
    .dropna(subset=["customer_id", "customer_name", "region", "signup_date"])
    .dropDuplicates(["customer_id"])
    .withColumn("signup_date", F.to_date("signup_date"))
)

products_clean_df = (
    products_bronze_df
    .dropna(subset=["product_id", "product_name", "category", "unit_price"])
    .dropDuplicates(["product_id"])
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)

orders_clean_df = (
    orders_bronze_df
    .dropna(subset=["order_id", "customer_id", "product_id", "quantity", "order_date", "status"])
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_date"))
)

# COMMAND ----------

# DBTITLE 1,Build Silver enriched table
silver_enriched_df = (
    orders_clean_df.alias("o")
    .join(customers_clean_df.alias("c"), on="customer_id", how="left")
    .join(products_clean_df.alias("p"), on="product_id", how="left")
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
        F.current_timestamp().alias("silver_updated_ts"),
    )
)

silver_enriched_df.write.mode("overwrite").format("delta").saveAsTable(SILVER_ENRICHED_ORDERS)
display(spark.table(SILVER_ENRICHED_ORDERS))

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta INSERT and UPDATE
# DEMO cell disabled for production runs — fake test orders must not pollute Silver/Gold
pass

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta DELETE
# DEMO cell disabled for production runs — fake test orders must not pollute Silver/Gold
pass

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta MERGE
# DEMO cell disabled for production runs — fake test orders must not pollute Silver/Gold
pass

# COMMAND ----------

# DBTITLE 1,Inspect Delta history
display(spark.sql(f"DESCRIBE HISTORY {SILVER_ENRICHED_ORDERS}"))
