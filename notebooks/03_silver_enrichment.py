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
# ─── CRUD Demo ───────────────────────────────────────────────────────────────
# A dedicated sandbox table keeps fake rows out of the Gold-layer aggregations.

SILVER_DEMO = f"{CATALOG}.{SCHEMA}.silver_crud_demo"

# Seed the demo table from a 10-row slice of the real Silver table
(
    spark.table(SILVER_ENRICHED_ORDERS).limit(10)
    .write.mode("overwrite").format("delta").saveAsTable(SILVER_DEMO)
)
print(f"Demo table created: {SILVER_DEMO} ({spark.table(SILVER_DEMO).count()} rows)")

# ── INSERT: add a brand-new order row ─────────────────────────────────────────
spark.sql(f"""
    INSERT INTO {SILVER_DEMO}
        (order_id, customer_id, customer_name, region, signup_date,
         product_id, product_name, category, unit_price, quantity,
         order_date, status, is_cancelled, total_amount, silver_updated_ts)
    VALUES
        (9999999, 1, 'Demo Customer', 'North', DATE('2024-01-01'),
         1, 'Demo Product', 'Electronics', 99.99, 2,
         DATE('2024-06-15'), 'PENDING', false, 199.98, current_timestamp())
""")
print("After INSERT — order_id=9999999:")
display(spark.table(SILVER_DEMO).filter("order_id = 9999999"))

# ── UPDATE: change status from PENDING → COMPLETED ────────────────────────────
spark.sql(f"""
    UPDATE {SILVER_DEMO}
    SET status = 'COMPLETED',
        is_cancelled = false,
        silver_updated_ts = current_timestamp()
    WHERE order_id = 9999999
""")
print("After UPDATE — status should now be COMPLETED:")
display(spark.table(SILVER_DEMO).filter("order_id = 9999999"))

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta DELETE
# ── DELETE: remove the demo order inserted above ─────────────────────────────
SILVER_DEMO = f"{CATALOG}.{SCHEMA}.silver_crud_demo"

spark.sql(f"""
    DELETE FROM {SILVER_DEMO}
    WHERE order_id = 9999999
""")

remaining = spark.table(SILVER_DEMO).filter("order_id = 9999999").count()
print(f"After DELETE — rows with order_id=9999999: {remaining}  (expected 0)")

# Show Delta history: every CRUD operation appears as a distinct version
print("Delta transaction log for the demo table:")
display(spark.sql(f"DESCRIBE HISTORY {SILVER_DEMO}"))

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta MERGE
# ── MERGE: upsert using the DeltaTable API ────────────────────────────────────
# Row A  → order_id already exists in demo table  → triggers WHEN MATCHED UPDATE
# Row B  → brand-new order_id               → triggers WHEN NOT MATCHED INSERT
from datetime import date
from delta.tables import DeltaTable

SILVER_DEMO = f"{CATALOG}.{SCHEMA}.silver_crud_demo"

first_existing_id = spark.table(SILVER_DEMO).select("order_id").first()["order_id"]

demo_schema = spark.table(SILVER_DEMO).schema

updates_df = spark.createDataFrame(
    [
        # Row A: update quantity and total_amount on an existing row
        # date() objects required — PyArrow cannot cast str → DateType
        (first_existing_id, 1, "Existing Customer", "South", date(2024, 1, 1),
         1, "Updated Product", "Electronics", 99.99, 9,
         date(2024, 6, 15), "COMPLETED", False, 899.91, None),
        # Row B: insert a completely new row
        (9999998, 2, "New Merged Customer", "West", date(2024, 3, 1),
         2, "New Product", "Accessories", 25.00, 3,
         date(2024, 9, 1), "COMPLETED", False, 75.00, None),
    ],
    schema=demo_schema,
)

DeltaTable.forName(spark, SILVER_DEMO).alias("target") \
    .merge(updates_df.alias("source"), "target.order_id = source.order_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print("After MERGE — final demo table state:")
display(spark.table(SILVER_DEMO).orderBy("order_id"))

# Clean up: drop the sandbox table so the real Silver table is untouched
spark.sql(f"DROP TABLE IF EXISTS {SILVER_DEMO}")
print("Demo table dropped — silver_enriched_orders is unchanged.")

# COMMAND ----------

# DBTITLE 1,Inspect Delta history
display(spark.sql(f"DESCRIBE HISTORY {SILVER_ENRICHED_ORDERS}"))
