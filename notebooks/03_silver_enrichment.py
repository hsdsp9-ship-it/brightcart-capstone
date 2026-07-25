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

# DBTITLE 1,Project configuration
from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "harpalsingh")
dbutils.widgets.text("schema", "brightcart")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

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
spark.sql(f"""
INSERT INTO {SILVER_ENRICHED_ORDERS}
SELECT
  999001 AS order_id,
  1 AS customer_id,
  'Customer 001' AS customer_name,
  'North' AS region,
  DATE('2023-01-05') AS signup_date,
  1 AS product_id,
  'Product 001' AS product_name,
  'Electronics' AS category,
  199.99 AS unit_price,
  1 AS quantity,
  DATE('2024-05-20') AS order_date,
  'PENDING' AS status,
  false AS is_cancelled,
  199.99 AS total_amount,
  current_timestamp() AS silver_updated_ts
""")

spark.sql(f"""
UPDATE {SILVER_ENRICHED_ORDERS}
SET status = 'COMPLETED',
    total_amount = quantity * unit_price,
    silver_updated_ts = current_timestamp()
WHERE order_id = 999001
""")

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta DELETE
spark.sql(f"""
INSERT INTO {SILVER_ENRICHED_ORDERS}
SELECT
  999002 AS order_id,
  2 AS customer_id,
  'Customer 002' AS customer_name,
  'South' AS region,
  DATE('2023-02-02') AS signup_date,
  2 AS product_id,
  'Product 002' AS product_name,
  'Accessories' AS category,
  49.99 AS unit_price,
  1 AS quantity,
  DATE('2024-05-20') AS order_date,
  'CANCELLED' AS status,
  true AS is_cancelled,
  49.99 AS total_amount,
  current_timestamp() AS silver_updated_ts
""")

spark.sql(f"DELETE FROM {SILVER_ENRICHED_ORDERS} WHERE order_id = 999002")

# COMMAND ----------

# DBTITLE 1,Demonstrate Delta MERGE
merge_source_df = spark.createDataFrame([
    (999001, 1, 'Customer 001', 'North', '2023-01-05', 1, 'Product 001', 'Electronics', 199.99, 2, '2024-05-20', 'COMPLETED', False, 399.98),
    (999003, 3, 'Customer 003', 'East', '2023-03-03', 3, 'Product 003', 'Home', 89.50, 1, '2024-05-21', 'COMPLETED', False, 89.50),
], [
    'order_id', 'customer_id', 'customer_name', 'region', 'signup_date', 'product_id', 'product_name',
    'category', 'unit_price', 'quantity', 'order_date', 'status', 'is_cancelled', 'total_amount'
]).select(
    'order_id', 'customer_id', 'customer_name', 'region', F.to_date('signup_date').alias('signup_date'),
    'product_id', 'product_name', 'category', 'unit_price', 'quantity', F.to_date('order_date').alias('order_date'),
    'status', 'is_cancelled', 'total_amount'
).withColumn('silver_updated_ts', F.current_timestamp())

delta_target = DeltaTable.forName(spark, SILVER_ENRICHED_ORDERS)
(
    delta_target.alias('t')
    .merge(merge_source_df.alias('s'), 't.order_id = s.order_id')
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

display(spark.table(SILVER_ENRICHED_ORDERS).filter(F.col('order_id') >= 999001).orderBy('order_id'))

# COMMAND ----------

# DBTITLE 1,Inspect Delta history
display(spark.sql(f"DESCRIBE HISTORY {SILVER_ENRICHED_ORDERS}"))
