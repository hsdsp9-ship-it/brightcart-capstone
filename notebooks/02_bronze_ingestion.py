# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 02 · Bronze Ingestion
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Read the raw CSVs with explicit schemas (no `inferSchema`), add ingestion metadata columns, and persist them as managed Delta tables: `bronze_customers`, `bronze_products`, `bronze_orders`.

# COMMAND ----------

# DBTITLE 1,Project configuration
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

dbutils.widgets.text("catalog", "harpalsingh")
dbutils.widgets.text("schema", "brightcart")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

CUSTOMERS_PATH = f"{RAW_VOLUME_PATH}/customers.csv"
PRODUCTS_PATH = f"{RAW_VOLUME_PATH}/products.csv"
ORDERS_PATH = f"{RAW_VOLUME_PATH}/orders.csv"

BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS = f"{CATALOG}.{SCHEMA}.bronze_orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

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
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv(PRODUCTS_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(ORDERS_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta tables
customers_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_CUSTOMERS)
products_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_PRODUCTS)
orders_df.write.mode("overwrite").format("delta").saveAsTable(BRONZE_ORDERS)

print(f"Created: {BRONZE_CUSTOMERS}")
print(f"Created: {BRONZE_PRODUCTS}")
print(f"Created: {BRONZE_ORDERS}")

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
