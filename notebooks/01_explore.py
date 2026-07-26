# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 01 · Explore
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Read the three raw CSVs (`customers`, `products`, `orders`), inspect schema and row counts, and profile the data for quality issues (nulls, duplicates, unexpected categorical values) that need to be handled in the Bronze/Silver notebooks.
# MAGIC
# MAGIC Run `00_data_generation` first if the raw CSVs don't exist yet.

# COMMAND ----------

# MAGIC %run ./00_data_generation

# COMMAND ----------

# DBTITLE 1,Project configuration
from pyspark.sql import functions as F
from notebooks.config import get_config

cfg = get_config()

CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]
RAW_VOLUME_PATH = cfg["raw_volume"]

CUSTOMERS_PATH = cfg["customers_path"]
PRODUCTS_PATH = cfg["products_path"]
ORDERS_PATH = cfg["orders_path"]

source_paths = {
    "customers": CUSTOMERS_PATH,
    "products": PRODUCTS_PATH,
    "orders": ORDERS_PATH,
}

for name, path in source_paths.items():
    print(f"{name}: {path}")

# COMMAND ----------

# DBTITLE 1,Read source data
customers_df = spark.read.option("header", True).option("inferSchema", True).csv(CUSTOMERS_PATH)
products_df = spark.read.option("header", True).option("inferSchema", True).csv(PRODUCTS_PATH)
orders_df = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)

print("Customers schema")
customers_df.printSchema()
print("Products schema")
products_df.printSchema()
print("Orders schema")
orders_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Preview data
display(customers_df)
display(products_df)
display(orders_df)

# COMMAND ----------

# DBTITLE 1,Row counts
row_counts = [
    ("customers", customers_df.count()),
    ("products", products_df.count()),
    ("orders", orders_df.count()),
]

display(spark.createDataFrame(row_counts, ["dataset", "row_count"]))

# COMMAND ----------

# DBTITLE 1,Null profiling helper
def null_profile(df, dataset_name: str):
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).withColumn("dataset", F.lit(dataset_name))

display(null_profile(customers_df, "customers"))
display(null_profile(products_df, "products"))
display(null_profile(orders_df, "orders"))

# COMMAND ----------

# DBTITLE 1,Duplicate checks
duplicate_checks = [
    ("customers", "customer_id", customers_df.count() - customers_df.select("customer_id").distinct().count()),
    ("products", "product_id", products_df.count() - products_df.select("product_id").distinct().count()),
    ("orders", "order_id", orders_df.count() - orders_df.select("order_id").distinct().count()),
]

display(spark.createDataFrame(duplicate_checks, ["dataset", "key_column", "duplicate_count"]))

# COMMAND ----------

# DBTITLE 1,Categorical value checks
display(customers_df.groupBy("region").count().orderBy("region"))
display(products_df.groupBy("category").count().orderBy("category"))
display(orders_df.groupBy("status").count().orderBy("status"))

# COMMAND ----------

# DBTITLE 1,Summary findings
# MAGIC %md
# MAGIC ## Findings to carry forward
# MAGIC
# MAGIC Update this markdown cell after reviewing the outputs above.
# MAGIC
# MAGIC Suggested points to record:
# MAGIC * Whether any nulls need to be dropped or imputed
# MAGIC * Whether any duplicate keys exist
# MAGIC * Whether inferred data types match the expected business schema
# MAGIC * Whether any unexpected `region`, `category`, or `status` values appear
# MAGIC * Any notes that should become explicit cleaning rules in the Bronze or Silver layers
