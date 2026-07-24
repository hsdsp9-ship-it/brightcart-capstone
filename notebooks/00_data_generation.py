# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 00 · Data Generation
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Generates the three source CSVs (`customers.csv`, `products.csv`, `orders.csv`) described in the capstone spec directly into a Unity Catalog Volume, plus a small helper to drop new incremental order batches later for the Auto Loader (Day 4) exercises.
# MAGIC
# MAGIC Run this notebook once at the start of the project. Re-run the final cell any time you need a fresh incremental batch file for streaming tests.

# COMMAND ----------

# DBTITLE 1,Install Faker
# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Project configuration
CATALOG = "harpalsingh"
SCHEMA = "brightcart"
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
CHECKPOINT_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

CUSTOMERS_PATH = f"{RAW_VOLUME_PATH}/customers.csv"
PRODUCTS_PATH = f"{RAW_VOLUME_PATH}/products.csv"
ORDERS_PATH = f"{RAW_VOLUME_PATH}/orders.csv"
INCOMING_ORDERS_DIR = f"{RAW_VOLUME_PATH}/incoming_orders"
SCHEMA_TRACKING_DIR = f"{CHECKPOINT_VOLUME_PATH}/schema_tracking"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

for path in [RAW_VOLUME_PATH, CHECKPOINT_VOLUME_PATH, INCOMING_ORDERS_DIR, SCHEMA_TRACKING_DIR]:
    dbutils.fs.mkdirs(path)

print({
    "raw_volume": RAW_VOLUME_PATH,
    "checkpoints": CHECKPOINT_VOLUME_PATH,
    "incoming_orders": INCOMING_ORDERS_DIR,
})

# COMMAND ----------

# DBTITLE 1,Generate source datasets
from datetime import date
import random

from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
)

random.seed(42)
Faker.seed(42)
fake = Faker()

N_CUSTOMERS = 1000
N_PRODUCTS = 1000
N_ORDERS = 1000

customer_regions = ["North", "South", "East", "West"]
product_categories = ["Electronics", "Accessories", "Home", "Office"]
category_nouns = {
    "Electronics": ["Headphones", "Speaker", "Mouse", "Keyboard", "Webcam", "Charger", "Monitor", "Router", "Tablet", "Smartwatch"],
    "Accessories": ["Phone Case", "Screen Protector", "Cable", "Laptop Sleeve", "Wrist Rest", "Stylus", "Strap", "Cover"],
    "Home": ["Desk Lamp", "Air Purifier", "Smart Plug", "Vacuum", "Humidifier", "Diffuser", "Blender", "Kettle"],
    "Office": ["Notebook", "Chair", "Desk", "Whiteboard", "Stapler", "Organizer", "Planner", "Pen Set"],
}
statuses = ["COMPLETED", "PENDING", "CANCELLED"]
status_weights = [0.78, 0.15, 0.07]

# customers.csv -- realistic names via Faker
customers_data = []
for customer_id in range(1, N_CUSTOMERS + 1):
    signup_dt = fake.date_between(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31))
    customers_data.append((
        customer_id,
        fake.name(),
        random.choice(customer_regions),
        signup_dt,
    ))

# products.csv -- Faker-generated brand word + category-appropriate noun
products_data = []
for product_id in range(1, N_PRODUCTS + 1):
    category = product_categories[(product_id - 1) % len(product_categories)]
    noun = random.choice(category_nouns[category])
    brand = fake.word().capitalize()
    products_data.append((
        product_id,
        f"{brand} {noun}",
        category,
        round(random.uniform(12.0, 750.0), 2),
    ))

# orders.csv -- Faker-generated order dates within the sales year
orders_data = []
for order_id in range(1, N_ORDERS + 1):
    order_date_val = fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 12, 31))
    orders_data.append((
        order_id,
        random.randint(1, N_CUSTOMERS),
        random.randint(1, N_PRODUCTS),
        random.randint(1, 5),
        order_date_val,
        random.choices(statuses, weights=status_weights, k=1)[0],
    ))

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

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
products_df = spark.createDataFrame(products_data, schema=products_schema)
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# coalesce(1) ensures a single part-file per dataset so the collapse step below
# does not silently drop rows that landed in other partitions
customers_df.coalesce(1).write.mode("overwrite").option("header", True).csv(CUSTOMERS_PATH.replace(".csv", ""))
products_df.coalesce(1).write.mode("overwrite").option("header", True).csv(PRODUCTS_PATH.replace(".csv", ""))
orders_df.coalesce(1).write.mode("overwrite").option("header", True).csv(ORDERS_PATH.replace(".csv", ""))

# Collapse Spark CSV output to a single file path per dataset
for source_dir, final_file in [
    (CUSTOMERS_PATH.replace(".csv", ""), CUSTOMERS_PATH),
    (PRODUCTS_PATH.replace(".csv", ""), PRODUCTS_PATH),
    (ORDERS_PATH.replace(".csv", ""), ORDERS_PATH),
]:
    part_files = [f.path for f in dbutils.fs.ls(source_dir) if f.name.startswith("part-")]
    if not part_files:
        raise ValueError(f"No part file found in {source_dir}")
    dbutils.fs.cp(part_files[0], final_file, True)
    dbutils.fs.rm(source_dir, True)

print("Source files created:")
for f in dbutils.fs.ls(RAW_VOLUME_PATH):
    print(f.path)

# COMMAND ----------

# DBTITLE 1,Preview generated data
display(spark.read.option("header", True).csv(CUSTOMERS_PATH))
display(spark.read.option("header", True).csv(PRODUCTS_PATH))
display(spark.read.option("header", True).csv(ORDERS_PATH))

# COMMAND ----------

# DBTITLE 1,Validate referential integrity
customers_read_df = spark.read.option("header", True).option("inferSchema", True).csv(CUSTOMERS_PATH)
products_read_df = spark.read.option("header", True).option("inferSchema", True).csv(PRODUCTS_PATH)
orders_read_df = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)

total_orders = orders_read_df.count()

orders_with_customer = (
    orders_read_df.join(customers_read_df.select("customer_id"), on="customer_id", how="inner")
    .select("order_id").distinct().count()
)
orders_with_product = (
    orders_read_df.join(products_read_df.select("product_id"), on="product_id", how="inner")
    .select("order_id").distinct().count()
)

integrity_summary = spark.createDataFrame([
    ("orders -> customers", total_orders, orders_with_customer, total_orders - orders_with_customer),
    ("orders -> products", total_orders, orders_with_product, total_orders - orders_with_product),
], ["relationship", "total_orders", "orders_with_matching_key", "orders_with_no_match"])

display(integrity_summary)

assert total_orders - orders_with_customer == 0, "Found orders with a customer_id not present in customers.csv"
assert total_orders - orders_with_product == 0, "Found orders with a product_id not present in products.csv"
print("Referential integrity confirmed: every order references an existing customer_id and product_id.")

# COMMAND ----------

# DBTITLE 1,Create incremental order drop helper
from datetime import datetime, timedelta, timezone


def write_incremental_orders(batch_size: int = 25, start_order_id: int = None):
    if start_order_id is None:
        # Derive a fresh, non-overlapping starting id from any previously
        # dropped incremental files, so repeated calls never regenerate
        # the same order_id range (required for a valid idempotency test).
        existing_files = [f.path for f in dbutils.fs.ls(INCOMING_ORDERS_DIR) if f.name.endswith(".csv")]
        if existing_files:
            existing_max = (
                spark.read.option("header", True).option("inferSchema", True)
                .csv(existing_files)
                .agg(F.max("order_id"))
                .collect()[0][0]
            )
            start_order_id = (existing_max or 100000) + 1
        else:
            start_order_id = 100001

    incremental_rows = []
    for offset in range(batch_size):
        incremental_rows.append((
            start_order_id + offset,
            random.randint(1, N_CUSTOMERS),
            random.randint(1, N_PRODUCTS),
            random.randint(1, 5),
            date(2024, 12, 15) + timedelta(days=random.randint(0, 10)),
            random.choices(statuses, weights=status_weights, k=1)[0],
        ))

    incremental_df = spark.createDataFrame(incremental_rows, schema=orders_schema)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    temp_dir = f"{INCOMING_ORDERS_DIR}/temp_{timestamp}"
    final_file = f"{INCOMING_ORDERS_DIR}/orders_incremental_{timestamp}.csv"

    incremental_df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)
    part_file = next(f.path for f in dbutils.fs.ls(temp_dir) if f.name.startswith("part-"))
    dbutils.fs.cp(part_file, final_file, True)
    dbutils.fs.rm(temp_dir, True)
    return final_file

new_file = write_incremental_orders()
print(f"Incremental batch written to: {new_file}")
