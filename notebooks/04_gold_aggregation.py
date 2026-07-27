# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 04 · Gold Aggregation
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Aggregate `silver_enriched_orders` (excluding `CANCELLED` orders) into three business-facing Gold tables: `gold_daily_revenue`, `gold_category_performance`, and `gold_customer_region_summary`.

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

SILVER_ENRICHED_ORDERS = f"{CATALOG}.{SCHEMA}.silver_enriched_orders"
GOLD_DAILY_REVENUE = f"{CATALOG}.{SCHEMA}.gold_daily_revenue"
GOLD_CATEGORY_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_category_performance"
GOLD_CUSTOMER_REGION_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_customer_region_summary"

# COMMAND ----------

# DBTITLE 1,Load Silver data
silver_df = spark.table(SILVER_ENRICHED_ORDERS)
active_orders_df = silver_df.filter(~F.col("is_cancelled"))

display(active_orders_df)

# COMMAND ----------

# DBTITLE 1,Merge stream data into active_orders
# ── Enrich bronze_orders_stream and union with batch silver ───────────────────────
# bronze_orders_stream is fed by 08_daily_data_generator (today's orders)
# and by any other incremental CSV files Auto Loader has picked up.
# Union it with the static batch silver so gold tables grow over time.

BRONZE_CUSTOMERS     = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_PRODUCTS      = f"{CATALOG}.{SCHEMA}.bronze_products"
BRONZE_ORDERS_STREAM = f"{CATALOG}.{SCHEMA}.bronze_orders_stream"

# Derive the columns to select for the stream-enriched DataFrame.
# Only include columns that can be built from bronze (drop silver-only audit
# columns like silver_updated_ts — they will be filled as null via
# allowMissingColumns=True in the unionByName below).
STREAM_COLS = [
    "order_id", "customer_id", "customer_name", "region", "signup_date",
    "product_id", "product_name", "category", "unit_price",
    "quantity", "order_date", "status", "is_cancelled", "total_amount", "_ingested_at"
]

try:
    stream_raw_df = spark.table(BRONZE_ORDERS_STREAM)
    stream_count  = stream_raw_df.count()

    if stream_count > 0:
        cust_df = spark.table(BRONZE_CUSTOMERS).select(
            "customer_id", "customer_name", "region", "signup_date")
        prod_df = spark.table(BRONZE_PRODUCTS).select(
            "product_id", "product_name", "category", "unit_price")

        stream_enriched_df = (
            stream_raw_df
            .join(cust_df, on="customer_id", how="left")
            .join(prod_df, on="product_id",  how="left")
            .withColumn("is_cancelled", F.col("status") == "CANCELLED")
            .withColumn("total_amount",  F.round(F.col("unit_price") * F.col("quantity"), 2))
            .withColumn("_ingested_at",  F.current_timestamp())
            .select(*STREAM_COLS)
        )

        # Union batch + stream.
        # allowMissingColumns=True fills null for any silver-only audit columns
        # (e.g. silver_updated_ts) that don’t exist in the stream-enriched data.
        # Downstream gold cells only use business columns so nulls are harmless.
        combined_df = silver_df.unionByName(
            stream_enriched_df, allowMissingColumns=True
        ).dropDuplicates(["order_id"])
        active_orders_df = combined_df.filter(~F.col("is_cancelled"))

        batch_count  = silver_df.count()
        active_count = active_orders_df.count()
        print(f"✅ Stream merged: {batch_count:,} batch + {stream_count:,} stream = {active_count:,} active orders")
    else:
        print("ℹ️  bronze_orders_stream is empty — using batch silver only")

except Exception as e:
    print(f"⚠️  Could not load bronze_orders_stream ({e}) — using batch silver only")

# COMMAND ----------

# DBTITLE 1,Build gold_daily_revenue
gold_daily_revenue_df = (
    active_orders_df
    .groupBy("order_date")
    .agg(
        F.sum("total_amount").alias("daily_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
    )
    .withColumn("_report_date", F.current_date())
    .orderBy("order_date")
)

gold_daily_revenue_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(GOLD_DAILY_REVENUE)
display(spark.table(GOLD_DAILY_REVENUE))

# COMMAND ----------

# DBTITLE 1,Build gold_category_performance
gold_category_performance_df = (
    active_orders_df
    .groupBy("category", "product_name")
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .withColumn("_report_date", F.current_date())
    .orderBy(F.desc("revenue"), F.asc("product_name"))
)

gold_category_performance_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(GOLD_CATEGORY_PERFORMANCE)
display(spark.table(GOLD_CATEGORY_PERFORMANCE))

# COMMAND ----------

# DBTITLE 1,Build gold_customer_region_summary
gold_customer_region_summary_df = (
    active_orders_df
    .groupBy("region", "customer_id", "customer_name")
    .agg(
        F.sum("total_amount").alias("customer_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_purchased"),
    )
    .withColumn("_report_date", F.current_date())
    .orderBy(F.desc("customer_revenue"), F.asc("customer_name"))
)

gold_customer_region_summary_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(GOLD_CUSTOMER_REGION_SUMMARY)
display(spark.table(GOLD_CUSTOMER_REGION_SUMMARY))

# COMMAND ----------

# DBTITLE 1,Spot-check validation
sample_region = "North"
manual_region_total = (
    active_orders_df
    .filter(F.col("region") == sample_region)
    .agg(F.round(F.sum("total_amount"), 2).alias("manual_total"))
)

summary_region_total = (
    spark.table(GOLD_CUSTOMER_REGION_SUMMARY)
    .filter(F.col("region") == sample_region)
    .agg(F.round(F.sum("customer_revenue"), 2).alias("summary_total"))
)

display(manual_region_total.crossJoin(summary_region_total))
