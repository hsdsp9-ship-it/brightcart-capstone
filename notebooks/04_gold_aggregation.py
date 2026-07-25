# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 04 · Gold Aggregation
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Aggregate `silver_enriched_orders` (excluding `CANCELLED` orders) into three business-facing Gold tables: `gold_daily_revenue`, `gold_category_performance`, and `gold_customer_region_summary`.

# COMMAND ----------

# DBTITLE 1,Project configuration
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "harpalsingh")
dbutils.widgets.text("schema", "brightcart")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

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

# DBTITLE 1,Build gold_daily_revenue
gold_daily_revenue_df = (
    active_orders_df
    .groupBy("order_date")
    .agg(
        F.sum("total_amount").alias("daily_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
    )
    .orderBy("order_date")
)

gold_daily_revenue_df.write.mode("overwrite").format("delta").saveAsTable(GOLD_DAILY_REVENUE)
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
    .orderBy(F.desc("revenue"), F.asc("product_name"))
)

gold_category_performance_df.write.mode("overwrite").format("delta").saveAsTable(GOLD_CATEGORY_PERFORMANCE)
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
    .orderBy(F.desc("customer_revenue"), F.asc("customer_name"))
)

gold_customer_region_summary_df.write.mode("overwrite").format("delta").saveAsTable(GOLD_CUSTOMER_REGION_SUMMARY)
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
