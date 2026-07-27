# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 08 · Daily Data Generator
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC Generates a fresh batch of orders with **today’s actual date** and drops a timestamped CSV into the Auto Loader landing zone before every pipeline run. This ensures the gold-layer aggregations and the business report reflect genuinely new data on each execution — not the same static 2024 dataset re-aggregated.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Derive the next available `order_id` from existing tables (never collides with historical data)
# MAGIC 2. Pick random `customer_id` (1–1000) and `product_id` (1–1000) from the existing dimension universe
# MAGIC 3. Write 15 orders with `order_date = today` as `orders_incremental_<timestamp>.csv`
# MAGIC 4. Auto Loader (notebook 05) picks the file up incrementally on the next trigger

# COMMAND ----------

# DBTITLE 1,Setup & config
import sys, os, random, csv
from datetime import date, datetime

try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _bundle_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
    if _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)
except Exception:
    pass

from pyspark.sql import functions as F
from notebooks.config import get_config

cfg = get_config()
CATALOG             = cfg["catalog"]
SCHEMA              = cfg["schema"]
INCOMING_ORDERS_DIR = cfg["incoming_orders_dir"]

BRONZE_ORDERS        = f"{CATALOG}.{SCHEMA}.bronze_orders"
BRONZE_ORDERS_STREAM = f"{CATALOG}.{SCHEMA}.bronze_orders_stream"

TODAY      = date.today()
TODAY_STR  = TODAY.isoformat()                    # e.g. "2026-07-27"
TIMESTAMP  = datetime.now().strftime("%Y%m%d%H%M%S")
N_ORDERS   = 15

print(f"Catalog       : {CATALOG}.{SCHEMA}")
print(f"Landing zone  : {INCOMING_ORDERS_DIR}")
print(f"Today         : {TODAY_STR}")
print(f"Orders to gen : {N_ORDERS}")

# COMMAND ----------

# DBTITLE 1,Derive next order_id
def safe_max_id(table_name: str) -> int:
    """Return the max order_id in a table, or 0 if the table is empty/missing."""
    try:
        val = spark.table(table_name).agg(F.max("order_id")).collect()[0][0]
        return int(val) if val is not None else 0
    except Exception:
        return 0

max_batch_id  = safe_max_id(BRONZE_ORDERS)
max_stream_id = safe_max_id(BRONZE_ORDERS_STREAM)
base_order_id = max(max_batch_id, max_stream_id) + 1

print(f"Max batch order_id  : {max_batch_id:,}")
print(f"Max stream order_id : {max_stream_id:,}")
print(f"New IDs start at    : {base_order_id:,}")

# COMMAND ----------

# DBTITLE 1,Generate today's orders
random.seed(int(TIMESTAMP) % (2**31))   # reproducible per-run seed

CUSTOMER_IDS = list(range(1, 1001))
PRODUCT_IDS  = list(range(1, 1001))
# Realistic status mix: ~70 % COMPLETED, 20 % PENDING, 10 % CANCELLED
STATUSES     = ["COMPLETED"] * 7 + ["PENDING"] * 2 + ["CANCELLED"]

orders = []
for i in range(N_ORDERS):
    orders.append({
        "order_id":    base_order_id + i,
        "customer_id": random.choice(CUSTOMER_IDS),
        "product_id":  random.choice(PRODUCT_IDS),
        "quantity":    random.randint(1, 5),
        "order_date":  TODAY_STR,
        "status":      random.choice(STATUSES),
    })

status_counts = {s: sum(1 for o in orders if o["status"] == s) for s in set(o["status"] for o in orders)}
print(f"Generated {len(orders)} orders for {TODAY_STR}")
print(f"  order_id range : {orders[0]['order_id']:,} — {orders[-1]['order_id']:,}")
print(f"  Status mix     : {status_counts}")
for o in orders[:3]:
    print(f"  {o}")
print("  ...")

# COMMAND ----------

# DBTITLE 1,Write CSV to landing zone
output_filename = f"orders_incremental_{TIMESTAMP}.csv"
volume_dest     = f"{INCOMING_ORDERS_DIR}/{output_filename}"

# Write directly to the UC Volume path using standard Python open().
# UC Volumes (/Volumes/...) are accessible via Python file I/O on Shared clusters.
# NEVER use /tmp/ + dbutils.fs.cp — /tmp/ is blocked on Shared UC clusters.
with open(volume_dest, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["order_id", "customer_id", "product_id", "quantity", "order_date", "status"]
    )
    writer.writeheader()
    writer.writerows(orders)

print(f"✅ Dropped {N_ORDERS} new orders into the Auto Loader landing zone")
print(f"   File      : {volume_dest}")
print(f"   Date      : {TODAY_STR}")
print(f"   order_ids : {base_order_id:,} → {base_order_id + N_ORDERS - 1:,}")

# COMMAND ----------

# DBTITLE 1,Confirm file in landing zone
all_files = dbutils.fs.ls(INCOMING_ORDERS_DIR)
print(f"Landing zone now contains {len(all_files)} file(s) (most recent first):")
for f in sorted(all_files, key=lambda x: x.modificationTime, reverse=True)[:5]:
    print(f"  {f.name:<55} {f.size:>6} bytes")
print("\nAuto Loader (notebook 05) will pick up the new file incrementally on the next trigger.")
