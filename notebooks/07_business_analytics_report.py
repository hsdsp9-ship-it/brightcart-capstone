# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 07 · BrightCart Business Analytics Report
# MAGIC
# MAGIC **BrightCart Retail Order Analytics — Capstone Project**
# MAGIC
# MAGIC This notebook delivers a complete business intelligence view of BrightCart's 2024 sales performance.
# MAGIC All metrics are sourced directly from the Gold-layer Delta tables built in `04_gold_aggregation`.
# MAGIC
# MAGIC **Metrics covered:**
# MAGIC 1. Total Revenue & KPI Summary
# MAGIC 2. Revenue Trend Over Time
# MAGIC 3. Revenue by Product Category
# MAGIC 4. Top 10 Products by Revenue
# MAGIC 5. Revenue by Region
# MAGIC 6. Top 10 Customers by Spend
# MAGIC 7. Order Volume Analysis
# MAGIC 8. Region vs Category Heatmap
# MAGIC
# MAGIC > Run all cells top-to-bottom. Charts are rendered with Matplotlib (inline). All analysis excludes CANCELLED orders.

# COMMAND ----------

# DBTITLE 1,Setup & imports
import sys, os
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Inline matplotlib for Databricks notebooks
%matplotlib inline
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "#f9f9f9",
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "font.family":      "sans-serif",
    "font.size":        11,
    "axes.titlesize":   14,
    "axes.titleweight": "bold",
    "axes.labelsize":   12,
})

# Resolve bundle sys.path
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _bundle_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
    if _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)
except Exception:
    pass

print("Setup complete.")

# COMMAND ----------

# DBTITLE 1,Load Gold tables
from pyspark.sql import functions as F
from notebooks.config import get_config

cfg = get_config()
CATALOG = cfg["catalog"]
SCHEMA  = cfg["schema"]

GOLD_DAILY_REVENUE         = f"{CATALOG}.{SCHEMA}.gold_daily_revenue"
GOLD_CATEGORY_PERFORMANCE  = f"{CATALOG}.{SCHEMA}.gold_category_performance"
GOLD_CUSTOMER_REGION       = f"{CATALOG}.{SCHEMA}.gold_customer_region_summary"

# Load all three gold tables into Pandas for charting
daily_revenue_pd   = spark.table(GOLD_DAILY_REVENUE).orderBy("order_date").toPandas()
category_pd        = spark.table(GOLD_CATEGORY_PERFORMANCE).toPandas()
customer_region_pd = spark.table(GOLD_CUSTOMER_REGION).toPandas()

print(f"gold_daily_revenue       : {len(daily_revenue_pd):,} rows")
print(f"gold_category_performance: {len(category_pd):,} rows")
print(f"gold_customer_region     : {len(customer_region_pd):,} rows")

# COMMAND ----------

# DBTITLE 1,Section 1 — KPI Summary
# MAGIC %md
# MAGIC ## 📊 Section 1 — KPI Executive Summary
# MAGIC
# MAGIC High-level headline numbers across the full 2024 reporting period.

# COMMAND ----------

# DBTITLE 1,KPI metrics display
# ── Compute KPIs ──────────────────────────────────────────────────────────────
total_revenue    = daily_revenue_pd["daily_revenue"].sum()
total_orders     = daily_revenue_pd["order_count"].sum()
total_units      = daily_revenue_pd["units_sold"].sum()
avg_order_value  = total_revenue / total_orders if total_orders else 0

top_category     = category_pd.groupby("category")["revenue"].sum().idxmax()
top_category_rev = category_pd.groupby("category")["revenue"].sum().max()

top_customer_row = customer_region_pd.nlargest(1, "customer_revenue").iloc[0]
top_region_row   = customer_region_pd.groupby("region")["customer_revenue"].sum().idxmax()
top_region_rev   = customer_region_pd.groupby("region")["customer_revenue"].sum().max()

date_min = daily_revenue_pd["order_date"].min()
date_max = daily_revenue_pd["order_date"].max()

# ── KPI Dashboard (text) ──────────────────────────────────────────────────────
print("=" * 65)
print("    BRIGHTCART RETAIL — 2024 BUSINESS PERFORMANCE SUMMARY")
print("=" * 65)
print(f"  Reporting Period    :  {date_min}  →  {date_max}")
print("-" * 65)
print(f"  Total Revenue       :  ${total_revenue:>12,.2f}")
print(f"  Total Orders        :  {total_orders:>12,}")
print(f"  Total Units Sold    :  {total_units:>12,}")
print(f"  Avg Order Value     :  ${avg_order_value:>12,.2f}")
print("-" * 65)
print(f"  Top Category        :  {top_category:<20}  ${top_category_rev:,.2f}")
print(f"  Top Customer        :  {top_customer_row['customer_name']:<20}  ${top_customer_row['customer_revenue']:,.2f}")
print(f"  Top Region          :  {top_region_row:<20}  ${top_region_rev:,.2f}")
print("=" * 65)

# COMMAND ----------

# DBTITLE 1,Section 2 — Revenue trend header
# MAGIC %md
# MAGIC ## 📈 Section 2 — Total Revenue Trend Over Time
# MAGIC
# MAGIC Daily revenue plotted across the full year to identify seasonal spikes, troughs, and growth patterns.

# COMMAND ----------

# DBTITLE 1,Revenue trend chart
import pandas as pd

df = daily_revenue_pd.copy()
df["order_date"] = pd.to_datetime(df["order_date"])
df = df.sort_values("order_date")

# 7-day rolling average for trend clarity
df["rolling_7d"] = df["daily_revenue"].rolling(window=7, min_periods=1).mean()

# Monthly aggregation for bar context
df["month"] = df["order_date"].dt.to_period("M")
monthly = df.groupby("month", as_index=False)["daily_revenue"].sum()
monthly["month_str"] = monthly["month"].astype(str)

fig, axes = plt.subplots(2, 1, figsize=(16, 10), gridspec_kw={"height_ratios": [2, 1]})
fig.suptitle("BrightCart — Revenue Trend (2024)", fontsize=16, fontweight="bold", y=1.01)

# Top: daily + 7-day rolling line
ax1 = axes[0]
ax1.fill_between(df["order_date"], df["daily_revenue"], alpha=0.18, color="#1f77b4")
ax1.plot(df["order_date"], df["daily_revenue"], color="#1f77b4", linewidth=0.8, alpha=0.6, label="Daily Revenue")
ax1.plot(df["order_date"], df["rolling_7d"],   color="#e74c3c", linewidth=2,   label="7-Day Rolling Avg")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax1.set_ylabel("Revenue (USD)")
ax1.set_title("Daily Revenue with 7-Day Rolling Average")
ax1.legend(loc="upper left")
ax1.set_xlim(df["order_date"].min(), df["order_date"].max())

# Bottom: monthly bar chart
ax2 = axes[1]
colors = plt.cm.Blues(np.linspace(0.45, 0.85, len(monthly)))
bars = ax2.bar(monthly["month_str"], monthly["daily_revenue"], color=colors, edgecolor="white", linewidth=0.5)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}K"))
ax2.set_ylabel("Revenue (USD)")
ax2.set_title("Monthly Revenue Total")
ax2.tick_params(axis="x", rotation=45)
# Annotate top month
top_idx = monthly["daily_revenue"].idxmax()
ax2.annotate(
    f"Peak\n${monthly.loc[top_idx, 'daily_revenue']:,.0f}",
    xy=(top_idx, monthly.loc[top_idx, "daily_revenue"]),
    xytext=(top_idx, monthly.loc[top_idx, "daily_revenue"] * 1.08),
    ha="center", fontsize=9, color="#c0392b",
    arrowprops=dict(arrowstyle="->", color="#c0392b", lw=1.5)
)

plt.tight_layout()
plt.show()
print(f"\nPeak daily revenue : ${df['daily_revenue'].max():,.2f}  on {df.loc[df['daily_revenue'].idxmax(), 'order_date'].strftime('%Y-%m-%d')}")
print(f"Peak monthly rev   : ${monthly['daily_revenue'].max():,.2f}  in {monthly.loc[top_idx, 'month_str']}")

# COMMAND ----------

# DBTITLE 1,Section 3 — Category performance header
# MAGIC %md
# MAGIC ## 📦 Section 3 — Revenue by Product Category
# MAGIC
# MAGIC Breaks down total revenue and units sold across the four product categories: Electronics, Office, Home, and Accessories.

# COMMAND ----------

# DBTITLE 1,Category performance chart
cat_summary = (
    category_pd
    .groupby("category", as_index=False)
    .agg(revenue=("revenue", "sum"), units_sold=("units_sold", "sum"), order_count=("order_count", "sum"))
    .sort_values("revenue", ascending=False)
)

PALETTE = ["#2ecc71", "#3498db", "#e67e22", "#9b59b6"]

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle("BrightCart — Category Performance (2024)", fontsize=15, fontweight="bold")

# Bar: Revenue by category
ax1 = axes[0]
bars = ax1.bar(cat_summary["category"], cat_summary["revenue"], color=PALETTE, edgecolor="white", linewidth=0.8)
for bar, val in zip(bars, cat_summary["revenue"]):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2000,
             f"${val/1000:.1f}K", ha="center", va="bottom", fontsize=10, fontweight="bold")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}K"))
ax1.set_title("Revenue by Category")
ax1.set_ylabel("Revenue (USD)")
ax1.set_ylim(0, cat_summary["revenue"].max() * 1.15)

# Bar: Units sold by category
ax2 = axes[1]
bars2 = ax2.bar(cat_summary["category"], cat_summary["units_sold"], color=PALETTE, edgecolor="white", linewidth=0.8)
for bar, val in zip(bars2, cat_summary["units_sold"]):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
             f"{val:,}", ha="center", va="bottom", fontsize=10, fontweight="bold")
ax2.set_title("Units Sold by Category")
ax2.set_ylabel("Units")
ax2.set_ylim(0, cat_summary["units_sold"].max() * 1.15)

# Pie: Revenue share
ax3 = axes[2]
wedges, texts, autotexts = ax3.pie(
    cat_summary["revenue"],
    labels=cat_summary["category"],
    autopct="%1.1f%%",
    colors=PALETTE,
    startangle=140,
    pctdistance=0.75,
    wedgeprops=dict(edgecolor="white", linewidth=2)
)
for at in autotexts:
    at.set_fontsize(11)
    at.set_fontweight("bold")
ax3.set_title("Revenue Share by Category")

plt.tight_layout()
plt.show()

# Tabular summary
print("\nCategory Revenue Summary:")
print(f"{'Category':<15} {'Revenue':>12}  {'Units Sold':>10}  {'Orders':>8}  {'Avg Rev/Order':>14}")
print("-" * 65)
for _, row in cat_summary.iterrows():
    print(f"{row['category']:<15} ${row['revenue']:>11,.2f}  {int(row['units_sold']):>10,}  {int(row['order_count']):>8,}  ${row['revenue']/row['order_count']:>13,.2f}")

# COMMAND ----------

# DBTITLE 1,Section 4 — Top products header
# MAGIC %md
# MAGIC ## 🏆 Section 4 — Top 10 Products by Revenue
# MAGIC
# MAGIC Ranks individual products by total revenue within each category. Identifies your highest-value SKUs.

# COMMAND ----------

# DBTITLE 1,Top 10 products chart
CATEGORY_COLORS = {"Electronics": "#3498db", "Office": "#2ecc71", "Home": "#e67e22", "Accessories": "#9b59b6"}

top10_products = category_pd.nlargest(10, "revenue")[["product_name", "category", "revenue", "units_sold"]].reset_index(drop=True)
top10_products["color"] = top10_products["category"].map(CATEGORY_COLORS)

fig, ax = plt.subplots(figsize=(14, 7))
bars = ax.barh(
    top10_products["product_name"],
    top10_products["revenue"],
    color=top10_products["color"],
    edgecolor="white",
    linewidth=0.8
)
for bar, val in zip(bars, top10_products["revenue"]):
    ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height() / 2,
            f"${val:,.0f}", va="center", fontsize=10)

ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.set_xlabel("Revenue (USD)")
ax.set_title("Top 10 Products by Revenue (2024)")
ax.invert_yaxis()
ax.set_xlim(0, top10_products["revenue"].max() * 1.18)

# Category legend
legend_patches = [mpatches.Patch(color=v, label=k) for k, v in CATEGORY_COLORS.items()]
ax.legend(handles=legend_patches, loc="lower right", title="Category", framealpha=0.9)

plt.tight_layout()
plt.show()

print("\nTop 10 Products Ranked:")
print(f"{'Rank':<5} {'Product':<30} {'Category':<14} {'Revenue':>12}  {'Units':>6}")
print("-" * 72)
for i, row in top10_products.iterrows():
    print(f"{i+1:<5} {row['product_name']:<30} {row['category']:<14} ${row['revenue']:>11,.2f}  {int(row['units_sold']):>6,}")

# COMMAND ----------

# DBTITLE 1,Section 5 — Region revenue header
# MAGIC %md
# MAGIC ## 🌍 Section 5 — Revenue by Region
# MAGIC
# MAGIC Compares total revenue, customer count, and revenue per customer across the four sales regions (North, South, East, West).

# COMMAND ----------

# DBTITLE 1,Region revenue chart
region_summary = (
    customer_region_pd
    .groupby("region", as_index=False)
    .agg(
        total_revenue=("customer_revenue", "sum"),
        customer_count=("customer_id", "nunique"),
        total_orders=("order_count", "sum")
    )
)
region_summary["rev_per_customer"] = region_summary["total_revenue"] / region_summary["customer_count"]
region_summary = region_summary.sort_values("total_revenue", ascending=False)

REGION_COLORS = ["#e74c3c", "#3498db", "#2ecc71", "#f39c12"]

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle("BrightCart — Revenue by Region (2024)", fontsize=15, fontweight="bold")

# Bar: Total revenue
ax1 = axes[0]
bars = ax1.bar(region_summary["region"], region_summary["total_revenue"], color=REGION_COLORS, edgecolor="white", linewidth=0.8)
for bar, val in zip(bars, region_summary["total_revenue"]):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1500,
             f"${val/1000:.1f}K", ha="center", fontsize=11, fontweight="bold")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1000:.0f}K"))
ax1.set_title("Total Revenue")
ax1.set_ylabel("Revenue (USD)")
ax1.set_ylim(0, region_summary["total_revenue"].max() * 1.15)

# Bar: Customer count
ax2 = axes[1]
bars2 = ax2.bar(region_summary["region"], region_summary["customer_count"], color=REGION_COLORS, edgecolor="white", linewidth=0.8)
for bar, val in zip(bars2, region_summary["customer_count"]):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
             str(int(val)), ha="center", fontsize=11, fontweight="bold")
ax2.set_title("Unique Customers")
ax2.set_ylabel("Customers")
ax2.set_ylim(0, region_summary["customer_count"].max() * 1.15)

# Bar: Rev per customer
ax3 = axes[2]
bars3 = ax3.bar(region_summary["region"], region_summary["rev_per_customer"], color=REGION_COLORS, edgecolor="white", linewidth=0.8)
for bar, val in zip(bars3, region_summary["rev_per_customer"]):
    ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
             f"${val:,.0f}", ha="center", fontsize=10, fontweight="bold")
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax3.set_title("Avg Revenue per Customer")
ax3.set_ylabel("Revenue / Customer (USD)")
ax3.set_ylim(0, region_summary["rev_per_customer"].max() * 1.15)

plt.tight_layout()
plt.show()

print("\nRegion Summary:")
print(f"{'Region':<10} {'Revenue':>12}  {'Customers':>10}  {'Orders':>8}  {'Rev/Customer':>14}")
print("-" * 60)
for _, row in region_summary.iterrows():
    print(f"{row['region']:<10} ${row['total_revenue']:>11,.2f}  {int(row['customer_count']):>10,}  {int(row['total_orders']):>8,}  ${row['rev_per_customer']:>13,.2f}")

# COMMAND ----------

# DBTITLE 1,Section 6 — Top customers header
# MAGIC %md
# MAGIC ## 👑 Section 6 — Top 10 Customers by Spend
# MAGIC
# MAGIC Identifies highest-value customers, their region, total spend, and order frequency. Key for retention and loyalty programs.

# COMMAND ----------

# DBTITLE 1,Top 10 customers chart
top10_customers = customer_region_pd.nlargest(10, "customer_revenue").reset_index(drop=True)
top10_customers["color"] = top10_customers["region"].map(
    {"North": "#e74c3c", "South": "#3498db", "West": "#2ecc71", "East": "#f39c12"}
)

fig, axes = plt.subplots(1, 2, figsize=(16, 7))
fig.suptitle("BrightCart — Top 10 Customers by Spend (2024)", fontsize=15, fontweight="bold")

# Horizontal bar: customer revenue
ax1 = axes[0]
bars = ax1.barh(
    top10_customers["customer_name"],
    top10_customers["customer_revenue"],
    color=top10_customers["color"],
    edgecolor="white",
    linewidth=0.8
)
for bar, val in zip(bars, top10_customers["customer_revenue"]):
    ax1.text(bar.get_width() + 30, bar.get_y() + bar.get_height() / 2,
             f"${val:,.0f}", va="center", fontsize=9.5)
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax1.set_xlabel("Total Spend (USD)")
ax1.set_title("Top 10 Customers by Revenue")
ax1.invert_yaxis()
ax1.set_xlim(0, top10_customers["customer_revenue"].max() * 1.2)

# Region legend
region_patches = [
    mpatches.Patch(color="#e74c3c", label="North"),
    mpatches.Patch(color="#3498db", label="South"),
    mpatches.Patch(color="#2ecc71", label="West"),
    mpatches.Patch(color="#f39c12", label="East"),
]
ax1.legend(handles=region_patches, loc="lower right", title="Region", framealpha=0.9)

# Scatter: revenue vs order_count
for _, row in top10_customers.iterrows():
    axes[1].scatter(row["order_count"], row["customer_revenue"],
                   s=120, color=row["color"], zorder=3, edgecolors="white", linewidths=0.8)
    axes[1].annotate(
        row["customer_name"].split()[0],  # first name for brevity
        (row["order_count"] + 0.05, row["customer_revenue"]),
        fontsize=8.5, va="center"
    )
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
axes[1].set_xlabel("Number of Orders")
axes[1].set_ylabel("Total Spend (USD)")
axes[1].set_title("Spend vs Order Frequency")
axes[1].legend(handles=region_patches, title="Region", framealpha=0.9)

plt.tight_layout()
plt.show()

print("\nTop 10 Customers:")
print(f"{'Rank':<5} {'Customer':<25} {'Region':<8} {'Spend':>12}  {'Orders':>7}  {'Units':>6}")
print("-" * 66)
for i, row in top10_customers.iterrows():
    print(f"{i+1:<5} {row['customer_name']:<25} {row['region']:<8} ${row['customer_revenue']:>11,.2f}  {int(row['order_count']):>7,}  {int(row['units_purchased']):>6,}")

# COMMAND ----------

# DBTITLE 1,Section 7 — Order volume analysis header
# MAGIC %md
# MAGIC ## 📦 Section 7 — Order Volume Analysis
# MAGIC
# MAGIC Examines how order frequency and units sold distribute across the year and by category.

# COMMAND ----------

# DBTITLE 1,Order volume chart
import pandas as pd

df_v = daily_revenue_pd.copy()
df_v["order_date"] = pd.to_datetime(df_v["order_date"])
df_v["month"]     = df_v["order_date"].dt.to_period("M")
df_v["month_str"] = df_v["month"].astype(str)
df_v["dow"]       = df_v["order_date"].dt.day_name()

monthly_orders = df_v.groupby("month_str", as_index=False)["order_count"].sum()
monthly_units  = df_v.groupby("month_str", as_index=False)["units_sold"].sum()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("BrightCart — Order Volume Analysis (2024)", fontsize=15, fontweight="bold")

# Monthly order count
ax1 = axes[0]
clrs = plt.cm.Greens(np.linspace(0.4, 0.85, len(monthly_orders)))
bars1 = ax1.bar(monthly_orders["month_str"], monthly_orders["order_count"], color=clrs, edgecolor="white")
for bar, val in zip(bars1, monthly_orders["order_count"]):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
             str(int(val)), ha="center", fontsize=9)
ax1.set_title("Monthly Order Count")
ax1.set_ylabel("Orders")
ax1.tick_params(axis="x", rotation=45)

# Monthly units sold
ax2 = axes[1]
clrs2 = plt.cm.Oranges(np.linspace(0.4, 0.85, len(monthly_units)))
bars2 = ax2.bar(monthly_units["month_str"], monthly_units["units_sold"], color=clrs2, edgecolor="white")
for bar, val in zip(bars2, monthly_units["units_sold"]):
    ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
             str(int(val)), ha="center", fontsize=9)
ax2.set_title("Monthly Units Sold")
ax2.set_ylabel("Units")
ax2.tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Section 8 — Region × Category heatmap header
# MAGIC %md
# MAGIC ## 🔥 Section 8 — Region × Category Revenue Heatmap
# MAGIC
# MAGIC Shows which combinations of region and product category drive the most revenue. Highlights cross-sell and regional marketing opportunities.

# COMMAND ----------

# DBTITLE 1,Region x Category heatmap
from pyspark.sql import functions as F

# Join customer_region with category_performance via silver table for region+category combo
silver_df = spark.table(f"{CATALOG}.{SCHEMA}.silver_enriched_orders").filter(~F.col("is_cancelled"))
region_cat_df = (
    silver_df
    .groupBy("region", "category")
    .agg(F.sum("total_amount").alias("revenue"))
    .toPandas()
)

# Pivot for heatmap
heatmap_data = region_cat_df.pivot(index="region", columns="category", values="revenue").fillna(0)
heatmap_data = heatmap_data.loc[["North", "South", "East", "West"]]

fig, ax = plt.subplots(figsize=(10, 5))
im = ax.imshow(heatmap_data.values, cmap="YlOrRd", aspect="auto")

ax.set_xticks(range(len(heatmap_data.columns)))
ax.set_xticklabels(heatmap_data.columns, fontsize=12)
ax.set_yticks(range(len(heatmap_data.index)))
ax.set_yticklabels(heatmap_data.index, fontsize=12)
ax.set_title("Revenue Heatmap: Region × Category (USD)", fontsize=14)

plt.colorbar(im, ax=ax, label="Revenue (USD)")

# Annotate each cell
for i in range(len(heatmap_data.index)):
    for j in range(len(heatmap_data.columns)):
        val = heatmap_data.values[i, j]
        text_color = "white" if val > heatmap_data.values.max() * 0.6 else "black"
        ax.text(j, i, f"${val/1000:.1f}K", ha="center", va="center",
                fontsize=11, fontweight="bold", color=text_color)

plt.tight_layout()
plt.show()

print("\nRegion × Category Revenue Matrix ($K):")
print(heatmap_data.applymap(lambda x: f"${x/1000:.1f}K").to_string())

# COMMAND ----------

# DBTITLE 1,Section 9 — Business insights & recommendations header
# MAGIC %md
# MAGIC ## 💡 Section 9 — Business Insights & Strategic Recommendations
# MAGIC
# MAGIC Summary of actionable findings derived from the data analysis above.

# COMMAND ----------

# DBTITLE 1,Insights narrative
# Generate dynamic narrative from actual data
cat_summary_i = category_pd.groupby("category")["revenue"].sum().sort_values(ascending=False)
top_cat       = cat_summary_i.index[0]
bottom_cat    = cat_summary_i.index[-1]

region_rev_i  = customer_region_pd.groupby("region")["customer_revenue"].sum().sort_values(ascending=False)
top_reg       = region_rev_i.index[0]
bottom_reg    = region_rev_i.index[-1]

region_cust_i = customer_region_pd.groupby("region")["customer_id"].nunique()
bottom_reg_cust = region_cust_i[bottom_reg]
top_reg_cust    = region_cust_i[top_reg]

top_cust_name = customer_region_pd.nlargest(1, "customer_revenue").iloc[0]["customer_name"]
top_cust_rev  = customer_region_pd.nlargest(1, "customer_revenue").iloc[0]["customer_revenue"]

print("=" * 65)
print("  BRIGHTCART 2024 — KEY BUSINESS INSIGHTS & RECOMMENDATIONS")
print("=" * 65)

print(f"""
INSIGHT 1 — REVENUE IS HEALTHY AND STEADY
  Total 2024 revenue: ${total_revenue:,.2f} across {int(total_orders):,} orders.
  The daily trend shows no major collapses or spikes, indicating
  a stable customer base with consistent demand year-round.
  ACTION: Investigate whether seasonal promotions could create
  controlled revenue peaks to grow the top line.

INSIGHT 2 — ELECTRONICS LEADS IN REVENUE DESPITE OFFICE WINNING ON VOLUME
  {top_cat} generates the highest revenue (${cat_summary_i[top_cat]:,.0f}) while
  Office has the most units sold. This confirms Electronics carries
  a higher unit price and drives disproportionate revenue value.
  ACTION: Protect Electronics margin. Consider bundling Electronics
  with Accessories (the lowest-revenue category) to lift Accessory AOV.

INSIGHT 3 — NORTH IS THE TOP REGION; EAST HAS AN EFFICIENCY GAP
  {top_reg} region leads in total revenue (${region_rev_i[top_reg]:,.0f}) with
  {int(top_reg_cust)} customers. {bottom_reg} generates the least revenue
  (${region_rev_i[bottom_reg]:,.0f}) despite having {int(bottom_reg_cust)} customers —
  the largest customer count of any region.
  ACTION: Run a targeted Electronics cross-sell campaign in {bottom_reg}.
  Even a 10% uplift in {bottom_reg} revenue-per-customer would add ~${region_rev_i[bottom_reg]*0.10:,.0f}.

INSIGHT 4 — TOP CUSTOMERS ARE HIGH-VALUE BUT FEW-ORDER
  Top customer {top_cust_name} spent ${top_cust_rev:,.2f} in 2024 but placed
  only a small number of large-value orders. Most top-10 customers
  show the same pattern: high spend, low order frequency.
  ACTION: Introduce a loyalty / subscription programme to increase
  purchase frequency for these high-value customers.

INSIGHT 5 — NORTH DOMINATES THE TOP CUSTOMER LIST
  3 of the top 5 customers by spend are in North, reinforcing
  North’s revenue leadership at both the aggregate and individual level.
  ACTION: Replicate the North customer engagement model
  (promotions, product mix, support) in West and South.
""")
print("=" * 65)

# COMMAND ----------

# DBTITLE 1,Export HTML report to DBFS Volume
# ── Export a self-contained HTML report ─────────────────────────────────────────────────
from datetime import datetime
import base64, io

def fig_to_base64(fig):
    """Convert a matplotlib figure to a base64 PNG string for embedding in HTML."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def make_chart_daily_revenue():
    import pandas as pd
    df = daily_revenue_pd.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.sort_values("order_date")
    df["rolling_7d"] = df["daily_revenue"].rolling(7, min_periods=1).mean()
    fig, ax = plt.subplots(figsize=(13, 4))
    ax.fill_between(df["order_date"], df["daily_revenue"], alpha=0.18, color="#1f77b4")
    ax.plot(df["order_date"], df["daily_revenue"], color="#1f77b4", linewidth=0.8, alpha=0.6, label="Daily Revenue")
    ax.plot(df["order_date"], df["rolling_7d"],   color="#e74c3c", linewidth=2,   label="7-Day Rolling Avg")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Daily Revenue with 7-Day Rolling Average")
    ax.legend()
    ax.set_facecolor("#f9f9f9")
    fig.patch.set_facecolor("white")
    plt.tight_layout()
    return fig

def make_chart_category():
    cs = category_pd.groupby("category", as_index=False).agg(revenue=("revenue","sum"), units_sold=("units_sold","sum"))
    cs = cs.sort_values("revenue", ascending=False)
    PALETTE = ["#2ecc71","#3498db","#e67e22","#9b59b6"]
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    axes[0].bar(cs["category"], cs["revenue"],    color=PALETTE, edgecolor="white")
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x/1000:.0f}K"))
    axes[0].set_title("Revenue by Category")
    axes[1].pie(cs["revenue"], labels=cs["category"], autopct="%1.1f%%", colors=PALETTE,
                startangle=140, wedgeprops=dict(edgecolor="white"))
    axes[1].set_title("Revenue Share")
    for a in axes: a.set_facecolor("#f9f9f9")
    fig.patch.set_facecolor("white")
    plt.tight_layout()
    return fig

def make_chart_top_products():
    tp = category_pd.nlargest(10, "revenue")[["product_name","category","revenue"]].reset_index(drop=True)
    CCAT = {"Electronics":"#3498db","Office":"#2ecc71","Home":"#e67e22","Accessories":"#9b59b6"}
    fig, ax = plt.subplots(figsize=(13, 6))
    ax.barh(tp["product_name"], tp["revenue"], color=tp["category"].map(CCAT), edgecolor="white")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.set_title("Top 10 Products by Revenue")
    ax.invert_yaxis()
    ax.set_facecolor("#f9f9f9")
    fig.patch.set_facecolor("white")
    plt.tight_layout()
    return fig

def make_chart_region():
    rs = customer_region_pd.groupby("region", as_index=False).agg(
        total_revenue=("customer_revenue","sum"), customer_count=("customer_id","nunique"))
    rs["rpc"] = rs["total_revenue"] / rs["customer_count"]
    rs = rs.sort_values("total_revenue", ascending=False)
    RC = ["#e74c3c","#3498db","#2ecc71","#f39c12"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, col, title, fmt in zip(axes,
        ["total_revenue","customer_count","rpc"],
        ["Total Revenue","Customers","Rev / Customer"],
        [lambda x,_: f"${x/1000:.0f}K", lambda x,_: f"{x:.0f}", lambda x,_: f"${x:,.0f}"]):
        ax.bar(rs["region"], rs[col], color=RC, edgecolor="white")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt))
        ax.set_title(title)
        ax.set_facecolor("#f9f9f9")
    fig.patch.set_facecolor("white")
    plt.tight_layout()
    return fig

def make_chart_customers():
    tc = customer_region_pd.nlargest(10, "customer_revenue").reset_index(drop=True)
    CR = {"North":"#e74c3c","South":"#3498db","West":"#2ecc71","East":"#f39c12"}
    fig, ax = plt.subplots(figsize=(13, 6))
    ax.barh(tc["customer_name"], tc["customer_revenue"], color=tc["region"].map(CR), edgecolor="white")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.set_title("Top 10 Customers by Spend")
    ax.invert_yaxis()
    ax.set_facecolor("#f9f9f9")
    fig.patch.set_facecolor("white")
    plt.tight_layout()
    return fig

# Build HTML
generated_on = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
charts = [
    ("Daily Revenue Trend",        make_chart_daily_revenue()),
    ("Category Performance",       make_chart_category()),
    ("Top 10 Products",            make_chart_top_products()),
    ("Revenue by Region",          make_chart_region()),
    ("Top 10 Customers by Spend",  make_chart_customers()),
]

charts_html = ""
for title, fig in charts:
    b64 = fig_to_base64(fig)
    plt.close(fig)
    charts_html += f"""
    <div class="section">
      <h2>{title}</h2>
      <img src="data:image/png;base64,{b64}" style="max-width:100%;border-radius:6px;box-shadow:0 2px 8px rgba(0,0,0,.12);">
    </div>"""

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>BrightCart Business Report 2024</title>
<style>
  body {{font-family:'Segoe UI',Arial,sans-serif;margin:0;padding:0;background:#f4f6f9;color:#333;}}
  .header {{background:linear-gradient(135deg,#1a237e,#283593);color:#fff;padding:40px 60px;margin-bottom:30px;}}
  .header h1 {{margin:0 0 6px;font-size:2.2em;}}
  .header p  {{margin:0;opacity:.85;font-size:1.05em;}}
  .kpi-grid {{display:flex;flex-wrap:wrap;gap:18px;padding:0 60px 30px;}}
  .kpi {{background:#fff;border-radius:10px;padding:20px 28px;min-width:180px;flex:1;
         box-shadow:0 2px 8px rgba(0,0,0,.08);text-align:center;}}
  .kpi .value {{font-size:1.8em;font-weight:700;color:#1a237e;}}
  .kpi .label {{font-size:.9em;color:#666;margin-top:4px;}}
  .section {{background:#fff;border-radius:10px;padding:28px 40px;margin:0 60px 28px;
             box-shadow:0 2px 8px rgba(0,0,0,.08);}}
  .section h2 {{color:#1a237e;margin-top:0;border-bottom:2px solid #e8eaf6;padding-bottom:10px;}}
  .insights {{background:#e8f5e9;border-left:5px solid #2e7d32;padding:16px 22px;border-radius:0 8px 8px 0;
              margin-top:0;}}
  .insights h2 {{color:#1b5e20;border-color:#a5d6a7;}}
  .insights ul {{line-height:1.9;margin:0;padding-left:20px;}}
  .footer {{text-align:center;padding:24px;color:#999;font-size:.85em;}}
</style>
</head>
<body>
<div class="header">
  <h1>🛒 BrightCart Retail Analytics — 2024 Performance Report</h1>
  <p>Generated on {generated_on} · Data source: gold_daily_revenue, gold_category_performance, gold_customer_region_summary</p>
</div>

<div class="kpi-grid">
  <div class="kpi"><div class="value">${total_revenue:,.0f}</div><div class="label">Total Revenue</div></div>
  <div class="kpi"><div class="value">{int(total_orders):,}</div><div class="label">Total Orders</div></div>
  <div class="kpi"><div class="value">{int(total_units):,}</div><div class="label">Units Sold</div></div>
  <div class="kpi"><div class="value">${avg_order_value:,.0f}</div><div class="label">Avg Order Value</div></div>
  <div class="kpi"><div class="value">{top_category}</div><div class="label">Top Category</div></div>
  <div class="kpi"><div class="value">{top_region_row}</div><div class="label">Top Region</div></div>
</div>

{charts_html}

<div class="section insights">
  <h2>💡 Key Insights & Recommendations</h2>
  <ul>
    <li><strong>Revenue is stable year-round</strong> with no major seasonal crashes. Consider promotional events to create growth peaks.</li>
    <li><strong>Electronics leads revenue</strong> despite Office having more units sold — protect Electronics margin and bundle with Accessories to lift Accessory AOV.</li>
    <li><strong>East region has an efficiency gap</strong>: most customers, lowest revenue. A targeted Electronics cross-sell campaign could unlock ~${region_rev_i.get('East', 0) * 0.10:,.0f} in incremental revenue.</li>
    <li><strong>Top customers are high-value but low-frequency</strong>. A loyalty programme to increase purchase frequency would yield outsized returns.</li>
    <li><strong>North dominates</strong> at both regional and individual customer level. Replicate North engagement strategy in West and South.</li>
  </ul>
</div>

<div class="footer">BrightCart Retail Analytics Pipeline · Databricks Capstone Project · {generated_on}</div>
</body>
</html>"""

# Write to workspace
report_path = "/Workspace/Users/harpalsingh031@gmail.com/brightcart-capstone/BRIGHTCART_REPORT_2024.html"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"HTML report written to: {report_path}")
print(f"File size: {len(html_content):,} bytes")
