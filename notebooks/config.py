"""Shared notebook configuration helper for Databricks notebooks.

Provides a `get_config()` function that reads `dbutils` widgets when
running in Databricks, or falls back to environment defaults otherwise.
"""
from typing import Dict


def get_config(catalog_default: str = "harpalsingh", schema_default: str = "brightcart") -> Dict[str, str]:
    try:
        # Databricks environment: ensure widgets exist and read them
        dbutils.widgets.text("catalog", catalog_default)
        dbutils.widgets.text("schema", schema_default)
        catalog = dbutils.widgets.get("catalog")
        schema = dbutils.widgets.get("schema")
    except Exception:
        # Non-Databricks execution (tests / local): use environment variables
        import os

        catalog = os.getenv("CATALOG", catalog_default)
        schema = os.getenv("SCHEMA", schema_default)

    raw_volume = f"/Volumes/{catalog}/{schema}/raw_data"
    checkpoint_volume = f"/Volumes/{catalog}/{schema}/checkpoints"

    return {
        "catalog": catalog,
        "schema": schema,
        "raw_volume": raw_volume,
        "checkpoint_volume": checkpoint_volume,
        "customers_path": f"{raw_volume}/customers.csv",
        "products_path": f"{raw_volume}/products.csv",
        "orders_path": f"{raw_volume}/orders.csv",
        "incoming_orders_dir": f"{raw_volume}/incoming_orders",
        "schema_tracking_dir": f"{checkpoint_volume}/schema_tracking",
    }
