# BrightCart Retail Order Analytics — Capstone Project

A Databricks capstone demonstrating the medallion architecture (Bronze/Silver/Gold), Delta Lake CRUD, incremental ingestion with Auto Loader, structured streaming with `foreachBatch` MERGE upserts, and CI/CD via Databricks Asset Bundles + GitHub Actions.

## Structure

* `notebooks/00_data_generation` — generates synthetic `customers`, `products`, `orders` CSVs into a Unity Catalog Volume (Faker-based), plus an incremental order-batch helper for streaming tests.
* `notebooks/01_explore` — schema, null, duplicate, and categorical-value profiling of the raw data.
* `notebooks/02_bronze_ingestion` — reads raw CSVs with explicit schemas into `bronze_customers`, `bronze_products`, `bronze_orders` Delta tables.
* `notebooks/03_silver_enrichment` — cleans/dedupes Bronze data, joins into `silver_enriched_orders`, and demonstrates Delta `INSERT`/`UPDATE`/`DELETE`/`MERGE`.
* `notebooks/04_gold_aggregation` — aggregates Silver (excluding cancelled orders) into `gold_daily_revenue`, `gold_category_performance`, `gold_customer_region_summary`.
* `notebooks/05_autoloader_ingestion` — incrementally ingests new order files via Auto Loader (`cloudFiles`) into `bronze_orders_stream`.
* `notebooks/06_streaming_silver_gold` — streams from `bronze_orders_stream`, joins dimension tables, and upserts into `silver_enriched_orders_stream` via `foreachBatch` + `MERGE`, keyed on `order_id`.

## Unity Catalog

All tables live under the `harpalsingh.brightcart` schema. Raw files and streaming checkpoints live in the `/Volumes/harpalsingh/brightcart/raw_data` and `/Volumes/harpalsingh/brightcart/checkpoints` volumes.

## Deployment (Databricks Asset Bundle)

```
databricks bundle validate --var="databricks_host=<workspace-url>"
databricks bundle deploy --target dev --var="databricks_host=<workspace-url>"
databricks bundle run brightcart_batch_pipeline --target dev --var="databricks_host=<workspace-url>"
```

## CI/CD

[.github/workflows/deploy.yml](.github/workflows/deploy.yml) validates and deploys the bundle, then runs the `brightcart_batch_pipeline` job on every push to `main`. Requires the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` repository secrets.
