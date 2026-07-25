# BrightCart Capstone — Final Combined Summary

## Batch Pipeline (Days 1–3)

Built a medallion-architecture batch pipeline: `01_explore` profiled the raw CSVs (0 nulls, 0 duplicate keys, all categorical values as expected); `02_bronze_ingestion` loaded `customers`/`products`/`orders` into explicit-schema Bronze Delta tables with ingestion metadata; `03_silver_enrichment` cleaned/deduplicated and joined them into `silver_enriched_orders`, demonstrating Delta `INSERT`, `UPDATE`, `DELETE`, and `MERGE`; `04_gold_aggregation` produced `gold_daily_revenue`, `gold_category_performance`, and `gold_customer_region_summary`. See [BUSINESS_INSIGHTS.md](BUSINESS_INSIGHTS.md) for the resulting analysis. All three notebooks were chained into a single Databricks Job (`BrightCart Batch Pipeline`) with task dependencies and a shared job cluster.

## Streaming & Automation (Day 4)

`05_autoloader_ingestion` used Auto Loader (`cloudFiles`, directory listing mode) to incrementally ingest new order files into `bronze_orders_stream`, with a dedicated checkpoint and schema-tracking location. `06_streaming_silver_gold` streamed from that table, joined against the Bronze dimension tables, and used `foreachBatch` + `MERGE INTO` to upsert into `silver_enriched_orders_stream`, keyed on `order_id`. Idempotency was explicitly validated: after cleaning up legacy duplicate rows introduced by an early data-generation bug, a duplicate-count check on `order_id` returned zero rows.

## CI/CD & Deployment (Day 5)

The project is defined as a Databricks Asset Bundle (`databricks.yml`) with `catalog_name`/`schema_name` variables wired into each notebook task's `base_parameters`, and each notebook reads them via `dbutils.widgets` — so the same code can target a different catalog/schema without modification. A GitHub Actions workflow (`.github/workflows/deploy.yml`) installs the Databricks CLI, validates, deploys, and runs the bundle on every push to `main` (or manually via `workflow_dispatch`, with catalog/schema override inputs). The deployed job (`BrightCart Batch Pipeline`) ran end-to-end and terminated `SUCCESS`, producing all Bronze/Silver/Gold tables from a clean CI-driven deploy.

## Lessons Learned

* **`input_file_name()` is blocked by Unity Catalog** on Standard/Serverless compute — use `_metadata.file_path` instead.
* **`workspace.host` in `databricks.yml` does not support variable interpolation** for authentication fields — must come from the `DATABRICKS_HOST` environment variable at run time.
* **The legacy `pip install databricks-cli` has no `bundle` command** — CI must install the current CLI via the `databricks/setup-cli` GitHub Action.
* **Local notebook references in a bundle need explicit file extensions** (`.py`) when the notebook source lives in a Git folder rather than referencing an existing workspace notebook path.
* **Serverless jobs may not be enabled workspace-wide** — a `job_clusters` block with an explicit `new_cluster` spec is a portable fallback.
* **A repeatable data-generation bug** (always defaulting `start_order_id` to the same value) silently created duplicate `order_id`s across incremental batches — caught via an explicit duplicate-count validation step, reinforcing the value of idempotency checks after every streaming upsert.
