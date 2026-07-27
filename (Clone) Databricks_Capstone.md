## Databricks Capstone Project

## Retail Order Analytics Pipeline

A Beginner-Friendly Hands-On Case Study | Duration: 1 Week (5 Working Days)

Covers: Batch ETL • Delta Lake CRUD • Auto Loader • Structured Streaming • CLI • CI/CD

## 1. Overview

This capstone project simulates a small retail company that wants a simple, reliable way to understand its daily sales performance. As a newly onboarded Data Engineer, you will build a beginner-level ETL pipeline on Databricks that ingests raw order data, cleans and enriches it, and produces summary tables that a business analyst could use to answer basic questions like “which products sell best?” and “which region generates the most revenue?”

This project covers the full journey from a basic batch pipeline through to Auto Loader, Structured Streaming, and CLI/CI-CD deployment — compressed into five working days by pairing a foundational topic with an automation/streaming topic each day. All the original scope is intact; nothing has been cut, only re-sequenced. Because of this, expect each day to run closer to 6-7 focused hours rather than 3-4 — this is an intensive, accelerated version of the project, better suited to a trainee who can dedicate most of the day to it.

## 2. Learning Objectives

By the end of this project, you will be able to:

- Read CSV files into Spark DataFrames and inspect their schema.

- Clean data by handling nulls, duplicates, and incorrect data types.

- Create and query Delta Lake tables using both PySpark and Spark SQL.

- Perform basic CRUD operations (INSERT, UPDATE, DELETE, MERGE) on a Delta table.

- Join multiple tables and derive new columns to enrich data.

- Aggregate data to produce simple, business-relevant summary tables.

- Chain multiple notebooks together into a single Databricks Job (Workflow).

- Incrementally ingest new files using Databricks Auto Loader (cloudFiles), instead of re-reading an entire directory.

- Build a simple Spark Structured Streaming pipeline with checkpointing and an upsert (MERGE) sink.

- Install, configure, and use the Databricks CLI to manage files, clusters, and jobs.

- Define a pipeline as code using a Databricks Asset Bundle (databricks.yml) and deploy it via the CLI.

- Set up a basic CI/CD workflow (GitHub Actions) that validates, deploys, and runs the pipeline automatically.

## 3. Business Scenario

“BrightCart” is a small online retail company selling electronics and accessories. The operations team currently tracks orders in flat CSV exports and has no automated way to see sales trends. You have been asked to build a simple pipeline that turns these raw exports into clean, query-ready tables so the business can answer:

- What is our total revenue, and how does it trend over time?

- Which product categories and individual products sell the best?

- Who are our top customers by spend?

- How much revenue comes from each region?

## 4. Dataset

You will work with three small CSV files. A ready-to-run data generation script is provided in the Appendix so you can create these files yourself in under a minute — no external download required.


## 4.1 customers.csv

| Column | Type | Description |
| --- | --- | --- |
| customer_id | Integer | Unique identifier for the customer |
| customer_name | String | Full name of the customer |
| region | String | One of: North, South, East, West |
| signup_date | Date | Date the customer created their account |

## 4.2 products.csv

| Column | Type | Description |
| --- | --- | --- |
| product_id | Integer | Unique identifier for the product |
| product_name | String | Name of the product |
| category | String | One of: Electronics, Accessories, Home, Office |
| unit_price | Double | Price per unit in USD |

## 4.3 orders.csv

| Column | Type | Description |
| --- | --- | --- |
| order_id | Integer | Unique identifier for the order |
| customer_id | Integer | Foreign key referencing customers |
| product_id | Integer | Foreign key referencing products |
| quantity | Integer | Number of units ordered |
| order_date | Date | Date the order was placed |
| status | String | One of: COMPLETED, CANCELLED, PENDING |

## 5. Environment & Prerequisites

- Access to a Databricks Community Edition or trial workspace.

- A running all-purpose cluster (single-node is sufficient) with a recent Databricks Runtime (e.g. 14.x or 15.x LTS).

- Basic familiarity with Python and SQL syntax (covered in prior training modules).

- A Unity Catalog Volume or DBFS location to upload the three CSV files.

- A local machine (or Databricks-provided terminal) to install the Databricks CLI, plus a personal access token for authentication.

- A free GitHub account and a small repository to hold the notebooks, the Asset Bundle definition, and the CI/CD workflow file (needed from Day 3 onward).

## 6. Final Deliverables Checklist

## Batch Pipeline:

- 01_explore, 02_bronze_ingestion, 03_silver_enrichment, and 04_gold_aggregation notebooks.

- Three Bronze Delta tables, one Silver Delta table, and 2–3 Gold Delta tables.


- A working Databricks Job chaining all three notebooks.

- A 1-page written summary of business insights and one recommendation.

## Streaming, Auto Loader & CI/CD:

- 05_autoloader_ingestion and 06_streaming_silver_gold notebooks.

- A working Auto Loader stream writing to bronze_orders_stream with a valid checkpoint location.

- A working Structured Streaming job upserting into silver_enriched_orders_stream.

- A Git repository containing all notebooks, a validated databricks.yml Asset Bundle, and a working GitHub Actions CI/CD workflow.

- A final 1-2 page combined summary covering both the batch and streaming/automation phases.

## 7. Evaluation Rubric

| Criterion | Weight | What Is Assessed |
| --- | --- | --- |
| Data Ingestion & Cleaning (Batch) | 15% | Correct schema handling, null/duplicate handling, Bronze Delta tables created. |
| Transformations & CRUD | 15% | Correct joins, derived columns, and use of INSERT/UPDATE/DELETE/MERGE on Delta tables. |
| Aggregation & Insights | 15% | Accuracy of Gold-layer aggregations and quality/relevance of business insights. |
| Streaming & Auto Loader | 20% | Correct incremental ingestion via cloudFiles, valid checkpointing, correct foreachBatch/MERGE upsert logic. |
| CLI & CI/CD Deployment | 20% | Working CLI profile, valid Asset Bundle deployed via CLI, and a functioning GitHub Actions workflow that deploys/runs the pipeline. |
| Code Quality & Documentation | 15% | Readable code, meaningful comments/markdown cells, clear final summary. |

## 8. Optional Stretch Goals

- Add a fourth CSV file (e.g. returns.csv) and incorporate it into both the batch and streaming pipeline.

- Use MERGE INTO instead of separate UPDATE/DELETE statements in the Silver layer.

- Add a simple data quality check task to the Job that fails the run if bronze_orders has zero rows.

- Visualize one Gold table using a Databricks SQL dashboard or a notebook chart.

- Switch the Auto Loader source from Directory Listing to File Notification mode and compare behavior.

- Add a CI/CD step that runs a basic notebook unit test before deploying the bundle.

## 9. Now we need to export report from golden layer 
