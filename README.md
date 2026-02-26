Nuroil Data Engineering Pipeline – Project Summary

Project Overview

This project demonstrates a complete end-to-end PySpark-based data engineering pipeline for a fictional fuel company called NurOil.
The goal was to extract raw CSV data, clean it, transform it, model it, and prepare it for analytics and Power BI dashboards.

The pipeline is automated using a custom Python pipeline runner instead of Airflow.

Project Architecture
1. Raw Data → PySpark Ingestion

Raw datasets:

stations.csv

customers.csv

sales.csv

inventory.csv

invoices.csv

These files are loaded into PySpark DataFrames.

2. Data Cleaning

Each dataset goes through:

column renaming

null-handling

data type correction

deduplication

correcting inconsistent values (fuel types, invoice statuses, etc.)

Output saved as:

data/cleaned/*.parquet

3. Data Transformation

Business logic added:

calculate totals

normalize fuel types

ensure foreign key relations

map stations to regions

unify formats

Output saved as transformed Parquet files.

4. Data Modeling (Star Schema)

A simple analytical schema was created:

Fact Table

fact_sales

Dimensions

dim_stations

dim_customers

dim_inventory

dim_invoices

These tables were built in SQLite for lightweight analytics.

5. Python Pipeline Runner

Instead of Airflow, I created a custom orchestration script:

python_pipeline_runner.py


This runner executes the pipeline in order:

ingest

clean

transform

model

export

Each step prints logs and status updates — similar to a simplified Airflow DAG but without the complexity.

6. Export for Power BI

Final models were exported in two formats:

✔ Parquet (analytics-ready)
✔ CSV (for Power BI import)

I also created a single combined CSV file containing all cleaned tables, useful for quick visualizations.

Project Status

The pipeline is fully implemented:

✔ Ingestion
✔ Cleaning
✔ Transformation
✔ SQLite modeling
✔ CSV + Parquet export
✔ Python pipeline runner working end-to-end



This project will be further enhanced. One of the first features will be using Airflow instead of sole python script for orchestration

Thank you for taking time to review the project.

If you have any enhancement comments, or any general inquiries about the project please don't hesitate to contact.
