# ✈️ Flights Data Engineering Pipeline — Databricks & dbt

## 📌 Project Overview

This project focuses on building an automated data engineering pipeline using **Databricks and dbt** to transform flight-related data into analytics-ready datasets.

The pipeline follows a **Bronze–Silver–Gold architecture** and implements metadata-driven dynamic dimension and fact tables, incremental data loading, CDC-based processing, and automated upserts. It is designed to process new and updated records efficiently without requiring a complete reload of the data every time.

## 🎯 Project Objectives

- Build a structured data pipeline using the Medallion Architecture.
- Transform raw data into clean, structured datasets using Databricks and dbt.
- Create reusable, metadata-driven dimension and fact tables.
- Implement incremental loading to process newly arriving and updated records.
- Generate SQL queries dynamically using metadata configurations.
- Automate data processing through a pipeline that handles new incoming data.
- Maintain consistent relationships between fact and dimension tables using business keys and surrogate keys.

## 🏗️ Architecture

The project follows the Bronze–Silver–Gold Medallion Architecture.

```text
        Source Flight Data
                |
                v
       +------------------+
       |   Bronze Layer   |
       |   Raw Data       |
       +------------------+
                |
                v
       +------------------+
       |   Silver Layer   |
       | Cleaned and      |
       | Transformed Data |
       +------------------+
                |
                v
       +----------------------+
       | Metadata-Driven      |
       | Transformations      |
       | Databricks + dbt     |
       +----------------------+
                |
                v
       +----------------------+
       |     Gold Layer       |
       | Dynamic Dimensions   |
       | Dynamic Fact Tables  |
       +----------------------+
                |
                v
       Incremental Loading
       and Automated Upserts
```

## 🥉 Bronze Layer

The Bronze layer serves as the raw data layer.

- Stores incoming source data for downstream processing.
- Provides the initial layer for the data transformation workflow.
- Acts as the starting point for processing flight, booking, customer, and related datasets.

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed data.

- Processes source datasets into structured tables.
- Prepares data for downstream analytical modelling.
- Provides transformed datasets for building dimension and fact tables.
- Supports incremental processing using change-tracking timestamps.

## 🥇 Gold Layer

The Gold layer contains analytics-ready dimension and fact tables.

### Dynamic Dimension Tables

Implemented metadata-driven dimension tables to make dimension creation and processing reusable across multiple datasets.

Key implementations:

- Defined metadata for source and target tables.
- Configured business key columns and CDC timestamp columns.
- Generated surrogate keys for dimension records.
- Maintained creation and update timestamps.
- Implemented incremental processing and upsert logic.
- Reused common processing logic instead of manually writing separate transformations for every dimension.

Example dimension tables include:

- `dimcustomers`
- `dimflights`
- `dimbookings`

### Dynamic Fact Tables

Developed a metadata-driven approach for creating and updating fact tables.

Key implementations:

- Configured fact columns and source/target table details.
- Defined dimension tables and their join keys through metadata.
- Generated SQL queries dynamically using Python.
- Joined fact source data with dimension tables using business keys.
- Retrieved dimension surrogate keys for analytical relationships.
- Implemented incremental loading and Delta Lake `MERGE` operations for upserts.

Example fact table:

- `fact_bookings`

The fact table combines booking-related information with the corresponding customer and flight dimension keys.

## ⚡ Incremental Data Loading

Implemented incremental loading to process only new or updated records instead of reprocessing the entire dataset on every run.

The process includes:

1. Identify the target table using metadata.
2. Retrieve the last processed timestamp from the target table.
3. Filter source records using the CDC timestamp.
4. Generate the incremental SQL query dynamically.
5. Transform and enrich records with dimension keys.
6. Insert new records and update existing records using upsert logic.

This approach reduces unnecessary data processing and supports efficient pipeline execution.

## 🔄 Dynamic SQL Generation

Created a reusable Python function to generate SQL queries from metadata configurations.

The metadata defines:

- Source and target tables.
- Fact columns.
- CDC column.
- Dimension table names and aliases.
- Join keys.
- Dimension surrogate keys.

The function uses this information to build the `SELECT`, `JOIN`, and incremental `WHERE` clauses dynamically.

This reduces repetitive SQL code and makes the pipeline easier to extend to additional fact tables.

## 🔁 Automated Pipeline Processing

Developed a pipeline workflow to process incoming data incrementally.

The workflow identifies records that need processing, applies the required transformations, resolves dimension keys, and loads the results into the Gold layer.

The pipeline supports:

- Reusable metadata-driven processing.
- Incremental data ingestion.
- Automated insert and update operations.
- Processing of newly arriving records.
- Scalable transformation logic across multiple datasets.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Databricks** | Data processing and lakehouse environment |
| **dbt** | SQL-based data transformations and modelling |
| **PySpark** | DataFrame processing and Python-based transformations |
| **Python** | Metadata configuration and dynamic SQL generation |
| **SQL** | Data querying, joins, and transformations |
| **Delta Lake** | Storage and `MERGE`-based upsert operations |
| **Unity Catalog** | Data and table organization within Databricks |

## 💡 Key Learnings

Through this project, I worked on:

- Implementing the Medallion Architecture.
- Building metadata-driven data engineering workflows.
- Creating dynamic dimension and fact tables.
- Understanding business keys versus surrogate keys.
- Implementing CDC-based incremental loading.
- Generating SQL queries programmatically.
- Performing insert and update operations using Delta Lake `MERGE`.
- Designing reusable transformations for multiple datasets.
- Automating the processing of new and updated records.

## 🚀 Future Enhancements

- Add data quality checks and validation rules.
- Improve handling of late-arriving and backdated records.
- Add pipeline monitoring and failure notifications.
- Introduce automated testing for data models.
- Optimize query execution and incremental processing.

## 👩‍💻 Author

Developed as a hands-on data engineering project focused on Databricks, dbt, incremental processing, and metadata-driven data warehouse development.
