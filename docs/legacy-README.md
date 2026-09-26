# 🚀 Metadata-Driven Azure Data Engineering Pipeline

> Enterprise Metadata-Driven Azure Data Engineering Pipeline built using **Azure SQL Database, Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, Azure Key Vault, and Medallion Architecture**.

---

## 📌 Project Overview

This project demonstrates the implementation of an end-to-end **Metadata-Driven Azure Data Engineering Pipeline** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The solution automates the ingestion, transformation, and analytics process by using metadata stored in Azure SQL Database instead of hardcoding source tables or pipelines. Ingestion is also **incremental**, driven by watermark values tracked in the same metadata layer, so re-runs only pull new or changed data instead of reloading full tables.

The project showcases enterprise-level Azure Data Engineering concepts including dynamic pipelines, metadata-driven ETL, incremental loading, Delta Lake, Azure Databricks notebooks, Azure Data Factory orchestration, Azure Key Vault integration, and business KPI generation.

---

## 🎯 Project Objectives

- Build a Metadata-Driven ETL Framework
- Implement Bronze, Silver and Gold Architecture
- Automate data ingestion using Azure Data Factory
- Load data incrementally using a watermark-based strategy
- Perform transformations using Azure Databricks
- Generate business-ready analytical datasets
- Demonstrate enterprise Azure Data Engineering practices
- Eliminate repetitive pipeline development through metadata configuration

---

## 🏗️ Solution Architecture

> The complete architecture diagram is available below.

![Architecture](Architecture\Brazilian E-Commerce-2026-07-01-054021.png)

## ⚙️ Technology Stack

| Category | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Storage | Azure Data Lake Storage Gen2 |
| Database | Azure SQL Database |
| ETL Orchestration | Azure Data Factory |
| Data Processing | Azure Databricks |
| Processing Engine | Apache Spark |
| Storage Format | Parquet, Delta Lake |
| Programming Language | Python, SQL |
| Security | Azure Key Vault |
| Architecture | Medallion Architecture |
| Version Control | Git & GitHub |

---

## 📂 Repository Structure

```
Metadata-Driven-Azure-Data-Engineering-Pipeline
│
├── Architecture
│   ├── Brazilian E-Commerce-2026-07-01-054021.png
│
├── Azure Data Factory
│   ├── ARMTemplateForFactory.json
│   ├── ARMTemplateParametersForFactory.json
│   └── Pipeline Screenshots
│
├── Azure SQL
│   ├── 01_CreateSchemas.sql
│   ├── 02_Metadata.sql
│   ├── 03_Logging.sql
│   └── 04_StoredProcedures.sql
│
├── Databricks
│   ├── BronzeToSilver.ipynb
│   └── SilverToGold.ipynb
│
├── Dataset
│   └── DatasetInfo.md
│
├── README.md
├── LICENSE
└── .gitignore
```

---

## 🔄 End-to-End Workflow

The project follows a Metadata-Driven ETL approach where Azure Data Factory dynamically reads metadata from Azure SQL Database and processes each configured source table without hardcoding pipeline logic — pulling only new or changed rows on every run.

### Workflow

1. Source data is stored in Azure SQL Database.
2. Azure Data Factory reads the list of tables from `Metadata.SourceTableConfig`.
3. A Lookup activity retrieves the metadata, including each table's watermark column and last-loaded watermark value.
4. A ForEach activity iterates through each configured table.
5. Copy Activity ingests only rows newer than the stored watermark into the Bronze layer as Parquet files.
6. On successful completion, a stored procedure updates the watermark value in `Metadata.SourceTableConfig` to the latest value loaded.
7. Azure Databricks Notebook 1 transforms Bronze data into cleansed Delta tables in the Silver layer.
8. Azure Databricks Notebook 2 builds analytical Gold tables and business KPIs.
9. Gold datasets are ready for reporting, dashboards, and business analytics.

---

# 🗄️ Azure SQL Layer

Azure SQL Database acts as the **source system** as well as the **metadata repository** for the pipeline.

## Business Tables

The following source tables are maintained in Azure SQL Database:

- Customers
- Orders
- Order Items
- Products
- Sellers
- Payments
- Reviews
- Product Categories
- Geolocation

---

## Metadata Layer

A metadata-driven approach eliminates hardcoded pipeline logic.

The `Metadata.SourceTableConfig` table stores:

- Source Table Name
- Target File Name
- Load Type
- Active Flag
- Watermark Column
- Last Loaded Watermark Value

Azure Data Factory dynamically reads this metadata and processes each table automatically — using the watermark fields to determine which rows are new since the last run.

---

## Logging Layer

The `Logging.ExecutionLog` table captures execution details including:

- Pipeline Name
- Activity Name
- Table Name
- Status
- Rows Copied
- Execution Duration
- Error Message
- Start Time
- End Time

This provides complete pipeline monitoring and auditability, and gates whether a table's watermark is advanced — a table's watermark only moves forward after its load is logged as successful, so a failed run never causes rows to be silently skipped on the next execution.

---

# 🥉 Bronze Layer

The Bronze layer stores the **raw ingested data** exactly as received from the source system.

### Characteristics

- Raw data
- Parquet format
- No transformations
- Historical storage
- Landing zone for all source tables
- Loaded incrementally per table, based on each table's watermark

Azure Data Factory dynamically copies all configured tables into Azure Data Lake Storage Gen2 Bronze container.

---

# 🥈 Silver Layer

The Silver layer contains **cleaned and transformed** datasets created using Azure Databricks.

Transformations performed include:

- Schema validation
- Duplicate removal
- Null handling
- Data type conversion
- Standardized column names
- Delta Lake conversion

The Silver layer represents trusted and analytics-ready transactional data.

---

# 🥇 Gold Layer

The Gold layer contains business-ready analytical datasets generated from Silver tables.

The following KPIs were created:

- Revenue By Month
- Top Products
- Top Customers
- Top Sellers
- Payment Analysis
- Review Analysis
- Average Order Value

These datasets can be directly consumed by reporting tools such as Power BI.

---

# 📊 Business KPIs Generated

The Gold Layer produces business-ready datasets that can be directly consumed by reporting and visualization tools.

| KPI | Description | Business Value |
|---|---|---|
| Revenue By Month | Monthly sales trend | Revenue trend analysis |
| Top Products | Products ranked by revenue and units sold | Product performance analysis |
| Top Customers | Customers ranked by total spending | Customer segmentation |
| Top Sellers | Sellers ranked by revenue | Seller performance evaluation |
| Payment Analysis | Payment method distribution | Payment behavior insights |
| Review Analysis | Review score distribution | Customer satisfaction analysis |
| Average Order Value | Average revenue per order | Business performance monitoring |

---

# ⏱️ Incremental Loading Strategy

The pipeline loads data incrementally instead of doing a full reload on every run.

- Each row in `Metadata.SourceTableConfig` defines a **watermark column** for that source table (e.g. a last-modified/updated timestamp) and stores the **last successfully loaded watermark value**.
- On each run, the Lookup activity retrieves the current watermark per table, and the Copy Activity's source query filters for rows newer than that value (`WHERE <watermark_column> > @last_watermark`).
- After a table loads successfully — confirmed via `Logging.ExecutionLog` — a stored procedure advances that table's watermark to the maximum value just loaded.
- If a load fails, the watermark is **not** advanced, so the next run safely retries the same window instead of skipping rows.
- This keeps the framework fully metadata-driven: incremental behavior is configured per table, not hardcoded per pipeline.

---

# 🚀 Key Features

- ✅ Metadata-Driven ETL Framework
- ✅ Dynamic Azure Data Factory Pipelines
- ✅ Incremental Data Loading via Watermark Strategy
- ✅ Parent-Child Pipeline Architecture
- ✅ Azure SQL Metadata Repository
- ✅ Azure Databricks Transformations
- ✅ Apache Spark Data Processing
- ✅ Delta Lake Implementation
- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Azure Data Lake Storage Gen2
- ✅ Azure Key Vault Integration
- ✅ Execution Logging Framework
- ✅ Dynamic Table Processing
- ✅ Enterprise Folder Structure
- ✅ Business KPI Generation

---

# 📈 Project Outcomes

This project demonstrates an enterprise-scale Azure Data Engineering solution capable of:

- Dynamically processing multiple source tables using metadata.
- Loading data incrementally using a per-table watermark strategy.
- Automating ETL workflows through Azure Data Factory.
- Building scalable Bronze, Silver and Gold data layers.
- Performing distributed data transformations using Apache Spark.
- Generating analytical datasets for reporting and dashboarding.
- Implementing secure credential management using Azure Key Vault.
- Following enterprise software engineering and data engineering best practices.

---

# 🛠️ How to Run

1. Create an Azure SQL Database.
2. Import the Brazilian E-Commerce Dataset.
3. Execute the SQL scripts located in the **Azure SQL** folder.
4. Deploy the Azure Data Factory ARM template.
5. Create the required Linked Services and Datasets.
6. Configure Azure Data Lake Storage Gen2.
7. Import the Databricks notebooks.
8. Execute the Master Pipeline.
9. Validate the Bronze, Silver and Gold layers, and confirm watermark values advance in `Metadata.SourceTableConfig` after each run.

---

# 🧠 Skills Demonstrated

## Azure Services

- Azure SQL Database
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Azure Key Vault

## Data Engineering

- ETL Pipelines
- Metadata-Driven Architecture
- Incremental Data Loading (Watermark Strategy)
- Medallion Architecture
- Delta Lake
- Apache Spark
- Data Transformation
- Data Validation
- Data Quality

## Programming

- Python
- SQL
- PySpark

## Engineering Practices

- Git
- GitHub
- Modular Pipeline Design
- Dynamic ETL
- Enterprise Folder Structure
- Secure Credential Management

---

# 🚀 Future Enhancements

- Slowly Changing Dimensions (SCD Type 2)
- Change Data Capture (CDC)
- Power BI Dashboard Integration
- CI/CD using Azure DevOps
- Managed Identity Authentication
- Unity Catalog Integration
- Automated Data Quality Checks
- Monitoring Dashboard
- Email Notifications on Pipeline Failures

---

# 👨‍💻 Author

**Pranay Yadagiri**

Final Year B.Tech Computer Science Engineering (Cyber Security)

Aspiring Azure Data Engineer

LinkedIn: <https://www.linkedin.com/in/pranay-yadagiri-90bb60362>

---

## ⭐ If you found this repository helpful, consider giving it a star!
