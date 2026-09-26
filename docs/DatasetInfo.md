# Brazilian E-Commerce Public Dataset by Olist

## Overview

This project uses the **Brazilian E-Commerce Public Dataset by Olist**, which contains real-world e-commerce transactional data from Brazilian marketplaces between 2016 and 2018.

The dataset was used as the source system for building a complete Metadata-Driven Azure Data Engineering Pipeline.

---

## Dataset Source

- **Provider:** Olist
- **Platform:** Kaggle
- **Link:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## Dataset Description

The dataset contains information related to:

- Customers
- Orders
- Products
- Sellers
- Payments
- Reviews
- Product Categories
- Geolocation

---

## Source Tables

| Table | Description |
|--------|-------------|
| customers | Customer information |
| orders | Order details |
| orderitems | Products purchased in each order |
| orderpayments | Payment information |
| orderreviews | Customer reviews |
| products | Product information |
| sellers | Seller details |
| category | Product category translation |
| geolocation | Customer and seller geographical information |

---

## Dataset Size

- **Total Tables:** 9
- **Customers:** ~99K
- **Orders:** ~99K
- **Products:** ~33K
- **Order Items:** ~112K
- **Reviews:** ~99K
- **Payments:** ~103K
- **Geolocation Records:** ~738K

---

## Purpose

This dataset was used to demonstrate an end-to-end Azure Data Engineering solution involving:

- Azure SQL Database
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- Medallion Architecture
- Metadata-Driven ETL Pipelines
- Business KPI Generation