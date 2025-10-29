# 🚀 Comprehensive Azure Data Engineering Project

This project simulates a **real-world, end-to-end data engineering solution** on the Microsoft Azure cloud platform. It demonstrates the construction of a robust, scalable, and fully automated ETL/ELT pipeline covering everything from **incremental data ingestion** to **dimensional modeling** and **CI/CD** integration.

---

## 🎯 Project Goals

The primary objectives of this project were to:

* Design and implement a **modern Lakehouse architecture** using Azure services.
* Build a **metadata-driven** pipeline for scalable, incremental data ingestion.
* Master advanced **PySpark** and **Spark Streaming** techniques for data processing.
* Implement core **Dimensional Data Modeling** principles, including **Slowly Changing Dimensions (SCDs)**.
* Establish a robust **CI/CD** workflow using **Databricks Asset Bundles (DABs)** and **GitHub**.

---

## 🏗️ Architecture Overview

The solution follows a **medallion architecture** (Bronze, Silver, Gold layers) and leverages a suite of powerful Azure and Databricks tools.



### **Key Components & Flow**

1.  **Ingestion (Bronze Layer):** **Azure Data Factory (ADF)** is used to orchestrate the initial data load.
    * **Incremental Ingestion:** ADF pipelines are designed to pull only new or updated records from the **Azure SQL DB Source** using **watermarking** to ensure efficiency.
    * **Backfilling Feature:** A mechanism is implemented within the ADF pipelines to handle historical data re-processing when necessary.
    * **External Trigger:** **Azure Logic Apps** are integrated to trigger the ADF pipelines based on external events or schedules.
2.  **Transformation (Silver Layer):** **Azure Databricks** and **PySpark** perform the cleaning and transformation.
    * **Spark Streaming:** The **Databricks Autoloader** is utilized for efficient, schema-inference-based **streaming ingestion** of files into the Bronze layer.
    * **Metadata-Driven Transformations:** **PySpark** and **Jinja2** templates are combined to create flexible and reusable transformation logic that adapts based on table metadata.
3.  **Modeling (Gold Layer):** The final layer is optimized for analytical workloads.
    * **Delta Live Tables (DLT):** Used to declaratively build and manage reliable data pipelines, automatically handling dependencies and quality checks.
    * **Dimensional Modeling:** Data is structured into a **Star Schema** with Fact and Dimension tables.
    * **SCD Implementation:** Implemented logic for **Slowly Changing Dimensions (SCDs)** (e.g., SCD Type 2) to track historical changes in dimension attributes.

---

## 🛠️ Technologies & Tools

This project is a deep dive into the following enterprise-grade data tools:

### **Azure Services**

| Tool | Role in Project |
| :--- | :--- |
| **Azure Data Factory (ADF)** | Orchestration, Scheduling, Incremental Data Ingestion, Looping Pipelines. |
| **Azure SQL DB** | Primary Source System, storing the initial raw data. |
| **Azure Logic Apps** | External triggers and integration for data pipelines. |

### **Databricks & Lakehouse**

| Tool | Role in Project |
| :--- | :--- |
| **Azure Databricks** | High-performance ETL/ELT processing engine. |
| **Unity Catalog** | Centralized governance, fine-grained access control, and data lineage. |
| **Delta Live Tables (DLT)** | Declarative pipeline development for simplified and reliable ETL/ELT. |
| **Delta Lake** | Storage format supporting ACID transactions, schema enforcement, and time travel. |
| **Spark Streaming** | Real-time or near-real-time ingestion using Databricks Autoloader. |

### **Code & Development**

| Tool | Role in Project |
| :--- | :--- |
| **PySpark** | Data cleaning, complex transformations, and custom utilities. |
| **Python Utilities** | Custom helper functions and modularized code for reusability. |
| **GitHub** | Source Control Management (SCM) for the entire project codebase. |
| **Databricks Asset Bundles (DAB)** | CI/CD automation for deploying notebooks, jobs, and MLOps components. |

---

## 💡 Data Engineering Concepts Demonstrated

* **Dimensional Data Modeling:** Building a Star Schema (Fact and Dimension tables) for analytical simplicity.
* **Slowly Changing Dimensions (SCDs):** Specifically, implementation of **SCD Type 2** to maintain a historical view of dimension attribute changes.
* **Incremental Data Loading:** Utilizing watermarking in ADF and merging/upsert strategies in Databricks for efficiency.
* **Metadata-Driven Pipelines:** Abstracting transformation logic using template engines (Jinja2) to easily onboard new data sources.
* **CI/CD:** Using Databricks Asset Bundles for repeatable, declarative, and automated deployment of the Databricks workspace artifacts.

---

## 🗺️ Repository Structure

. ├── adf_pipelines/ # Azure Data Factory exported ARM Templates and JSON definitions ├── databricks_notebooks/ # Core PySpark, DLT, and Streaming code │ ├── bronze/ # Streaming ingestion (Autoloader) │ ├── silver/ # PySpark transformations (including metadata-driven) │ └── gold/ # DLT pipelines and SCD implementation ├── databricks_bundles/ # Configuration files for Databricks Asset Bundles (DAB) ├── sql_scripts/ # DDL/DML for Azure SQL DB Source and Gold Schema ├── utils/ # Reusable Python/PySpark utility modules └── README.md