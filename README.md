## Stream Processing Pipeline with SCD Type 2 Implementation
<br>

### **Overview**
   - This project implements a real-time streaming pipeline that ingests high-velocity hospital admission data via Azure Event Hubs and processes it using Spark Structured Streaming on Databricks.
   - It captures real-time patient events, cleanses "dirty" data, and maintains a historical record of patient attributes using Slowly Changing Dimensions (SCD) Type 2.
   - The architecture follows the Medallion Design Pattern, ensuring data progresses from raw events to high-quality, analytics-ready tables in a Star Schema.

### **Tech Stack**
Spark Structured Streaming, Kafka, Azure Event Hubs, Azure Synapse Analytics, Faker, PySpark, Azure Databricks, ADLS Gen2, Key Vault.

### **Description**
1. Data Generation
   - Streaming data is generated programmatically using a custom Python producer.
   - The python producer utilizes Faker library to generate hospital admission record like patient IDs, departments (ICU, Oncology, etc.), admission/discharge timestamps etc.
   - These events are streamed to Azure Event Hubs using the Kafka protocol.

2. Data Ingestion (Bronze Layer)
   - The ingestion engine uses Spark Structured Streaming to consume raw JSON byte payloads from Event Hubs.
   - Data is stored in its rawest form in Delta file format.

3. Transformation (Silver Layer)
   - In the Silver layer, the raw JSON is parsed into a structured schema and undergoes data cleaning(Invalid age/admission time are replaced with realistic values)
   - Schema Evolution: The pipeline is configured with .option("mergeSchema", "true") to handle potential upstream changes gracefully.
   - Auditability: An ingestion_time column is appended to every record for tracking and downstream watermarking.

4. Data Modelling & Warehousing (Gold Layer)
   - Used watermarking mechanism to read only new records from silver layer since the last successful execution.
   - Implemented SCD Type 2 to track historical changes in patient demographics. Used a Hash-based comparison (SHA-256) to detect changes in dimension tables.
   - Transforms the cleaned data into a Star Schema designed for BI and reporting.
   - Star Schema Design:
        - Dim_Patient: Historical tracking of patient details.
        - Dim_Department: Deduplicated list of hospital departments.
        - Fact_Tbl: High-performance table capturing metrics like length_of_stay_hours and is_currently_admitted status.
   - Azure Synapse Analytics: External tables are created in Synapse to provide a SQL interface over the Delta Lake, allowing analysts to query the Gold layer using SQL.
