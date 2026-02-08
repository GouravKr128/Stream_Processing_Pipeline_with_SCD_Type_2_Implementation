## Stream Processing Pipeline with SCD Type 2 Implementation
<br>

### **Overview**
   - This project implements a robust, end-to-end data engineering pipeline for hospital admission data. It captures real-time patient events, cleanses "dirty" data, and maintains a historical record of patient attributes using Slowly Changing Dimensions (SCD) Type 2.
   - The architecture follows the Medallion Design Pattern, ensuring data progresses from raw events to high-quality, analytics-ready tables in a Star Schema.

### **Tech Stack**
Spark Structured Streaming, Kafka, Azure Event Hubs, Azure Synapse Analytics, Faker, PySpark, Azure Databricks, ADLS Gen2, Key Vault.

### **Description**
1. Data Generation
   - To simulate a production environment, a custom Python script utilizes the Faker library to generate synthetic transactional data and user profiles. This generator produces a continuous stream of JSON-formatted events, simulating real-world scenarios such as user attribute updates (e.g., address changes or subscription tier shifts) which necessitate SCD Type 2 logic.

2. Data Ingestion
   - The generated data is produced to Apache Kafka, which serves as the distributed messaging backbone. These streams are integrated with Azure Event Hubs (utilizing the Kafka surface), providing a scalable, managed entry point for the cloud ecosystem. Azure Databricks then consumes these streams in real-time using Spark Structured Streaming, ensuring low-latency ingestion into the landing zone.

3. Transformation
The transformation layer is where the core logic resides. Using Spark, the pipeline performs:
  - Schema Enforcement: Ensuring incoming JSON data matches the expected structure.
  - Stateful Processing: Implementing the SCD Type 2 logic by comparing incoming records with existing records in the Silver/Gold layers.
  - Version Control: Automatically retiring old records (setting end_date and is_current = False) and inserting new records (setting start_date and is_current = True) when a change is detected in tracked attributes.

4. Data Modelling & Warehousing
The processed data is organized into a Star Schema to optimize for analytical queries:
  - Fact Tables: Store high-volume transactional events.
  - Dimension Tables: Store descriptive attributes, maintained via the SCD Type 2 process.
The final, modeled data is surfaced in Azure Synapse Analytics. By utilizing Synapse as the Data Warehouse, the project enables high-performance BI reporting and complex analytical workloads across the historical dataset.
