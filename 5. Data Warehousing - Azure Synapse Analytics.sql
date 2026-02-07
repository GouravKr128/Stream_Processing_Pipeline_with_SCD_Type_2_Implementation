create schema gold;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'gk@34345';

CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';


CREATE EXTERNAL DATA SOURCE gold_layer
WITH (
    LOCATION = 'abfss://gold@2adls.dfs.core.windows.net/',
    CREDENTIAL = storage_credential
);


CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);


-- PATIENT DIMENSION Table
CREATE EXTERNAL TABLE gold.dim_patient (
    patient_id VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    effective_from DATETIME2,
    surrogate_key BIGINT,
    effective_to DATETIME2,
    is_current BIT
)
WITH (
    LOCATION = 'dim_patient/',
    DATA_SOURCE = gold_layer,
    FILE_FORMAT = ParquetFileFormat
);

--DEPARTMENT DIMENSION Table
CREATE EXTERNAL TABLE gold.dim_department (
    surrogate_key BIGINT,
    department NVARCHAR(200),
    hospital_id INT
)
WITH (
    LOCATION = 'dim_department/',
    DATA_SOURCE = gold_layer,
    FILE_FORMAT = ParquetFileFormat
);

--FACT TABLE
CREATE EXTERNAL TABLE gold.fact_tbl (
    fact_id BIGINT,
    patient_sk BIGINT,
    department_sk BIGINT,
    admission_time DATETIME2,
    discharge_time DATETIME2,
    admission_date DATE,
    length_of_stay_hours FLOAT,
    is_currently_admitted BIT,
    bed_id INT,
    event_ingestion_time DATETIME2
)
WITH (
    LOCATION = 'fact_tbl/',
    DATA_SOURCE = gold_layer,
    FILE_FORMAT = ParquetFileFormat
);

-- Query
SELECT * FROM gold.dim_patient;

SELECT * FROM gold.dim_department;

SELECT * FROM gold.fact_tbl;


