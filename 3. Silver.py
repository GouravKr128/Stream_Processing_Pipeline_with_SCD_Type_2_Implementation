from pyspark.sql.types import *
from pyspark.sql.functions import *

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.2adls.dfs.core.windows.net",
  "<<Access_key>>"
)

bronze_path = "abfss://bronze@2adls.core.windows.net/"
silver_path = "abfss://silver@2adls.core.windows.net/"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#Defin Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

# Parse it to dataframe
bronze_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

# convert time column to Timestamp
bronze_df = bronze_df.withColumn("admission_time", to_timestamp("admission_time"))
bronze_df = bronze_df.withColumn("discharge_time", to_timestamp("discharge_time"))

# Handle invalid admission_times
bronze_df = bronze_df.withColumn("admission_time",
                               when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                   current_timestamp())
                               .otherwise(col("admission_time")))

# Handle Invalid Age
bronze_df = bronze_df.withColumn("age",
                               when(col("age")>100,floor(rand()*90+1).cast("int"))
                               .otherwise(col("age"))
                               )

# Handle missing column
expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]

for col_name in expected_cols:
    if col_name not in bronze_df.columns:
        bronze_df = bronze_df.withColumn(col_name, lit(None))

# Add the ingestion_timestamp column
bronze_df = bronze_df.withColumn("ingestion_time", current_timestamp())

# Handle schema evolution & write to silver layer  
(
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema","true")
    .option("checkpointLocation", silver_path + "checkpoint")
    .start(silver_path)
)

