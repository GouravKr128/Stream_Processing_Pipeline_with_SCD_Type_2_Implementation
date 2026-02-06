from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name="<<Eventhub_Name>>"  
event_hub_conn_str = "<<Connection_string>>"


kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
# Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

# Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.1adls.dfs.core.windows.net",
  "<<Access_key>>"
)

bronze_path = "abfss://bronze@1adls.core.windows.net/"

# Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_path+"checkpoint")
    .start(bronze_path)
)