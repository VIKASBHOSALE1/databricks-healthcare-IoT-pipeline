# Databricks notebook source
landing_path = '/Volumes/cat_healthcare_iot_patients/default/landing_zone'
bronze_path = 'cat_healthcare_iot_patients.bronze.Bronze_Table'
checkpoint_path = '/Volumes/cat_healthcare_iot_patients/infra_structure/streaming_vol/checkpoints/bronze_stream'
schema_path = '/Volumes/cat_healthcare_iot_patients/infra_structure/streaming_vol/schemas/'

# COMMAND ----------

df = spark.readStream.format('cloudFiles') \
    .option("cloudFiles.format",'json')\
            .option("cloudFiles.schemalocation",schema_path)\
            .option("cloudFiles.schemaEvolutionMode", "addnewColumns")\
            .load(landing_path)
            

# COMMAND ----------

# DBTITLE 1,Add missing imports and fix NameError
from pyspark.sql.functions import current_timestamp, to_date, input_file_name, col

df_metadata = (
    df.withColumn('ingestion_timestamp', current_timestamp())
    .withColumn('ingestion_date', to_date("ingestion_timestamp"))
    .withColumn('source_file', col("_metadata.file_name"))
    )

# COMMAND ----------

# DBTITLE 1,Cell 4
# Clean column names by removing invalid Delta characters
for col_name in df_metadata.columns:
    clean_name = col_name.replace('(', '').replace(')', '').replace('°', '').replace(' ', '_')
    df_metadata = df_metadata.withColumnRenamed(col_name, clean_name)

df_metadata.writeStream.format('delta') \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .toTable(bronze_path) \
    .awaitTermination()

# COMMAND ----------

#     - Read the bronze table using spark.read.format
df_bronze= spark.read.format('delta').table(bronze_path)
# display(df_bronze)

# COMMAND ----------

# spark.sql(f'DESCRIBE HISTORY {bronze_path}')
display(spark.sql(f"DESCRIBE HISTORY {bronze_path}"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC