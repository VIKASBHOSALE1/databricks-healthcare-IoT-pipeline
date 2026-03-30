# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import col, lit, to_timestamp,when,current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------


bronze_path = 'cat_healthcare_iot_patients.bronze.Bronze_Table'


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from cat_healthcare_iot_patients.bronze.Bronze_Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select Patient_ID,Timestamp  ,count(*) from cat_healthcare_iot_patients.bronze.Bronze_Table
# MAGIC group by Patient_ID,Timestamp
# MAGIC having count(*)>1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 3

df = spark.readStream.format("delta").table(bronze_path)

# COMMAND ----------

# DBTITLE 1,Cell 8
# CELL 2 — Transformations (apply ALL of these in a single foreachBatch function):
#   a) Cast timestamp column to TimestampType
#   b) Filter rows where heart_rate IS NULL OR spo2 IS NULL
#   c) Filter out-of-range: heart_rate NOT BETWEEN 30 AND 250
#   d) Filter out-of-range: spo2 NOT BETWEEN 50 AND 100
#   e) Add column: is_anomaly (1 if any vital breaches critical threshold, else 0)
#   f) Add column: alert_severity ('CRITICAL', 'WARNING', or 'NORMAL')
#   g) Add column: silver_processed_at = current_timestamp()


silver_validated_readings = (
    df.withColumn('timestamp', col('Timestamp').cast('timestamp'))
    # Filter rows where Heart_Rate_bpm or Systolic_BP_mmHg are NULL
    .filter(col('Heart_Rate_bpm').isNotNull() & col('Systolic_BP_mmHg').isNotNull())
    # Add heart_rate_status
    .withColumn('heart_rate_status', 
                when(col('Heart_Rate_bpm').cast('double') > 96, 'High')
                .when(col('Heart_Rate_bpm').cast('double') < 63, 'Low')
                .otherwise('Normal'))
    # Add blood_pressure_status (using systolic)
    .withColumn('blood_pressure_status', 
                when(col('Systolic_BP_mmHg').cast('double') > 140, 'High')
                .when(col('Systolic_BP_mmHg').cast('double') < 100, 'Low')
                .otherwise('Normal'))
    # Add alert_severity based on the statuses above
    .withColumn('alert_severity', 
        when(
            (col('heart_rate_status') != "Normal") & (col('blood_pressure_status') != "Normal"), 
            'CRITICAL'
        )
        .when(
            (col('heart_rate_status') != "Normal") | (col('blood_pressure_status') != "Normal"), 
            'WARNING')
        .otherwise('NORMAL')
    )
   .withColumn('is_anomaly', when(col('alert_severity') == 'CRITICAL', '1').otherwise('0'))
    # Add processing timestamp
    .withColumn('silver_processed_at', current_timestamp())
    .drop('source_file','ingestion_timestamp')
)

# COMMAND ----------

def merge_batch_function(batch_df, batch_id):
    target_table = "cat_healthcare_iot_patients.silver.Silver_Table"
    
    # 1. Deduplicate the incoming batch FIRST (Just in case Bronze has duplicates)
    # Match on the unique "Medical Event" (Patient + Time of reading)
    deduplicated_batch = batch_df.dropDuplicates(["Patient_ID", "Timestamp"])

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        
        (delta_table.alias("t")
            .merge(deduplicated_batch.alias("s"), 
                   "t.Patient_ID = s.Patient_ID AND t.Timestamp = s.Timestamp") # <--- THE CRITICAL KEYS
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        (deduplicated_batch.write
         .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table))


# COMMAND ----------

# MAGIC %skip
# MAGIC # # Run this once to clear the bad memory
# MAGIC # checkpoint_silver_path = "/Volumes/cat_healthcare_iot_patients/infra_structure/streaming_vol/checkpoints/silver_ingestion"
# MAGIC # dbutils.fs.rm(checkpoint_silver_path, True)

# COMMAND ----------

# CELL 4 — Run stream with trigger(availableNow=True)
# spark.conf.set("spark.sql.shuffle.partitions", "4")
# DBTITLE 1,Run stream with trigger(availableNow=True)
stream = (
    silver_validated_readings.writeStream
    .foreachBatch(merge_batch_function)
    .option("checkpointLocation", "/Volumes/cat_healthcare_iot_patients/infra_structure/streaming_vol/checkpoints/silver_ingestion")
    .trigger(availableNow=True).option('mergeSchema', 'true')
    .start("cat_healthcare_iot_patients.silver.Silver_Table")
)
# DBTITLE 1,Wait for stream to finish
stream.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT alert_severity, COUNT(*) as record_count 
# MAGIC FROM cat_healthcare_iot_patients.silver.Silver_Table
# MAGIC GROUP BY alert_severity

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   heart_rate_status, 
# MAGIC   COUNT(*) as record_count
# MAGIC FROM cat_healthcare_iot_patients.silver.silver_table
# MAGIC GROUP BY heart_rate_status
# MAGIC

# COMMAND ----------


display(spark.sql("DESCRIBE HISTORY cat_healthcare_iot_patients.silver.Silver_Table"))

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC
# MAGIC # # # # Drop both tables to be safe
# MAGIC # # spark.sql("DROP TABLE IF EXISTS cat_healthcare_iot_patients.bronze.Bronze_Table")
# MAGIC spark.sql("DROP TABLE IF EXISTS cat_healthcare_iot_patients.silver.Silver_Table")
# MAGIC
# MAGIC # # Delete ALL checkpoints
# MAGIC dbutils.fs.rm("/Volumes/cat_healthcare_iot_patients/infra_structure/streaming_vol/checkpoints/silver_ingestion", True)

# COMMAND ----------

