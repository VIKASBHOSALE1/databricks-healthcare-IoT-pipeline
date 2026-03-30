# Databricks notebook source
import pyspark.sql.functions as F

df = spark.read.table('cat_healthcare_iot_patients.silver.silver_table')
# display(df)

# COMMAND ----------

# DBTITLE 1,Cell 2
# GOLD TABLE 1 — gold_patient_vitals_daily
#   Read from Silver table (batch read — no streaming needed for Gold in CE)
#   Group by: patient_id, date_trunc('day', timestamp)
#   Aggregate: avg(heart_rate), avg(temperature), avg(systolic_bp)
#   Add: max_heart_rate, count of readings
#   Write as Delta table to gold_patient_vitals_hourly

df_agg = df.groupBy('Patient_ID', 'date') \
  .agg(F.avg(F.col('Heart_Rate_bpm').cast('double')).alias('avg_heart_rate'), 
       F.round(F.avg(F.col('Temperature_C').cast('double')),2).alias('avg_temperature'), 
       F.avg(F.col('Systolic_BP_mmHg').cast('double')).alias('avg_systolic_bp'), 
       F.max(F.col('Heart_Rate_bpm').cast('double')).alias('max_heart_rate'), 
       F.count('*').alias('reading_count'))
df_agg.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('cat_healthcare_iot_patients.gold.gold_patient_vitals_daily')

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from cat_healthcare_iot_patients.gold.gold_patient_vitals_daily

# COMMAND ----------

# GOLD TABLE 2 — gold_anomaly_alerts
#   Filter Silver where alert saverity = critical 
#    Select: patient_id, ward_id, timestamp, heart_rate, spo2, temperature,
#           alert_severity, silver_processed_at
#   Add column: alert_category (derive from which vital breached threshold)
#   Write as Delta table 

df_alerts = df.filter(F.col('is_anomaly') == '1') \
  .select('Patient_ID',  'date', 'Heart_Rate_bpm', 'blood_pressure_status', 'Temperature_C', 
          'alert_severity','is_anomaly', 'Silver_Processed_At') 
    
df_alerts.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('cat_healthcare_iot_patients.gold.gold_anomaly_alerts')


# COMMAND ----------

# MAGIC %skip
# MAGIC display(df_alerts)

# COMMAND ----------

# DBTITLE 1,Cell 6
# GOLD TABLE 3 — gold_shift_summary
#   Add column: shift — use CASE WHEN hour(timestamp) logic:
#     Morning shift: 6 AM - 2 PM, Evening shift: 2 PM - 10 PM, Night shift: 10 PM - 6 AM
#   Group by:  date(timestamp), shift
#   Aggregate: total patients, total readings, alert count, avg vitals per day  per shift

df_shift_summary =(df.withColumn('shift', F.when((F.hour('timestamp') >= 6) & (F.hour('timestamp') < 14), 'morning') \
                                         .when((F.hour('timestamp') >= 14) & (F.hour('timestamp') < 22), 'evening') \
                                         .when((F.hour('timestamp') >= 22) | (F.hour('timestamp') < 6), 'night')) \
  .groupBy('date', 'shift') \
  .agg(F.countDistinct('Patient_ID').alias('total_patients'), 
       F.count('*').alias('total_readings'), 
       F.count(F.when(F.col('is_anomaly') == '1', F.col('is_anomaly'))).alias('alert_count'), 
       F.round(F.avg(F.col('Heart_Rate_bpm').cast('double')),2).alias('avg_heart_rate'), 
       F.round(F.avg(F.col('Temperature_C').cast('double')),2).alias('avg_temperature'), 
       F.avg(F.col('Systolic_BP_mmHg').cast('double')).alias('avg_systolic_bp')))

df_shift_summary.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('cat_healthcare_iot_patients.gold.gold_shift_summary')

# COMMAND ----------

# MAGIC %skip
# MAGIC display(df_shift_summary.orderBy('date'))