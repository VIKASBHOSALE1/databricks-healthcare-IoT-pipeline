# Databricks notebook source
# DBTITLE 1,a
#   1. Read the CSV from DBFS using spark.read.csv() with header=True, inferSchema=True
#   2. Display the first 10 rows using display() — understand your data

data_df = spark.read.csv(r'/Volumes/cat_healthcare_iot_patients/default/raw_iot_data/Raw_iot_data/',header=True,inferSchema=True)
# display(data_df.limit(10))

# COMMAND ----------

#   3. Print the schema using printSchema()
data_df.printSchema()

# COMMAND ----------

display(data_df.count())

# COMMAND ----------

#   4. Check for null values in each column
from pyspark.sql.functions import count, when, isnull,col
data_df.select([count(when(isnull(c), c)).alias(c) for c in data_df.columns]).show()


# COMMAND ----------

# data_df.select("Timestamp").distinct().show()

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import to_date,col, date_trunc

def Ingest_landing_zone (df):
    try:
        target_path = "/Volumes/cat_healthcare_iot_patients/default/landing_zone"
        # convertinf timestamp column to date column
        data_df = df.withColumn("date", to_date(col("Timestamp")))
        raw_count = data_df.count()

        data_df.coalesce(1)\
            .write\
            .mode("append")\
                .partitionBy("date")\
                .format("json")\
                    .save(target_path)
        
        print(f"\n{data_df} ingested to {target_path} with processed {raw_count} records")
    
    except Exception as e:
        print(f"ERROR TYPE: {type(e).__name__}")
        print(f"ERROR MESSAGE: {str(e)}")
        raise e
    

Ingest_landing_zone(data_df)

# COMMAND ----------

# MAGIC %sh ls /Volumes/cat_healthcare_iot_patients/default/landing_zone/

# COMMAND ----------

# DBTITLE 1,Cell 7
# for creating and listing files in a directory
path = "/Volumes/cat_healthcare_iot_patients/default/landing_zone/"
dbutils.fs.ls(path)

# COMMAND ----------

# for cleaning up the directory
# dbutils.fs.rm("/Volumes/cat_helathcare_iot_patients/default/landing_zone/",True)

# COMMAND ----------

print("Hello Git Integration")

# COMMAND ----------

# MAGIC %md
# MAGIC
