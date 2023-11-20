# Databricks notebook source
#display(dbutils.fs.ls('mnt/bronze/SalesLT/'))

# COMMAND ----------

#input_path = '/mnt/bronze/SalesLT/Address/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform tramsformation for all tables

# COMMAND ----------

tbl_name = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    tbl_name.append(i.name.split('/')[0])

# COMMAND ----------

tbl_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# COMMAND ----------

for name in tbl_name:
    path = '/mnt/bronze/SalesLT/' + name + '/' + name + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or 'date' in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), 'UTC'), 'dd-MM-yyyy'))
    output_path = '/mnt/silver/SalesLT/' + name + '/'
    df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

#display(df)

# COMMAND ----------

#display(dbutils.fs.ls('mnt/silver/SalesLT/Address'))
