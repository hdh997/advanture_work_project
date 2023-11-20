# Databricks notebook source
#dbutils.fs.ls('mnt/silver/SalesLT/')

# COMMAND ----------

#dbutils.fs.ls('mnt/gold/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform trasformation for all tables

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# COMMAND ----------

tbl_name = []
for n in dbutils.fs.ls('mnt/silver/SalesLT/'):
    tbl_name.append(n.name.split('.')[0])

# COMMAND ----------

tbl_name

# COMMAND ----------

for name in tbl_name:
    path = '/mnt/silver/SalesLT/' + name
    df = spark.read.format('delta').load(path)

    col_names = df.columns

    for old_col_name in col_names:
        #convert from ColumnName to Column_name
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i , char in enumerate(old_col_name)] ).lstrip("_")

        # chnage the column nmae using withColumnRenamed
        df = df.withColumnRenamed(old_col_name, new_col_name)
    output_path = '/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)


# COMMAND ----------

#display(dbutils.fs.ls('mnt/gold/SalesLT/'))
