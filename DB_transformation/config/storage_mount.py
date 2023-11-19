# Databricks notebook source
# First assign role Storage Blob Data Contributor to user then mount
def mounting(container_name):
    configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
    }

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
    source = f"abfss://{container_name}@advwork22adls.dfs.core.windows.net/",
    mount_point = f"/mnt/{container_name}",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT/")

# COMMAND ----------

mounting('bronze')

# COMMAND ----------

mounting('silver')

# COMMAND ----------

mounting('gold')

# COMMAND ----------

#dbutils.fs.unmount("/mnt/bronze")
