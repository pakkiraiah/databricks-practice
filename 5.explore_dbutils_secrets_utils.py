# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Datalake using Access Keys - explore_dbutils_secrets_utils
# MAGIC ##1.Set the spark config fs.azure.account.key
# MAGIC ##2.List the files from the DEMO container
# MAGIC ##3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

dbutils.secrets.listScopes()

dbutils.secrets.list("formula1-scope")
#dbutils.secrets.list("databrickspracticesa")
#spark.conf.set("fs.azure.account.key.databrickspracticesa.dfs.core.windows.net", "ynj0e8Zj0sFSbKvxPSuBhc3tqMHcdoEZkuHfWlFCR0Me9bDt1iEF6buTbEb3rAXGvwb843ZsNxY8+ASt4L/o6w==")

# COMMAND ----------

#get key
dbutils.secrets.get(scope="formula1-scope",key="access-key")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net"));

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickspracticesa.dfs.core.windows.net/circuits.csv"))