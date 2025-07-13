# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Datalake using Access Keys
# MAGIC ##1.Set the spark config fs.azure.account.key
# MAGIC ##2.List the files from the DEMO container
# MAGIC ##3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1_account_key=dbutils.secrets.get(scope="formula1-scope",key="access-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databrickspracticesa.dfs.core.windows.net", formula1_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net"));

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickspracticesa.dfs.core.windows.net/circuits.csv"))