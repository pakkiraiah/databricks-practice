# Databricks notebook source
# MAGIC %md
# MAGIC #Explore DBFS Root
# MAGIC ##1. List All teh folders in DBFS root
# MAGIC ##2. Interact with DBFS file Browser
# MAGIC ##3. Upload file to ROOT
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

display(spark.read.csv("abfss://demo@databrickspracticesa.dfs.core.windows.net/circuits.csv").limit(10))

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))
