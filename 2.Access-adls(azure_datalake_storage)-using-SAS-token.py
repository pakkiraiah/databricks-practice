# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Datalake using SAS(Shared Access Signature) Token
# MAGIC ##1.Set the spark config fs.azure.account.key
# MAGIC ##2.List the files from the DEMO container
# MAGIC ##3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

dbutils.secrets.listScopes()

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

sas_token_key=dbutils.secrets.get(scope="formula1-scope", key="sas-token-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickspracticesa.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspracticesa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspracticesa.dfs.core.windows.net",sas_token_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net"));

# COMMAND ----------

df =spark.read.csv("abfss://demo@databrickspracticesa.dfs.core.windows.net/circuits.csv")
df.limit(10).display()