# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Datalake using Service Principal
# MAGIC ##1.Register Azure AD /service principal
# MAGIC ##2. Generate secret/password for the application
# MAGIC ##3. Set Spartk config with APP/Client Id Directory /Tenent Id & Secret
# MAGIC ##4. Assign Role "Storage Blob Data Contributor" to the Data Lake
# MAGIC ##2.List the files from the DEMO container
# MAGIC ##3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

client_id=dbutils.secrets.get(scope="formula1-scope",key="client-id")
tenant_id=dbutils.secrets.get(scope="formula1-scope",key="tenant-id")
client_secret=dbutils.secrets.get(scope="formula1-scope",key="client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickspracticesa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickspracticesa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickspracticesa.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickspracticesa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickspracticesa.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickspracticesa.dfs.core.windows.net"));

# COMMAND ----------

df=spark.read.csv("abfss://demo@databrickspracticesa.dfs.core.windows.net/circuits.csv")
df.limit(10).display();