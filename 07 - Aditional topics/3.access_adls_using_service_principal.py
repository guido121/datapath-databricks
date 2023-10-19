# Databricks notebook source
# MAGIC %md ###Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark config with App/Cliente Id, Directory/ Tenant Id & Secret
# MAGIC 4. Asign Role 'Storage Blob Data contributor' to the Data Lake

# COMMAND ----------

cliente_id = "81d7100a-4e73-452b-81a2-50a262ae1cd4"
tenant_id = "3209b50b-b79b-43dc-9fc4-8d42c406dd61"
client_secret = "MLI8Q~VdFmu6b9urr.47m8PPizWPm~xYfhPyDakj"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sa7090377522.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sa7090377522.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sa7090377522.dfs.core.windows.net", cliente_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sa7090377522.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sa7090377522.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@sa7090377522.dfs.core.windows.net")

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@sa7090377522.dfs.core.windows.net/circuits.csv")
display(circuits_df)
