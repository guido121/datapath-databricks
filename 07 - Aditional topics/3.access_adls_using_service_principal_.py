# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sa70903775222.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sa70903775222.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sa70903775222.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sa70903775222.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sa70903775222.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@sa70903775222.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://raw@sa70903775222.dfs.core.windows.net/circuits.csv"))
