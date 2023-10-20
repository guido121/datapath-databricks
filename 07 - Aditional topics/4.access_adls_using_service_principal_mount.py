# Databricks notebook source
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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@sa70903775222.dfs.core.windows.net/",
  mount_point = "/mnt/sa70903775222/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/sa70903775222/raw"))

# COMMAND ----------

display(spark.read.csv("/mnt/sa70903775222/raw/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("/mnt/sa70903775222/raw/races.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/sa70903775222/raw')
