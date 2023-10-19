# Databricks notebook source
# MAGIC %md ###Access Azure Data Lake using Service Principal
# MAGIC ####Steps to follow
# MAGIC 1. Guardar client_id, tenant_id and client_secret en key vault
# MAGIC 1. Configurar scope en Databricks
# MAGIC 1. Realizar la configuración en el notebook
# MAGIC 1. Call file system unity mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmounts)

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

cliente_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id') #"81d7100a-4e73-452b-81a2-50a262ae1cd4"
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id') # "3209b50b-b79b-43dc-9fc4-8d42c406dd61"
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret') # "MLI8Q~VdFmu6b9urr.47m8PPizWPm~xYfhPyDakj"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sa7090377522.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sa7090377522.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sa7090377522.dfs.core.windows.net", cliente_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sa7090377522.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sa7090377522.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": cliente_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@sa7090377522.dfs.core.windows.net/",
  mount_point = "/mnt/sa7090377522/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/sa7090377522/raw")

# COMMAND ----------

circuits_df = spark.read.csv("/mnt/sa7090377522/raw")
display(circuits_df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/sa7090377522/raw')

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Obtener los secretos desde key vault
    cliente_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id') #"81d7100a-4e73-452b-81a2-50a262ae1cd4"
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id') # "3209b50b-b79b-43dc-9fc4-8d42c406dd61"
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret') # "MLI8Q~VdFmu6b9urr.47m8PPizWPm~xYfhPyDakj"
    #Configuraciones de Spark
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": cliente_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Montar el storage account container
    dbutils.fs.mount(
        source = "abfss://raw@sa7090377522.dfs.core.windows.net/",
        mount_point = "/mnt/sa7090377522/raw",
        extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("sa7090377522","raw")

# COMMAND ----------


