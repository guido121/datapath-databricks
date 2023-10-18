# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.sa709037752.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sa709037752.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sa709037752.dfs.core.windows.net", "sp=rl&st=2023-10-18T01:21:58Z&se=2023-10-18T09:21:58Z&sv=2022-11-02&sr=c&sig=rxmIETxNyTriQZaJfUVerHB0MDwDw3HZzQykEva2yoA%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@sa709037752.dfs.core.windows.net")

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@sa709037752.dfs.core.windows.net/circuits.csv")
display(circuits_df)
