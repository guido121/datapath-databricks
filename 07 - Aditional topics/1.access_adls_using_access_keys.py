# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.sa709037752.dfs.core.windows.net",
    "itpWwgo6hBvGngxPIDuqerJ7rbIJDGp4g/KeWMvLuiwoXiGETsZCrV6z6Uii74WNU/b7ul9zb4gu+AStEg718w=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://raw@sa709037752.dfs.core.windows.net")

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@sa709037752.dfs.core.windows.net/circuits.csv")
display(circuits_df)
