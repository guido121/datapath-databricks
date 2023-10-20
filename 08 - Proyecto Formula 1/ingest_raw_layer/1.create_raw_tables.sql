-- Databricks notebook source
-- MAGIC %md ##Create tables for CSV files

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId	INT,
  circuitRef STRING,	
  name STRING,
  location STRING,
  country STRING,
  lat	DOUBLE,
  lng	DOUBLE,
  alt	INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/sa70903775222/raw/circuits.csv", header true);

-- COMMAND ----------

-- MAGIC %md **Create races table**

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/sa70903775222/raw/races.csv", header true, delimiter ',');

-- COMMAND ----------

-- MAGIC %md ###Create tables for JSON files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/sa70903775222/raw/constructors.json");

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/sa70903775222/raw/drivers.json");

-- COMMAND ----------

-- MAGIC %md ###Create results table

-- COMMAND ----------

-- MAGIC %md ###Create pit stops

-- COMMAND ----------

-- MAGIC %md ###Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
 qualifyId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 position INT,
 q1 STRING,
 q2 STRING,
 q3 STRING
)
USING json
OPTIONS (path "/mnt/sa70903775222/raw/qualifying");

-- COMMAND ----------

-- MAGIC %md ###Create table lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lapTimes;
CREATE TABLE IF NOT EXISTS f1_raw.lapTimes(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/sa70903775222/raw/lap_times");

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/sa70903775222/processed"

-- COMMAND ----------


