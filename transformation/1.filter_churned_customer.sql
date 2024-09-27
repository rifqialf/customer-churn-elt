-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Filter files to only churned customer data
-- MAGIC 1. Read files
-- MAGIC 2. Filter all data
-- MAGIC 3. Write new files to gold directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Read + Filter Status file

-- COMMAND ----------

-- Drop existing status table in gold directory
DROP TABLE IF EXISTS customerchurn.gold.status_churned;

-- Create new status table in gold directory, where only churned customers data are to be used
CREATE TABLE customerchurn.gold.status_churned AS
SELECT *, current_timestamp() as `processing_time`
FROM customerchurn.silver.status as status
WHERE status.churn_value = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Read Service, Demographic files + Filter both files with status_churned data using Semi-Join + Write gold tables 

-- COMMAND ----------

-- Drop existing demographic table in gold directory
DROP TABLE IF EXISTS customerchurn.gold.demographic_churned;

-- Create new demographic table in gold directory by performing semi-join to status_churned table
CREATE TABLE customerchurn.gold.demographic_churned AS
SELECT *, current_timestamp() as `processing_time`
FROM customerchurn.silver.demographic as demographic_churned
WHERE EXISTS (
  SELECT 1
  FROM customerchurn.gold.status_churned as status_churned
  WHERE demographic_churned.customer_id = status_churned.customer_id
);

-- COMMAND ----------

-- Drop existing service table in gold directory
DROP TABLE IF EXISTS customerchurn.gold.service_churned;

-- Create new service table in gold directory by performing semi-join to status_churned table
CREATE TABLE customerchurn.gold.service_churned AS
SELECT *, current_timestamp() as `processing_time`
FROM customerchurn.silver.service as service_churned
WHERE EXISTS (
  SELECT 1
  FROM customerchurn.gold.status_churned as status_churned
  WHERE service_churned.customer_id = status_churned.customer_id
);
