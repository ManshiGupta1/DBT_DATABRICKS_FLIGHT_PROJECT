# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.raw.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/booking")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA  workspace.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.bronzevolume

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/customers/data/`

# COMMAND ----------

