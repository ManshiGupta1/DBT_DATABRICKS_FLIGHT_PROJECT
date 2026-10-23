# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## data ingestion - INCREMENTAL

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
src_value

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option(
        "cloudFiles.schemaLocation",
        f"/Volumes/workspace/bronze/bronzevolume/{src_value}/schema"
    ) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(
        f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/"
    )

# COMMAND ----------

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option(
        "checkpointLocation",
        f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint"
    ) \
    .option(
        "path",
        f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data"
    ) \
    .start()

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/customers/data/`

# COMMAND ----------

