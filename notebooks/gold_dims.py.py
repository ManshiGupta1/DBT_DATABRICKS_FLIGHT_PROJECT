# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.column import Column

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARAMETERS

# COMMAND ----------


dimension_configs = [
    {
        "key_cols": ["flight_id"],
        "cdc_col": "modified_date",
        "backdated_refresh": None,
        "source_schema": "silver",
        "source_object": "silver_flights",
        "target_schema": "gold",
        "target_object": "dimflights",
        "surrogate_key": "dimFlightsKey"
    },
    {
        "key_cols": ["passenger_id"],
        "cdc_col": "modified_date",
        "backdated_refresh": None,
        "source_schema": "silver",
        "source_object": "silver_customers",
        "target_schema": "gold",
        "target_object": "dimcustomers",
        "surrogate_key": "dimCustomersKey"
    },
    {
        "key_cols": ["booking_id"],
        "cdc_col": "modified_date",
        "backdated_refresh": None,
        "source_schema": "silver",
        "source_object": "silver_bookings",
        "target_schema": "gold",
        "target_object": "dimbookings",
        "surrogate_key": "dimBookingsKey"
    },
    {
        "key_cols": ["airport_id"],
        "cdc_col": "modified_date",
        "backdated_refresh": None,
        "source_schema": "silver",
        "source_object": "silver_airports",
        "target_schema": "gold",
        "target_object": "dimairports",
        "surrogate_key": "dimairportsKey"
    }


]


# COMMAND ----------

# MAGIC %md
# MAGIC ### incremental data ingestion

# COMMAND ----------

# DBTITLE 1,last load date

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    monotonically_increasing_id
)
from delta.tables import DeltaTable


def process_dimension(cfg):

    # 1. Read metadata
    key_cols_list = cfg["key_cols"]
    cdc_cols = cfg["cdc_col"]
    backdated_refresh = cfg["backdated_refresh"]

    source_schema = cfg["source_schema"]
    source_object = cfg["source_object"]

    target_schema = cfg["target_schema"]
    target_object = cfg["target_object"]
    surrogate_key = cfg["surrogate_key"]

    source_table = f"workspace.{source_schema}.{source_object}"
    target_table = f"workspace.{target_schema}.{target_object}"

    target_exists = spark.catalog.tableExists(target_table)
    
    # 2. Determine the incremental watermark
    if backdated_refresh:
        last_load = backdated_refresh

    elif target_exists:
        last_load = spark.sql(
            f"SELECT MAX({cdc_cols}) FROM {target_table}"
        ).first()[0]

        if last_load is None:
            last_load = "1900-01-01 00:00:00"

    else:
        last_load = "1900-01-01 00:00:00"

    # 3. Read incremental source records
    df_src = spark.table(source_table).filter(
        col(cdc_cols) > lit(last_load)
    )

    if not df_src.take(1):
        print(f"No new data for {target_table}")
        return

    # 4. Existing target: join and classify records
    if target_exists:

        df_tgt = spark.table(target_table).select(
            *key_cols_list,
            surrogate_key,
            "create_date",
            "update_date"
        )

        df_join = (
            df_src.alias("src")
            .join(
                df_tgt.alias("trg"),
                on=key_cols_list,
                how="left"
            )
        )

        df_old = df_join.filter(
            col(surrogate_key).isNotNull()
        )

        df_new = df_join.filter(
            col(surrogate_key).isNull()
        )

        # Preserve old keys and creation dates
        df_old_enr = df_old.withColumn(
            "update_date", current_timestamp()
        )

        # Get the maximum existing key
        max_surrogate_key = spark.sql(
            f"SELECT MAX({surrogate_key}) FROM {target_table}"
        ).first()[0]

        if max_surrogate_key is None:
            max_surrogate_key = 0

    else:
        # First load: all source records are new
        df_new = df_src
        df_old_enr = None
        max_surrogate_key = 0

    # 5. Generate keys for new records
    df_new_enr = (
        df_new
        .withColumn(
            surrogate_key,
            lit(max_surrogate_key)
            + lit(1)
            + monotonically_increasing_id()
        )
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )

    # 6. Combine existing and new records
    if df_old_enr is not None:
        df_upsert = df_old_enr.unionByName(df_new_enr)
    else:
        df_upsert = df_new_enr

    # 7. Create target on first load; MERGE on subsequent loads
    
   
    # 7. Create target on first load; MERGE on subsequent loads

    if not target_exists:
        (df_upsert.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

    else:
        merge_condition = " AND ".join(
            [f"t.`{k}` = s.`{k}`" for k in key_cols_list]
        )

        (DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(df_upsert.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Dimension processed: {target_table}")


    



# COMMAND ----------


for cfg in dimension_configs:
    process_dimension(cfg)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.airports

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.customers where passenger_id="P0049"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimcustomers where passenger_id = "P0049"

# COMMAND ----------

