# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

catalog = "workspace"
source_schema = "silver"
target_schema = "gold"

source_table = f"{catalog}.{source_schema}.silver_bookings"
target_table = f"{catalog}.{target_schema}.fact_bookings"

# COMMAND ----------

# DBTITLE 1,Cell 2
dimensions = [
    {
        "table": f"{catalog}.{target_schema}.dimcustomers",
        "alias": "dimcustomers",
        "surrogate_key": "dimCustomersKey",
        "join_keys": [("passenger_id", "passenger_id")]
    },
    {
        "table": f"{catalog}.{target_schema}.dimflights",
        "alias": "dimflights",
        "surrogate_key": "dimFlightsKey",
        "join_keys": [("flight_id", "flight_id")]
    },
    {
        "table": f"{catalog}.{target_schema}.dimairports",
        "alias": "dimairports",
        "surrogate_key": "dimairportsKey",
        "join_keys": [("airport_id", "airport_id")]
    }
]

fact_columns = ["amount", "booking_date", "modified_date","dimcustomersKey","dimFlightsKey","dimairportsKey","booking_date"]

source_table = f"{catalog}.{source_schema}.silver_bookings"
cdc_col = "modified_date"
target_table = f"{catalog}.gold.fact_bookings"

# COMMAND ----------

from pyspark.sql.functions import col, lit

if spark.catalog.tableExists(target_table):
    last_load_date = spark.sql(
        f"SELECT MAX(`{cdc_col}`) FROM {target_table}"
    ).first()[0]

    if last_load_date is None:
        last_load_date = "1900-01-01 00:00:00"
else:
    last_load_date = "1900-01-01 00:00:00"

print("Last load date:", last_load_date)

# COMMAND ----------

# DBTITLE 1,Cell 4
def fact_query_incremental(
    source_table,
    dimensions,
    fact_columns,
    cdc_col,
    last_load_date
):
    # 1. SELECT fact columns
    select_cols = [
        f"src.`{column}` AS `{column}`"
        for column in fact_columns
    ]

    # 2. Add dimension surrogate keys
    for dim in dimensions:
        select_cols.append(
            f"{dim['alias']}.{dim['surrogate_key']}"
        )

    # 3. Build FROM clause
    query = f"""
        SELECT
            {", ".join(select_cols)}
        FROM {source_table} src
    """

    # 4. Build dimension JOIN clauses
    for dim in dimensions:
        join_conditions = []

        for source_key, dimension_key in dim["join_keys"]:
            join_conditions.append(
                f"src.`{source_key}` = "
                f"{dim['alias']}.`{dimension_key}`"
            )

        query += f"""
        LEFT JOIN {dim['table']} {dim['alias']}
            ON {" AND ".join(join_conditions)}
        """

    # 5. Incremental filter
    query += f"""
        WHERE src.`{cdc_col}` > TIMESTAMP('{last_load_date}')
    """

    return query

# COMMAND ----------

query =  fact_query_incremental(
    source_table,dimensions,
    fact_columns,
    cdc_col,
    last_load_date
)


# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# DBTITLE 1,Cell 9
from delta.tables import DeltaTable

if 'df_fact' in locals() and df_fact is not None and df_fact.take(1):

    target_exists = spark.catalog.tableExists(target_table)

    if not target_exists:

        # First load
        (df_fact.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print(f"Initial load completed: {target_table}")

    else:

        # Build dynamic MERGE condition
        merge_condition = " AND ".join(
            [f"t.`{key}` = s.`{key}`" for key in key_cols]
        )

        # Update existing records and insert new records
        (DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(
                df_fact.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Upsert completed: {target_table}")

else:
    print(f"No new data for {target_table}")

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %sql
# MAGIC select * from workspace.gold.fact_bookings

# COMMAND ----------

