import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="stage_bookings",
  comment="staging the data"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/booking/data")
    return df
  
@dlt.view(
    name="trans_booking",
    comment="transforming the data"
)

def trans_booking():
    df= spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
                .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))\
                .withColumn("modified_date",current_timestamp())\
                .drop("_rescued_data")

    return df

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings_v2",
    comment = "silver table for bookings"
)
@dlt.expect_all(rules)
def silver_bookings():
    df = spark.readStream.table("trans_booking")
    return df