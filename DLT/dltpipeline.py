
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

############################################
# 1. FLIGHTS DATA

@dlt.view(
    name="trans_flights",
    comment="Transforming the flights data"
)
def trans_flights():

    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data")

    df = df.drop("_rescued_data") \
        .withColumn("modified_date", current_timestamp()) \
        .withColumn(
            "flight_date",
            to_date(col("flight_date"), "yyyy-MM-dd")
        )

    return df


dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)


############################################
# 2. BOOKINGS DATA

@dlt.view(
    name="trans_bookings",
    comment="Transforming the bookings data"
)
def trans_bookings():

    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/booking/data")

    df = df.withColumn(
            "amount", col("amount").cast(DoubleType())
        ) \
        .withColumn(
            "booking_date",
            to_date(col("booking_date"), "yyyy-MM-dd")
        ) \
        .withColumn(
            "modified_date", current_timestamp()
        ) \
        .drop("_rescued_data")

    return df


dlt.create_streaming_table("silver_bookings")

dlt.create_auto_cdc_flow(
    target="silver_bookings",
    source="trans_bookings",
    keys=["booking_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)


############################################
# 3. CUSTOMERS / PASSENGERS DATA

@dlt.view(
    name="trans_customers",
    comment="Transforming the customers data"
)
def trans_customers():

    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data")

    df = df.drop("_rescued_data") \
        .withColumn("modified_date", current_timestamp())

    return df


dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
    target="silver_customers",
    source="trans_customers",
    keys=["passenger_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)


############################################
# 4. AIRPORTS DATA

@dlt.view(
    name="trans_airports",
    comment="Transforming the airports data"
)
def trans_airports():

    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data")

    df = df.drop("_rescued_data") \
        .withColumn("modified_date", current_timestamp())

    return df


dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)

############################################################
#silver business view
import dlt

@dlt.table(
    name="silver_business",
    comment="Business-level joined data"
)
def silver_business():
    df = (
        dlt.readStream("silver_bookings")
        .join(dlt.readStream("silver_flights"), ["flight_id"])
        .join(dlt.readStream("silver_airports"), ["airport_id"])
        .join(dlt.readStream("silver_customers"), ["passenger_id"])
        .drop("modified_date")
    )
    return df

    return df