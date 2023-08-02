
from pyspark.sql.functions import month, avg, unix_timestamp, dayofmonth, hour, col, desc

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_enriched = df.withColumn(
        "duration_time_in_minutes",
        (
            unix_timestamp(df["tpep_dropoff_datetime"])
            - unix_timestamp(df["tpep_pickup_datetime"])
        )
        / 60,
    )

    # Average fare by pickup and dropoff locations
    df_avg_fare = df_enriched.groupBy("PULocationID", "DOLocationID").agg(
        avg("fare_amount").alias("average_fare_amount")
    )

    df_avg_fare.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/average_fare_by_location")

    # Average fare by passenger count
    df_avg_fare_passenger = df_enriched.groupBy("passenger_count").agg(
        avg("fare_amount").alias("average_fare_amount")
    )

    df_avg_fare_passenger.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/average_fare_by_passenger_count")
    
    # Correlation between fare amount and trip distance
    correlation = df_enriched.stat.corr("fare_amount", "trip_distance")

    # Convert the correlation to an RDD and save it as a text file
    spark.sparkContext.parallelize([correlation]).format("txt").saveAsTextFile(f"{gcs_output_path}/correlation_between_fare_and_distance")


    spark.stop()
