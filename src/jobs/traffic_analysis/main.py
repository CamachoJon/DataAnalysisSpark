from pyspark.sql.functions import avg, unix_timestamp, dayofweek, hour, weekofyear, round


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_enriched = df.withColumn(
        "trip_duration",
        round((
            unix_timestamp(df["tpep_dropoff_datetime"])
            - unix_timestamp(df["tpep_pickup_datetime"])
        ) / 3600, 1))
    df_enriched = df_enriched.withColumn("average_speed", round(df_enriched["trip_distance"] / df_enriched["trip_duration"], 1))

    df_speed_by_trip_hour = df_enriched.groupBy("VendorID", "trip_distance", "trip_duration", hour("tpep_pickup_datetime").alias("hour_of_day")).agg(
        avg("average_speed").alias("average_speed")
    ).orderBy("hour_of_day")
    
    df_speed_by_trip_hour.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/speed_by_trip_hour")

    df_speed_by_trip_day = df_enriched.groupBy("VendorID", "trip_distance", "trip_duration", dayofweek("tpep_pickup_datetime").alias("day_of_week")).agg(
        avg("average_speed").alias("average_speed")
    ).orderBy("day_of_week")

    df_speed_by_trip_day.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/speed_by_trip_day")

    df_speed_by_trip_week = df_enriched.groupBy("VendorID", "trip_distance", "trip_duration", weekofyear("tpep_pickup_datetime").alias("weekofyear")).agg(
        avg("average_speed").alias("average_speed")
    ).orderBy("weekofyear")

    df_speed_by_trip_week.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/speed_by_trip_week")

    spark.stop()