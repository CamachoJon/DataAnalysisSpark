from pyspark.sql.functions import month, avg, hour, dayofweek


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_enriched = df.withColumn(
        "tip_percentage",
        (
            df["tip_amount"] / df["total_amount"]
        ) * 100,
    )

    df_location = df_enriched.groupBy("PULocationID").agg(
        avg("tip_percentage").alias("average_tip_percentage") 
    ).orderBy("average_tip_percentage", ascending=False)  

    df_location.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/location_analysis")

    correlation = df_enriched.stat.corr("tip_percentage", "trip_distance")

    spark.sparkContext.parallelize([correlation]).saveAsTextFile(f"{gcs_output_path}/correlation_between_tip_and_distance")

    df_month = df_enriched.groupBy(
        month("tpep_pickup_datetime").alias("month")).agg(
        avg("tip_percentage").alias("average_tip_percentage")
    ).orderBy("month", ascending=True)  \

    df_month.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/month_analysis")

    df_day = df_enriched.groupBy(
        dayofweek("tpep_pickup_datetime").alias("dayofweek")).agg(
        avg("tip_percentage").alias("average_tip_percentage")
    ).orderBy("dayofweek", ascending=True)

    df_day.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/day_analysis")

    df_hour = df_enriched.groupBy(
        hour("tpep_pickup_datetime").alias("hour")).agg(
        avg("tip_percentage").alias("average_tip_percentage")
    ).orderBy("hour", ascending=True)

    df_hour.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/hour_analysis")
    
    df_type = df_enriched.groupBy("payment_type").agg(
            avg("tip_percentage").alias("average_tip_percentage")
        ).orderBy("average_tip_percentage", ascending=False) 
    
    df_type.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/type_analysis")
    
    spark.stop()