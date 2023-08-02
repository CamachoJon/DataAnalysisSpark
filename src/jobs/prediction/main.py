
from pyspark.sql.functions import month, avg, unix_timestamp, dayofmonth, hour, col, desc, dayofweek
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_enriched = df.withColumn("hour_of_day", hour(df["tpep_pickup_datetime"])) \
                    .withColumn("day_of_week", dayofweek(df["tpep_pickup_datetime"]))
    
    df_pickups = df_enriched.groupBy("hour_of_day", "day_of_week").count()

    (train_df, test_df) = df_pickups.randomSplit([0.7, 0.3])

    # Create a VectorAssembler which consumes columns hour_of_day and day_of_week and produces a column 'features'
    vec_assembler = VectorAssembler(inputCols=["hour_of_day", "day_of_week"], outputCol="features")
    train_df = vec_assembler.transform(train_df)

    # Define Linear Regression Model
    lr = LinearRegression(featuresCol="features", labelCol="count")

    # Fit the model
    lr_model = lr.fit(train_df)

    # Transform the test data to include the features column
    test_df = vec_assembler.transform(test_df)

    # Use the model to make predictions
    predictions = lr_model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="count", predictionCol="prediction", metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

    # Convert RMSE value to RDD
    rmse_rdd = spark.sparkContext.parallelize([rmse])

    # Save the RDD to a text file
    rmse_rdd.saveAsTextFile(f"{gcs_output_path}/rmse")

    predictions.select("hour_of_day", "day_of_week", "count", "prediction") \
           .coalesce(1) \
           .write \
           .mode("overwrite") \
           .option("header","true") \
           .csv(f"{gcs_output_path}/predictions")

    spark.stop()
