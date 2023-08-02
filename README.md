# Taxi Trips Analysis with PySpark

This README file provides a comprehensive description of a set of PySpark scripts used for analyzing taxi trips data. Each script corresponds to a specific job that provides insights into different characteristics of these trips.

## Prerequisites

These scripts are designed to run on a Spark cluster and require a valid SparkSession (`spark`) to execute. The data format, as well as the input and output paths (`gcs_input_path` and `gcs_output_path`), are passed as parameters to the main `analyze()` function. 

## Data Import

All jobs load the taxi trips data from the specified `gcs_input_path` into a DataFrame. The data format is defined by the `format` parameter, defaulting to "parquet".

## Job Descriptions

### Fare Analysis

This job investigates the factors that could potentially influence the fare amount of a taxi trip. It groups the data by vendor and calculates average fare and trip distances. The results are ordered by average fare amount and saved to the location specified by `gcs_output_path`. The key outputs include information about the average fare and average trip distance for each vendor, which could be instrumental in understanding how different taxi services price their trips.

### Trip Analysis

This job focuses on the analysis of trip duration and pickup-dropoff locations. The duration of each trip in minutes is calculated and used to find the average trip time per month, per day of the month, and per hour. The top 10 pickup and dropoff locations are also determined based on the number of occurrences. The results of this analysis can provide valuable insights into trip lengths and popular locations for taxi services, which can be crucial for optimizing taxi routes and services.

### Tip Analysis

This job examines tip percentages, looking at how they relate to different aspects of the trips. A tip percentage column is calculated for each row. The data is then grouped by different categories - including pickup location, month, day of the week, hour, and payment type - and the average tip percentage for each category is computed. Additionally, the correlation between tip percentage and trip distance is calculated. These analyses could reveal trends in tipping behavior based on time, location, and trip distance, providing valuable information for taxi drivers and services.

### Traffic Analysis

This job investigates average speed during taxi trips, providing insights into traffic patterns. First, trip duration and average speed are calculated for each trip. The data is then grouped by vendor, trip distance, and trip duration, and the average speed is calculated for each hour of the day, each day of the week, and each week of the year. The results of this analysis can highlight times of heavy traffic, which can be useful for route planning and understanding when it's most efficient to drive.

### Taxi Pickups Prediction

This job performs a regression analysis to predict the count of taxi pickups based on the hour of the day and day of the week. It uses a Linear Regression model trained on data aggregated by the hour and day of the week, with the pickup count serving as the target variable. The model's predictions and its performance (RMSE) are saved as CSV files. This prediction could be highly valuable for taxi services looking to optimize their operations based on expected demand.

## Data Export

The results of the analyses are written to CSV files at the specified `gcs_output_path`. Each file overwrites any pre-existing data to ensure the output directory is kept clean and up-to-date. Depending on the script, several files may be generated, such as `month_analysis.csv`, `day_analysis.csv`, `hour_analysis.csv`, `top_pickup_locations.csv`, `top_dropoff_locations.csv`, `speed_by_trip_hour.csv`, `speed_by_trip_day.csv`, `speed_by_trip_week.csv`, `location_analysis.csv`, `type_analysis.csv`, `predictions.csv`, and others. Each of these files contains the results of a specific analysis.

## Closing the Spark Session

At the end of each script, the SparkSession is closed by invoking `spark.stop()`, ensuring that all resources are properly released after the script execution.

## Summary

These PySpark scripts provide valuable insights into taxi trips data, demonstrating the use of Apache Spark for big data processing and analysis. The scripts encompass various data analysis techniques, including aggregation, regression modeling, and correlation analysis, making them a useful resource for data analysis projects.