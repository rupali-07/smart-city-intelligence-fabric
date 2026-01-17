# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b1915185-c9fa-4491-9a7f-bdc9eac2f8cb",
# META       "default_lakehouse_name": "SmartCity_Lakehouse",
# META       "default_lakehouse_workspace_id": "b6b48a8a-36bd-4726-97e9-a85cbfb2b422",
# META       "known_lakehouses": [
# META         {
# META           "id": "b1915185-c9fa-4491-9a7f-bdc9eac2f8cb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Smart City – Gold Data Model
# Author: Rupali Singh  
# Layer: Silver → Gold  
# Purpose: Create analytics-ready fact and dimension tables for Power BI


# MARKDOWN ********************

# # Smart City – Gold Data Model
# Author: Rupali Singh  
# Layer: Silver → Gold  
# Purpose: Create analytics-ready fact and dimension tables for Power BI


# CELL ********************

##Imports
from pyspark.sql.functions import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Read Silver Tables

traffic_df = spark.table("traffic_silver")
air_df = spark.table("air_quality_silver") \
.drop("congestion_level") 
energy_df = spark.table("energy_silver")
emergency_df = spark.table("emergency_silver") \
.drop("congestion_level") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

traffic_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

traffic_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_area = traffic_df.select("area").distinct()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date = traffic_df.select("date").distinct() \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("day_name", date_format("date", "EEEE"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_hour = traffic_df.select("hour").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fact table
fact_city = traffic_df \
    .join(air_df, ["date","area","hour"], "left") \
    .join(energy_df, ["date","area","hour"], "left") \
    .join(emergency_df, ["date","area","hour"], "left")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_city = fact_city.select(
    "date",
    "area",
    "hour",
    col("congestion_level").alias("traffic_congestion"),
    col("avg_speed_kmph").alias("avg_speed"),
    "pm25",
    "pm10",
    "air_quality_category",
    "energy_consumption_kwh",
    "renewable_percentage",
    "incident_count",
    "avg_response_time"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#write to gold

fact_city.write.mode("overwrite").saveAsTable("FactCityMetrics")
dim_area.write.mode("overwrite").saveAsTable("DimArea")
dim_date.write.mode("overwrite").saveAsTable("DimDate")
dim_hour.write.mode("overwrite").saveAsTable("DimHour")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("FactCityMetrics").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
