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

# # Smart City – Data Generator (Clean)
# Author: Rupali Singh  
# Purpose: Generate large-scale smart city data (hourly grain)


# CELL ********************

##Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Configurations
areas = [
    "Downtown",
    "Residential North",
    "Residential South",
    "Industrial Zone",
    "IT Park",
    "Old City",
    "Airport Zone"
]

start_date = "2022-01-01"
end_date = "2024-12-31"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Date Dimensions
date_df = spark.sql(f"""
SELECT explode(
    sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )
) as date
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Hour Dimension
hours_df = spark.range(0, 24).withColumnRenamed("id", "hour")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#traffic dataset (hourly-core)
traffic_df = date_df \
.crossJoin(spark.createDataFrame([(a,) for a in areas], ["area"])) \
.crossJoin(hours_df) \
.withColumn(
    "congestion_level",
    when(col("hour").between(8,10), rand()*30 + 60)
    .when(col("hour").between(17,20), rand()*35 + 65)
    .otherwise(rand()*40 + 25)
).withColumn(
    "avg_speed_kmph",
    when(col("congestion_level") > 70, rand()*10 + 15)
    .otherwise(rand()*20 + 30)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Air quality dataset
air_df = traffic_df.select("date","area","hour","congestion_level") \
.withColumn("pm25", col("congestion_level") * (rand()*0.6 + 0.8)) \
.withColumn("pm10", col("pm25") * (rand()*1.3 + 1.1)) \
.withColumn(
    "air_quality_category",
    when(col("pm25") > 100, "Severe")
    .when(col("pm25") > 75, "Very Poor")
    .when(col("pm25") > 50, "Poor")
    .otherwise("Moderate")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Energy Dataset
energy_df = traffic_df.select("date","area","hour") \
.withColumn(
    "energy_consumption_kwh",
    when(col("area") == "Industrial Zone", rand()*80000 + 250000)
    .when(col("area") == "Downtown", rand()*50000 + 120000)
    .otherwise(rand()*40000 + 80000)
).withColumn(
    "renewable_percentage",
    rand()*20 + 10
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Emergency dataset
emergency_df = traffic_df.select("date","area","hour","congestion_level") \
.withColumn(
    "incident_count",
    when(col("congestion_level") > 75, rand()*3 + 1)
    .otherwise(rand()*1)
).withColumn(
    "avg_response_time",
    when(col("area") == "Downtown", rand()*5 + 8)
    .otherwise(rand()*7 + 10)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write silver tables
#"""traffic_df.write.mode("overwrite").saveAsTable("traffic_silver")
#air_df.write.mode("overwrite").saveAsTable("air_quality_silver")
#energy_df.write.mode("overwrite").saveAsTable("energy_silver")
#emergency_df.write.mode("overwrite").saveAsTable("emergency_silver")"""

traffic_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("traffic_silver")

air_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("air_quality_silver")

energy_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("energy_silver")

emergency_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("emergency_silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#validation
spark.table("traffic_silver").printSchema()
spark.table("traffic_silver").count()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

traffic_df.printSchema()
traffic_df.select("hour").show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Regenerate dependent silver tables (hourly grain)
air_df.write.mode("overwrite").saveAsTable("air_quality_silver")
energy_df.write.mode("overwrite").saveAsTable("energy_silver")
emergency_df.write.mode("overwrite").saveAsTable("emergency_silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("air_quality_silver").count()
spark.table("energy_silver").count()
spark.table("emergency_silver").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
