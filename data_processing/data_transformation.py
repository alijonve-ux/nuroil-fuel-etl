from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum
import os

spark = SparkSession.builder.appName("NurOil_Transformation").getOrCreate()

CLEAN_PATH = r"C:\Users\alijo\Desktop\nuroil\data\cleaned"

# Load cleaned data
stations_df  = spark.read.parquet(os.path.join(CLEAN_PATH, "stations_clean_temp.parquet"))
sales_df     = spark.read.parquet(os.path.join(CLEAN_PATH, "sales_clean_temp.parquet"))

# Transformations
avg_price_df = sales_df.groupBy("station_id").avg("price_per_liter") \
    .withColumnRenamed("avg(price_per_liter)", "avg_price_per_liter")
revenue_df = sales_df.groupBy("station_id").sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "total_revenue")

stations_df = stations_df.join(avg_price_df, "station_id", "left").join(revenue_df, "station_id", "left")

# Save transformed version
stations_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "stations_transformed.parquet"))
print("✅ Transformation complete.")
spark.stop()
