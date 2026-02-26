from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("HamidOil_SaveCleaned").getOrCreate()

CLEAN_PATH = r"C:\Users\alijo\Desktop\hamidoil\data\cleaned"
CSV_PATH = r"C:\Users\alijo\Desktop\hamidoil\data\cleaned\csv"

# Load data
stations_df  = spark.read.parquet(os.path.join(CLEAN_PATH, "stations_transformed.parquet"))
customers_df = spark.read.parquet(os.path.join(CLEAN_PATH, "customers_clean_temp.parquet"))
sales_df     = spark.read.parquet(os.path.join(CLEAN_PATH, "sales_clean_temp.parquet"))
inventory_df = spark.read.parquet(os.path.join(CLEAN_PATH, "inventory_clean_temp.parquet"))
invoices_df  = spark.read.parquet(os.path.join(CLEAN_PATH, "invoices_clean_temp.parquet"))

# Save final parquet files
stations_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "stations_final.parquet"))
customers_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "customers_final.parquet"))
sales_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "sales_final.parquet"))
inventory_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "inventory_final.parquet"))
invoices_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "invoices_final.parquet"))

# Save csv

stations_df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(CSV_PATH, "stations"))
customers_df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(CSV_PATH, "customers"))
sales_df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(CSV_PATH, "sales"))
inventory_df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(CSV_PATH, "inventory"))
invoices_df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(CSV_PATH, "invoices"))

print("✅ All final parquet & csv files saved succesfully.")
spark.stop()
