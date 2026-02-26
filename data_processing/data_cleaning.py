from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, regexp_replace
import os

spark = SparkSession.builder.appName("NurOil_DataCleaning").getOrCreate()

CLEAN_PATH = r"C:\Users\alijo\Desktop\nuroil\data\cleaned"

# Load previously ingested raw data
stations_df  = spark.read.parquet(os.path.join(CLEAN_PATH, "stations_raw.parquet"))
customers_df = spark.read.parquet(os.path.join(CLEAN_PATH, "customers_raw.parquet"))
sales_df     = spark.read.parquet(os.path.join(CLEAN_PATH, "sales_raw.parquet"))
inventory_df = spark.read.parquet(os.path.join(CLEAN_PATH, "inventory_raw.parquet"))
invoices_df  = spark.read.parquet(os.path.join(CLEAN_PATH, "invoices_raw.parquet"))

# Cleaning
stations_df = stations_df.dropDuplicates(["station_id"]).withColumn("region", trim(upper(col("region"))))
customers_df = customers_df.dropDuplicates(["customer_id"]) \
    .withColumn("region", trim(upper(col("region")))) \
    .withColumn("email", regexp_replace(col("email"), " ", ""))
sales_df = sales_df.dropDuplicates(["sale_id"]) \
    .withColumn("fuel_type", upper(trim(col("fuel_type")))) \
    .withColumn("payment_type", upper(trim(col("payment_type"))))

# Handle negatives
sales_df = sales_df.withColumn("liters", when(col("liters") < 0, None).otherwise(col("liters")))
sales_df = sales_df.withColumn("price_per_liter", when(col("price_per_liter") < 0, None).otherwise(col("price_per_liter")))

inventory_df = inventory_df.dropDuplicates(["inventory_id"]).withColumn("fuel_type", upper(trim(col("fuel_type"))))
invoices_df = invoices_df.dropDuplicates(["invoice_id"]).withColumn("status", upper(trim(col("status"))))

print("✅ Cleaning complete.")

# Save intermediate cleaned data
stations_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "stations_clean_temp.parquet"))
customers_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "customers_clean_temp.parquet"))
sales_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "sales_clean_temp.parquet"))
inventory_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "inventory_clean_temp.parquet"))
invoices_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "invoices_clean_temp.parquet"))

spark.stop()
