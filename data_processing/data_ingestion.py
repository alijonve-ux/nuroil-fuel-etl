from pyspark.sql import SparkSession
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("NurOil_DataIngestion") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Define paths
RAW_PATH = r"C:\Users\alijo\Desktop\nuroil\data\raw"
CLEAN_PATH = r"C:\Users\alijo\Desktop\nuroil\data\cleaned"

# Create directories
os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(CLEAN_PATH, exist_ok=True)

# Load raw CSV data
print("📥 Loading raw data...")
stations_df  = spark.read.csv(os.path.join(RAW_PATH, "stations.csv"), header=True, inferSchema=True)
customers_df = spark.read.csv(os.path.join(RAW_PATH, "customers.csv"), header=True, inferSchema=True)
sales_df     = spark.read.csv(os.path.join(RAW_PATH, "sales.csv"), header=True, inferSchema=True)
inventory_df = spark.read.csv(os.path.join(RAW_PATH, "inventory.csv"), header=True, inferSchema=True)
invoices_df  = spark.read.csv(os.path.join(RAW_PATH, "invoices.csv"), header=True, inferSchema=True)

print("✅ Data ingestion completed successfully.")

# Save temporarily as parquet for next steps
stations_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "stations_raw.parquet"))
customers_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "customers_raw.parquet"))
sales_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "sales_raw.parquet"))
inventory_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "inventory_raw.parquet"))
invoices_df.write.mode("overwrite").parquet(os.path.join(CLEAN_PATH, "invoices_raw.parquet"))

print("✅ Raw parquet files saved to:", CLEAN_PATH)
spark.stop()
