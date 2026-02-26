import sqlite3
import pandas as pd
import os
from pyspark.sql import SparkSession

# Define paths
CLEAN_PATH = r"C:\Users\alijo\Desktop\nuroil\data\cleaned"
DB_PATH = r"C:\Users\alijo\Desktop\nuroil\warehouse\nuroil_dw.sqlite"

# We will make sure directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

#Start Spark to read parquet files
spark = SparkSession.builder.appName("nurOil_LoadSQLite").getOrCreate()

print("Loading cleaned parquet data into SQLite warehouse...")

# Tables to Load

tables = ["stations_final", "customers_final", "sales_final", "inventory_final", "invoices_final"]

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

for t in tables:
    parquet_file=os.path.join(CLEAN_PATH, f"{t}.parquet")

    #Read parquet .from Pandas to SQLite
    df = spark.read.parquet(parquet_file).toPandas()
    df.to_sql(t, conn, if_exists="replace", index=False)

    print(f" Loaded {t} into SQLite ({len(df)} rows)")

conn.close()
spark.stop()
print("Data successfully stored in SQLite warehouse at:", DB_PATH)

