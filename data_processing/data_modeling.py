import os
import sqlite3
import pandas as pd

# ---------------------------
# 1. Define Paths
# ---------------------------
BASE_DIR = r"C:\Users\alijo\Desktop\nuroil"
CLEAN_PATH = os.path.join(BASE_DIR, "data", "cleaned")
DB_PATH = os.path.join(BASE_DIR, "nuroil.db")

# ---------------------------
# 2. Connect to SQLite
# ---------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("🧱 Creating Data Model (Star Schema)...")

# Drop old tables (important when rerunning)
cur.executescript("""
DROP TABLE IF EXISTS dim_stations;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_invoices;
DROP TABLE IF EXISTS dim_inventory;
DROP TABLE IF EXISTS fact_sales;

-- ===========================
-- DIMENSION TABLES
-- ===========================

CREATE TABLE dim_stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT,
    region TEXT,
    capacity_liters REAL,
    manager_name TEXT,
    open_date TEXT
);

CREATE TABLE dim_customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    join_date TEXT,
    loyalty_points REAL,
    region TEXT
);

CREATE TABLE dim_invoices (
    invoice_id TEXT PRIMARY KEY,
    station_id TEXT,
    vendor TEXT,
    invoice_amount REAL,
    invoice_date TEXT,
    status TEXT
);

CREATE TABLE dim_inventory (
    inventory_id TEXT PRIMARY KEY,
    station_id TEXT,
    fuel_type TEXT,
    delivery_liters REAL,
    current_stock REAL,
    delivery_date TEXT
);

-- ===========================
-- FACT TABLE
-- ===========================
CREATE TABLE fact_sales (
    sale_id TEXT PRIMARY KEY,
    station_id TEXT,
    customer_id TEXT,
    fuel_type TEXT,
    liters REAL,
    price_per_liter REAL,
    total_amount REAL,
    payment_type TEXT,
    sale_date TEXT,
    FOREIGN KEY (station_id) REFERENCES dim_stations(station_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);
""")

print("✅ Tables created successfully!")

# ---------------------------
# 3. Load Parquet Data → SQLite
# ---------------------------
print("📦 Loading cleaned parquet data into SQLite warehouse...")

parquet_files = {
    "dim_stations": "stations_clean.parquet",
    "dim_customers": "customers_clean.parquet",
    "dim_invoices": "invoices_clean.parquet",
    "dim_inventory": "inventory_clean.parquet",
    "fact_sales": "sales_clean.parquet"
}

for table, file in parquet_files.items():
    file_path = os.path.join(CLEAN_PATH, file)
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        df.to_sql(table, conn, if_exists="append", index=False)
        print(f"✅ Loaded {len(df)} records into {table}")
    else:
        print(f"⚠️ File not found: {file_path}")

conn.commit()
conn.close()

print("\n🎯 Data modeling and loading complete! SQLite data warehouse is ready.")
