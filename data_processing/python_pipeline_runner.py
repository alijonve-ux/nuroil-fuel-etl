import os, datetime

LOG_FILE = r"C:\Users\alijo\Desktop\nuroil\pipeline_logs.txt"

def log(msg):
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.datetime.now()}] {msg}n")
    print(msg)

steps = [
    ("Data Ingestion", "data_ingestion.py"),
    ("Data Cleaning", "data_cleaning.py"),
    ("Data Transformation", "data_transformation.py"),
    ("Save Cleaned Data", "save_cleaned.py"),
    ("Data Modeling", "data_modeling.py"),
    ("Load to SQLite Warehouse", "load_to_datawarehouse.py"),
    
]

for name, script in steps:
    log(f"\n Running step: {name} ...")
    result = os.system(f"python \"{script}\"")
    if result != 0:
        log(f" ERROR: Step '{name}' failed with exist code {result}\n")
        break
    else:
        log(f" Step '{name}' completed succesfully. \n")

print(" Pipeline execution finished. \n")
