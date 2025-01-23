import pandas as pd
import logging
import random

# Configure logging
logging.basicConfig(
    filename="etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

def extract():
    logging.info("Starting data extraction.")
    # Simulating data extraction
    data = [{"id": i, "value": random.randint(1, 100)} for i in range(1, 6)]
    logging.info("Data extraction completed.")
    return pd.DataFrame(data)

def transform(df):
    logging.info("Starting data transformation.")
    # Simulating data transformation
    df["transformed_value"] = df["value"] * 2
    logging.info("Data transformation completed.")
    return df

def load(df):
    logging.info("Starting data loading.")
    # Simulating data loading by saving to a CSV file
    df.to_csv("output.csv", index=False)
    logging.info("Data loading completed.")

def etl_pipeline():
    logging.info("ETL pipeline started.")
    try:
        data = extract()
        transformed_data = transform(data)
        load(transformed_data)
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
