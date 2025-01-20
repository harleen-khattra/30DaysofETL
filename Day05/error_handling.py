import logging

logging.basicConfig(filename='etl_pipeline.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def extract():
    try:
        # Simulate extraction logic
        data = open("data.csv", "r").readlines()
        return data
    except FileNotFoundError as e:
        logging.error(f"Error in extraction: {e}")
        return []

def transform(data):
    try:
        # Simulate transformation logic
        transformed_data = [int(item) for item in data]
        return transformed_data
    except ValueError as e:
        logging.error(f"Error in transformation: {e}")
        return []

def load(data):
    try:
        # Simulate loading logic
        with open("output.txt", "w") as f:
            f.writelines(str(data))
    except Exception as e:
        logging.error(f"Error in loading: {e}")

# Main ETL pipeline
def etl_pipeline():
    data = extract()
    if data:
        transformed_data = transform(data)
        load(transformed_data)

etl_pipeline()
