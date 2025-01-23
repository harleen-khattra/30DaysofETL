# Import necessary libraries
import pandas as pd
import sqlite3
import re


# Extract: Read data from CSV file
def extract_data(file_path):
    return pd.read_csv(file_path)

# Transform: Perform data cleaning and validation
def transform_data(data):
    # Standardize column names
    data.columns = [col.strip().lower() for col in data.columns]

    # Convert age to numeric and registration_date to datetime
    data['age'] = pd.to_numeric(data['age'], errors='coerce')
    data['registration_date'] = pd.to_datetime(data['registration_date'], errors='coerce')

    # Drop rows with missing required fields
    required_fields = ['id', 'name', 'email']
    data = data.dropna(subset=required_fields)

    # Remove duplicates
    data = data.drop_duplicates()

    # Validate email format
    email_regex = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'
    data = data[data['email'].apply(lambda x: bool(re.match(email_regex, str(x))))]

    # Log invalid rows
    invalid_rows = data[data.isnull().any(axis=1)]
    invalid_rows.to_csv('invalid_rows.csv', index=False)

    # Drop rows with missing data in other columns
    data = data.dropna()

    return data

# Load: Insert cleaned data into SQLite database
def load_data_to_db(data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE,
        age INTEGER,
        registration_date TEXT
    )
    ''')

    # Load data into the database
    data.to_sql('users', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()

# Main function to run the ETL pipeline
def run_etl_pipeline():
    csv_file_path = 'data.csv'  # Specify the CSV file path
    db_path = 'etl_pipeline.db'

    # Step 2: Extract data from CSV
    data = extract_data(csv_file_path)

    # Step 3: Transform data
    transformed_data = transform_data(data)

    # Step 4: Load data into SQLite database
    load_data_to_db(transformed_data, db_path)

    # Verify data loaded into the database
    conn = sqlite3.connect(db_path)
    print(pd.read_sql_query('SELECT * FROM users', conn))
    conn.close()

# Run the ETL pipeline
if __name__ == '__main__':
    run_etl_pipeline()
