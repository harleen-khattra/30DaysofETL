import requests
import json
import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")

api_url = 'https://jsonplaceholder.typicode.com/posts'

logging.info('Starting the ETL pipeline...')

#fetch the data
try:
    logging.info('Fetching the data from api..')
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    logging.info('Data fetched successfully')
    data = response.json()
    print(json.dumps(data[0], indent=4))
except requests.exceptions.RequestException as e:
    print(f'Error occured: {e}')

#create dataframe
df = pd.DataFrame(data)
print(df.head())

logging.info('Transforming the data')
df = df[['userId', 'id', 'title', 'body']]

#add a new column
df['body_length'] = df['body'].apply(len)
print('Transformed data')
print(df.head())
logging.info('Data transformation completed')

#df.to_csv('posts.csv', index=False)


#Connect to SQLite database (or create it if it doesn't exist)

try: 
    logging.info('Connecting to database')
    db_name = "etl_pipeline.db"
    conn = sqlite3.connect(db_name)  # Establishes connection
    cursor = conn.cursor()
    logging.info('Database connected...')

    #Create a table in the database
    table_name = "api_data"
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            userId INTEGER,
            id INTEGER PRIMARY KEY,
            title TEXT,
            body TEXT,
            body_length INTEGER
            );
        """)
    logging.info(f'Table: {table_name} created successfully')
    print(f"Table '{table_name}' created successfully (if not exists).")

    #Insert data into the table
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data successfully inserted into table '{table_name}'.")
    logging.info(f"Data successfully inserted into table '{table_name}'.")

    logging.info('Fetching data...')
    #Query the database to verify insertion
    print("\nSample Data from Database:")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    logging.info('Data fetched successfully')
except:
    print('Error occured')
finally:
    #Close the connection
    conn.close()
    print("\nDatabase connection closed.")
    logging.info('Database connection closed successfully..')

logging.info('ETL pipeline finished..')