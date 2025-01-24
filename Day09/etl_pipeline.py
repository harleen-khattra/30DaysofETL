import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('data/sales.db')
cursor = conn.cursor()

# Read CSV file into pandas DataFrame
df = pd.read_csv('data/sales_data.csv')  # Adjust the path to your CSV file

# Load the data into the sales table
df.to_sql('sales', conn, if_exists='replace', index=False)  # 'sales' is the table name

# Commit and close the connection
conn.commit()
conn.close()

print("CSV data loaded into sales.db successfully!")
