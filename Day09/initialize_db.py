import sqlite3
import pandas as pd
import requests

# Initialize SQLite database
conn = sqlite3.connect('sales.db')
cursor = conn.cursor()

# Load and execute the schema from sales.sql
with open("sales.sql", "r") as file:
    cursor.executescript(file.read())

# Load products data from CSV file
df_products = pd.read_csv('products.csv')
df_products.to_sql('products', conn, if_exists='append', index=False)

# Load customers data from JSON file
df_customers = pd.read_json('customers.json')
df_customers.to_sql('customers', conn, if_exists='append', index=False)

# Insert sales data into the sales table (dynamic data handling)
sales_data = [
    (1, '2025-01-10', 1, 101, 1000.00, 1),
    (2, '2025-01-10', 2, 102, 1400.00, 2),
    (3, '2025-01-11', 3, 103, 300.00, 2),
    (4, '2025-01-12', 4, 104, 120.00, 1),
    (5, '2025-01-12', 5, 101, 250.00, 1),
    (6, '2025-01-13', 6, 102, 60.00, 3),
]
cursor.executemany('''
    INSERT INTO sales (sale_id, date, product_id, customer_id, sales_amount, quantity)
    VALUES (?, ?, ?, ?, ?, ?)
''', sales_data)

# Commit changes and close connection
conn.commit()
conn.close()

print("Schema created and data inserted successfully!")

