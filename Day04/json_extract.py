import json
import pandas as pd
import sqlite3

#Extract the data

#Load the json data
with open('sample.json' , 'r') as file:
    data = json.load(file)

#Inspect the data structure
print(json.dumps(data, indent=4))


#Transform the data

#Extract customer data
customers = data['customers']

#Mornalize customers and orders data
customers_df = pd.json_normalize(customers)
orders_df = pd.json_normalize(customers, 'orders',['id'], record_prefix='order_')

#Rename columns 
customers_df.rename(columns={'id': 'customer_id','address.city':'city'},inplace=True)
orders_df.rename(columns={'id':'customer_id', 'order_order_id' : 'order_id'},inplace=True)

#Drop unnecessary columns
customers_df.drop(columns=['orders','address.zip'], inplace=True)

print(customers_df)
print(orders_df)

# Ensure correct data types
customers_df['customer_id'] = customers_df['customer_id'].astype(int)
orders_df['customer_id'] = orders_df['customer_id'].astype(int)
orders_df['order_id'] = orders_df['order_id'].astype(int)
orders_df['order_total'] = orders_df['order_total'].astype(float)


customers_df.to_csv('customers.csv', index=False)
orders_df.to_csv('orders.csv', index=False)


#Load the data

try:
    connection = sqlite3.connect('etl_pipeline.db')
    connection.execute('PRAGMA foreign_keys = ON;')
    cursor = connection.cursor()

    #Create customers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT 
        )
    ''')

    
    # Create Orders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orders (
            order_id INTEGER PRIMARY KEY,
            order_total REAL NOT NULL,
            customer_id INTEGER NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES Customers(customer_id)
        )
    ''')

    #Insert data into the customers,order tables
    # Convert DataFrames to tuples
    customer_records = [tuple(row) for row in customers_df.itertuples(index=False, name=None)]
    orders_records = [tuple(row) for row in orders_df[['order_id', 'order_total', 'customer_id']].itertuples(index=False, name=None)]


    try:
        cursor.executemany('INSERT INTO Customers VALUES (?, ?, ?)', customer_records)
    except sqlite3.Error as e:
        print(f"Error inserting into Customers: {e}")

    try:
        cursor.executemany('INSERT INTO Orders VALUES (?, ?, ?)', orders_records)
    except sqlite3.Error as e:
        print(f"Error inserting into Orders: {e}")

    connection.commit()

    print("Data successfully loaded into the SQLite database!")



    # Verify data insertion
    print("\nCustomers Table:")
    for row in cursor.execute('SELECT * FROM Customers'):
        print(row)

    print("\nOrders Table:")
    for row in cursor.execute('SELECT * FROM Orders'):
        print(row)
    
except sqlite3.Error as e:
    print(f'Error occured: {e}')

finally:
    connection.close()

    