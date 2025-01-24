import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("sales.db")

# Function to run and display query results
def run_query(query, description):
    print(f"\n{description}")
    print("=" * len(description))
    result = pd.read_sql_query(query, conn)
    print(result)

# Test 1: Verify Products Table
run_query("SELECT * FROM products", "Products Table Data")

# Test 2: Verify Customers Table
run_query("SELECT * FROM customers", "Customers Table Data")

# Test 3: Verify Sales Table
run_query("SELECT * FROM sales", "Sales Table Data")

# Test 4: Total Sales by Product
run_query("""
    SELECT 
        p.product_name,
        SUM(s.sales_amount) AS total_sales
    FROM 
        sales s
    JOIN 
        products p ON s.product_id = p.product_id
    GROUP BY 
        p.product_name
""", "Total Sales by Product")

# Test 5: Total Sales by Customer
run_query("""
    SELECT 
        c.customer_name,
        SUM(s.sales_amount) AS total_sales
    FROM 
        sales s
    JOIN 
        customers c ON s.customer_id = c.customer_id
    GROUP BY 
        c.customer_name
""", "Total Sales by Customer")

# Test 6: Total Sales by Category
run_query("""
    SELECT 
        p.category,
        SUM(s.sales_amount) AS total_sales
    FROM 
        sales s
    JOIN 
        products p ON s.product_id = p.product_id
    GROUP BY 
        p.category
""", "Total Sales by Category")

# Close the connection
conn.close()
