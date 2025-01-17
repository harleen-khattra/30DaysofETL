import pymysql
import csv

#Database connection details
db_config = {
    'host' : 'localhost',
    'user' : 'root',
    'password' : 'password', #Replace with password
    'database' : 'employees'
}

#Create the connection
def create_connection():
    try: 
        connection = pymysql.connect(**db_config)
        print('Database connection successful')
        return connection
    except pymysql.MySQLError as e:
        print(f'Error connecting the database: {e}')
        exit()

#Extract the data
def extract_data():
    try:
        with connection.cursor() as cursor:
            sql_query = "Select * from employee"
            cursor.execute(sql_query)
            results = cursor.fetchall()
            return results
            
    except pymysql.MySQLError as e:
        print(f'Error fetching data, {e}')
        return []

#Write the data to a csv file       
def write_to_csv(employees):
    try:
        with open('employees.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['ID', 'Name', 'Position', 'Salary'])
            for emp in employees:
                writer.writerow(emp)
        print("Data written successfully")
    except Exception as e:
        print(f'Error writing data: {e}')



if __name__ =="__main__":
    connection = create_connection()
    employees = extract_data()
    if employees:
        write_to_csv(employees)
    else:
        print("No data found!")

    connection.close()
    print('Connection closed successfully')

