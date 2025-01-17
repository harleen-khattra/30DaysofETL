#import libraries
import pandas as pd
from datetime import datetime
import sqlite3

#Load the csv data into a pandas dataframe
df = pd.read_csv('data.csv')

#print the first few lines of the dataframe
print(df.head())

#find the number of duplicate rows
print('Number of duplicated rows are: ', df.duplicated().sum())

#drop the duplicate lines and save the changes
df.drop_duplicates(inplace=True)

#Count the null values in each column
print('Null values in each column are: ')
print(df.isnull().sum())

'''
Age column has one row that has value null. To handle this, we can either:
1. Drop the null row  or
2. Replace with mean/median/mode of the age column  or
3. Froward/backward filling
'''

#Drop the null row
df= df.dropna(subset = ['age'])

'''
#Replace with mean
mean_age = df['age'].mean()
df['age'] = df['age'].fillna(mean_age)

#Replace with median
median_age = df['age'].median()
df['age'] = df['age'].fillna(median_age)

#Replace with mode
mode_age = df['age'].mode()[0]
df['age'] = df['age'].fillna(mean_age)

#use the forward fill (previous non-null value)
df['age'] = df['age'].fillna(method = 'ffill')

#use the backward fill (next non-null value)
df['age'] = df['age'].fillna(method= 'bfill')

'''
#Summary of data frame
print(df.info())

#reset the index
df.reset_index(drop=True, inplace=True)

print(df.isnull().sum())

#drop the rows where department is null
df = df.dropna(subset=['department'])

print(df.info())

'''
To handle the salary missing values, we can drop rows or fill with mean/median/mode/forward/backward fill
--> Another approach can be we can fill the missing values based on the department
'''
mean_by_department = df.groupby('department')['salary'].transform('mean')
df['salary'] = df['salary'].fillna(mean_by_department)

print(df.isnull().sum())

#Fill the missing date with current date
df['joining_date'] = df['joining_date'].fillna(datetime.now().strftime('%Y-%M-%D'))

print(df.isnull().sum())


#To rename columns
df.rename(columns={'name': 'employee_name', 'department': 'employee_department'}, inplace=True)

print(df.head())

#Convert data types (float --> int)
df['salary'] = df['salary'].astype(int)

print(df.info())

df.reset_index(drop=True, inplace=True)

print(df.info())


#Load the transformed data

#Connect to sqlite3
conn = sqlite3.connect('employees.db')

#Load dataframe to sql database
df.to_sql('employees', conn, if_exists='replace', index=False)

result = pd.read_sql("Select * from employees", conn)
print(result)

conn.close()
