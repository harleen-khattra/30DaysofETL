import pandas as pd

df = pd.read_csv('data.csv')

#Print the data types of each column
print(df.dtypes)

#Count the the number of values in each category
print(df['Category'].value_counts())

#Print all the unique values
print(df['Category'].unique())

#Detect outliers in numerical columns
'''
Methods to detect outliers are:
1. Visual inspection: Boxplot, Scatterplot
2. Statistical methods: IQR, Z-score
'''
#IQR method

#1. Find the 25th percentile
q1 = df['Sales'].quantile(0.25)

#2. Find the 75th percentile
q3 = df['Sales'].quantile(0.75)

#3. Compute the IQR
iqr = q3-q1

#4. Find the lower bound
lower = q1-1.5*iqr

#5.Find the upper bound
upper = q3+1.5*iqr

#6. Any data point that are below the lower bound and above the upper bound are outliers
outliers = df[(df['Sales'] < lower) | (df['Sales'] > upper)]
print('Outliers: ')
print(outliers)

#Print the data that is not in the listed categories
print(df[~df["Category"].isin(["Electronics", "Furniture"])])

#Filter rows based on a condition
print(df[df['Sales'] > 300])

#Filter rows based on multiple condition
print(df[(df['Sales']>300) & (df['Category'] != 'Electronics') ])

#Filter based on string patterns
df[df['Category'].str.contains('El', regex=True)]

#Filter using a custom function
def sales(row):
    return row['Sales']>300

filtered_df = df[df.apply(sales, axis=1)]
print(filtered_df)

#Normalize a column value
df['normalized_sales'] = (df['Sales'] - df['Sales'].min()) / (df['Sales'].max() - df['Sales'].min())
print(df)

#Create a rank column
df['Sales_rank'] = df['Sales'].rank(ascending=False)
print(df)

#Create a column that multiply rows
df['total_amount'] = df['Sales'] * df['Quantity']
print(df)

#Multi indexing
df.set_index(['Category','Subcategory'], inplace=True)
print(df)

#To accees multi indexing
print(df.loc[('Electronics','Laptop')])

#Reset index back to deafult
df.reset_index(inplace=True)
print(df)

'''
Aggregate functions:
sum(), mean(), median(), count(), min(), max()
'''

#Pivot table
pivot = df.pivot_table(
    values="Sales",
    index="Category",
    columns="Subcategory",
    fill_value=0
)
print(pivot)
