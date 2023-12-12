import pandas as pd

data = {
    'employee_name': ['Andy', 'Beth', 'Cindy', 'Dale', 'Andy', 'Cindy'],
    'employee_id': [123456, 789456, 654123, 963852, 123457, 654124],
    'date_joined': ['2015-02-15', None, '2017-05-16', '2018-01-15', '2009-07-10', None],
    'age': [45, 36, 34, 25, 45, 34],
    'yrs_of_experience': [24, 14, 14, 4, 23, 15]
}

df = pd.DataFrame(data)

# Fill NULL values with '2009-12-01'
df['date_joined'].fillna('2009-12-01', inplace=True)

# Convert 'date_joined' column to datetime format
df['date_joined'] = pd.to_datetime(df['date_joined'])

# Create a new column 'join_month' to extract the month from the 'date_joined' column
df['join_month'] = df['date_joined'].dt.strftime('%Y-%m')

# Remove duplicate employee records to avoid double-counting
#df = df.drop_duplicates(subset=['employee_name', 'date_joined'], keep='first')

# Group by 'join_month' and count the number of employees in each group
monthly_join_count = df['join_month'].value_counts().sort_index()
#groupby(['join_month']).count()

print(monthly_join_count)
