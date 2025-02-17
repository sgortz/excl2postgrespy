# insert data from excel to postgres parsing each line at a time

import pandas as pd
import psycopg2
import os

# Database configuration
TABLE_NAME = os.getenv("TABLE_NAME")

# Excel file path
EXCEL_FILE = "usa_states.xlsx"
SHEET_NAME = "Sheet1"  # Change if needed

# Load Excel file into a pandas DataFrame
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

# Skip the first column
df = df.iloc[:, 1:]

# Replace NaN values with empty strings for character varying columns
df = df.fillna("")

# Create a database connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()
print("Cursor: ", cursor)

# Prepare insert query
columns = list(df.columns)
query = f"INSERT INTO {TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

print("Reading excel cell: ", df.iloc[0])
print(query)

# Insert each row one at a timeclear
for row in df.itertuples(index=False):
    print("row: ", row)
    cursor.execute(query, row)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data loaded successfully!")
