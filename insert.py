# insert data from excel to postgres all at once

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

import os
from dotenv import load_dotenv

# Excel file path
EXCEL_FILE = "usa_states.xlsx"
SHEET_NAME = "Sheet1"  # Change if needed

# Load Excel file into a pandas DataFrame
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

# Skip the id column (identity auto-generated)
df = df.iloc[:, 1:]

# Create a database connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# Prepare insert query
columns = list(df.columns)
values = [tuple(row) for row in df.to_numpy()]
query = f"""
    INSERT INTO {os.getenv("TABLE_NAME")} ({', '.join(columns)}) 
    VALUES %s
"""
print("Reading excel cell: ", values)
print("Inserting data into the database...")
print(query)
execute_values(cursor, query, values)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data loaded successfully!")
