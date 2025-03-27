# sqlite_setup.py - Set up SQLite database and create 'faker' table

import sqlite3

# Define SQLite database name
database_name = "faker_data.db"

# SQL statement to create the "faker" table if it does not exist
faker_create_table = ('''
    CREATE TABLE IF NOT EXISTS faker (
        id TEXT PRIMARY KEY,
        name TEXT,
        address TEXT,
        telephone TEXT,
        birthdate DATE,
        city TEXT,
        email TEXT,
        country TEXT,
        job TEXT,
        timezone TEXT
    )
''')

# Function to create the SQLite table

def create_sqlite_table():
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    cursor.execute(faker_create_table)  # Execute table creation query
    conn.commit()  # Commit changes
    cursor.close()

# Create the table
create_sqlite_table()

# Connect to the SQLite database and fetch all records
conn = sqlite3.connect(database_name)
cursor = conn.cursor()
cursor.execute("SELECT * FROM faker")
data = cursor.fetchall()
cursor.close()

# Print all records in the faker table
for row in data:
    print(row)
