import sqlite3
import os

# Absolute path to your SQLite database
# this path was used for testing: 
#database_path = '/Users/allison.burns/Desktop/exposome/TEST_DB/zip9_exposomes.db'

# use this path for exposome server 
database_path = '/data/exposome_db/zip9_exposomes.db'

# Check if the database file exists
if not os.path.exists(database_path):
    print(f"Database file not found at {database_path}")
else:
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # List all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Print all tables
    print("Tables in the database:")
    for table in tables:
        print(table[0])


    # Execute the PRAGMA table_info query to get column headers of the UCR table
    cursor.execute("PRAGMA table_info(USDA_FARA)")
    columns_info = cursor.fetchall()

    # Extract and print the column names
    column_names = [column[1] for column in columns_info]
    print("\nColumn names in the 'FARA' table:")
    print(column_names)

    
    # Optionally, query the UCR table
    cursor.execute("SELECT * FROM UCR LIMIT 10;")
    rows = cursor.fetchall()
    
    print("\nSample data from UCR table:")
    for row in rows:
        print(row)
  # Execute the query
    cursor.execute("SELECT COUNT(*) FROM UCR")

    # Fetch the result
    count = cursor.fetchone()[0]

    # Print the count
    print("This is the total count of data in the database: ", count)
    
    conn.close()