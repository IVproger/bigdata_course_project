import psycopg2 as psql
from pprint import pprint
import os
import re

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
        password=file.read().rstrip()

# build connection string
conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=team14 dbname=team14_projectdb password={}".format(password)

conn = None
cur = None

try:
    # Connect to the database
    print("Connecting to the PostgreSQL database...")
    conn = psql.connect(conn_string)
    cur = conn.cursor()

    # Get all table names in the public schema
    print("Fetching table names...")
    cur.execute("""
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public';
    """)
    tables = cur.fetchall()

    if not tables:
        print("No tables found in the public schema.")
    else:
        # Drop each table
        print("Dropping tables...")
        for table in tables:
            table_name = table[0]
            print(f"  Dropping table: {table_name}")
            # Use IF EXISTS to avoid errors if table doesn't exist
            # Use CASCADE to drop dependent objects
            cur.execute(f"DROP TABLE IF EXISTS public.\"{table_name}\" CASCADE;")

        # Commit the changes
        print("Committing changes...")
        conn.commit()
        print("All tables dropped successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
    if conn:
        conn.rollback() # Rollback changes on error

finally:
    # Close cursor and connection
    if cur:
        cur.close()
    if conn:
        conn.close()
        print("Database connection closed.")